import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.email import send_email

logger = logging.getLogger(__name__)


# Dynamic variables
runtime_args = Variable.get("dpp_canada", deserialize_json=True)
global_conf = runtime_args['global']
deployment_location = global_conf['deployment_location']
logfile_location = global_conf['logfile_location']
keytab_location = global_conf['keytab_location']
keytab_principal = global_conf['keytab_principal']
email_list_alerts = global_conf['email_list_alerts']
application_hive_db = global_conf['application_hive_db']
log_level = global_conf['log_level']
prop_file_suffix = global_conf['properties_files_suffix']
hive_conn_id = global_conf['hive_cli_conn_id']
hive_table_base_path = global_conf['hive_table_base_path']
yarn_queue_name = global_conf['yarn_queue_name']
ddl_sub_folders = global_conf['gmail_etl_job_properties']['ddl_sub_folders']

ingestion_job_script = f'{deployment_location}/bash/generic/ingestion_job_runner.sh'
allocation_job_script = f'{deployment_location}/bash/generic/allocation_job_runner.sh'
ddl_base_folder = f'{deployment_location}/hive/ddls/'
ingestion_application_name = 'DppCanadaIngestion'

# Rona etl properties
rona_g1_jobs_properties_list = global_conf['rona_etl_job_properties']['rona_g1_jobs_properties_list']
rona_g2_job_property = global_conf['rona_etl_job_properties']['rona_g2_job_property']
rona_g3_job_property = global_conf['rona_etl_job_properties']['rona_g3_job_property']
rona_g4_allocation_job_property = global_conf['rona_etl_job_properties']['rona_g4_allocation_job_property'][
    'job_property']
rona_g4_allocation_master = global_conf['rona_etl_job_properties']['rona_g4_allocation_job_property']['master']
rona_g4_allocation_output = global_conf['rona_etl_job_properties']['rona_g4_allocation_job_property']['output']
rona_g5_job_property = global_conf['rona_etl_job_properties']['rona_g5_job_property']

# gmail etl properties
gmail_g1_jobs_properties_list = global_conf['gmail_etl_job_properties']['gmail_g1_jobs_properties_list']
gmail_g2_jobs_properties_list = global_conf['gmail_etl_job_properties']['gmail_g2_jobs_properties_list']
gmail_g3_jobs_properties_list = global_conf['gmail_etl_job_properties']['gmail_g3_jobs_properties_list']
gmail_g4_allocation_job_property = global_conf['gmail_etl_job_properties']['gmail_g4_allocation_job_property'][
    'job_property']
gmail_g4_allocation_master = global_conf['gmail_etl_job_properties']['gmail_g4_allocation_job_property']['master']
gmail_g4_allocation_output = global_conf['gmail_etl_job_properties']['gmail_g4_allocation_job_property']['output']
gmail_g5_job_property = global_conf['gmail_etl_job_properties']['gmail_g5_job_property']
gmail_g6_allocation_job_property = global_conf['gmail_etl_job_properties']['gmail_g6_allocation_job_property'][
    'job_property']
gmail_g6_allocation_master = global_conf['gmail_etl_job_properties']['gmail_g6_allocation_job_property']['master']
gmail_g6_allocation_output = global_conf['gmail_etl_job_properties']['gmail_g6_allocation_job_property']['output']
gmail_g7_job_property = global_conf['gmail_etl_job_properties']['gmail_g7_job_property']

# Combined DPP property
combined_etl_job_property = global_conf['combined_etl_job_property']

#start_date = '2020-01-31'
start_date = '2019-01-01'
end_date = '2022-02-04'


instance_date = """{{ macros.ds_format(ts_nodash, "%Y%m%dT%H%M%S", "%Y-%m-%d-%H-%M-%S") }}"""


def slack_notification(context):
    slack_webhook_token = BaseHook.get_connection('dpp_canada_slack_conn_id').password
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    failed_alert = SlackWebhookOperator(
        task_id='slack_notification',
        http_conn_id='dpp_canada_slack_conn_id',
        webhook_token=slack_webhook_token,
        message=slack_msg)
    return failed_alert.execute(context=context)




# create default args for DAG
# -------------------------------------
default_args = {
    'owner': 'Merch Assortment Canada Team',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 9),
    'email': email_list_alerts,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback': slack_notification

}



# define dag
with DAG('canada_combined_dpp', default_args=default_args, schedule_interval='@once', catchup=False,
         template_searchpath=deployment_location) as dag:
    dummy_start_task = DummyOperator(
        task_id='start_task'
    )
    dummy_end_task = DummyOperator(
        task_id='end_task')

    # Start Rona DPP Task Group definition
    with TaskGroup(group_id='canada_rona_dpp', tooltip="Tasks for Canada Rona Dpp") as rona_dpp_group:

        rona_g1_jobs = []
        # 2. ingestion job for parallel running
        for job in rona_g1_jobs_properties_list:
            rona_g1_jobs.append(BashOperator(
                task_id=job + '_transformation_task',
                bash_command=f"""{ingestion_job_script} {job} 2048 1 10G 20G 256M true 3 60 40G {application_hive_db} {logfile_location} {job}{prop_file_suffix} {start_date} {end_date} {log_level} {yarn_queue_name} {keytab_location} {keytab_principal} {instance_date} """))

        dummy_join_task = DummyOperator(
            task_id='join_task')

        rona_g2_job = BashOperator(
            task_id=rona_g2_job_property + '_transformation_task',
            bash_command=f"""{ingestion_job_script} {rona_g2_job_property} 2048 1 10G 20G 256M true 5 60 40G {application_hive_db} {logfile_location} {rona_g2_job_property}{prop_file_suffix} {start_date} {end_date} {log_level} {yarn_queue_name} {keytab_location} {keytab_principal} {instance_date} """)

        rona_g3_job = BashOperator(
            task_id=rona_g3_job_property + '_transformation_task',
            bash_command=f"""{ingestion_job_script} {rona_g3_job_property} 2048 1 10G 20G 256M true 5 60 40G {application_hive_db} {logfile_location} {rona_g3_job_property}{prop_file_suffix} {start_date} {end_date} {log_level} {yarn_queue_name} {keytab_location} {keytab_principal} {instance_date} """)

        rona_g4_allocation_job = BashOperator(
            task_id=rona_g4_allocation_job_property + '_allocation_task',
            bash_command=f"""{allocation_job_script} {rona_g4_allocation_job_property} 2048 1 10G 20G 256M true 5 60 40G {logfile_location} {rona_g4_allocation_job_property}{prop_file_suffix} {rona_g4_allocation_master} {rona_g4_allocation_output} {application_hive_db} {start_date} {end_date} {log_level} {yarn_queue_name} {keytab_location} {keytab_principal} {instance_date} """)

        rona_g5_job = BashOperator(
            task_id=rona_g5_job_property + '_transformation_task',
            bash_command=f"""{ingestion_job_script} {rona_g5_job_property} 2048 1 10G 20G 256M true 3 60 40G {application_hive_db} {logfile_location} {rona_g5_job_property}{prop_file_suffix} {start_date} {end_date} {log_level} {yarn_queue_name} {keytab_location} {keytab_principal} {instance_date} """)

        rona_g1_jobs >> dummy_join_task >> rona_g2_job >> rona_g3_job >> rona_g4_allocation_job >> rona_g5_job
    # End Rona DPP Task Group definition

    # Start gmail DPP Task Group definition
    with TaskGroup(group_id='canada_gmail_dpp', tooltip="Tasks for Canada gmail Dpp") as gmail_dpp_group:

        gmail_g1_jobs = []
        # 1. ingestion job for parallel running
        for job in gmail_g1_jobs_properties_list:
            gmail_g1_jobs.append(BashOperator(
                task_id=job + '_transformation_task',
                bash_command=f"""{ingestion_job_script} {job} 2048 1 10G 20G 256M true 4 60 60G {application_hive_db} {logfile_location} {job}{prop_file_suffix} {start_date} {end_date} {log_level} {yarn_queue_name} {keytab_location} {keytab_principal} {instance_date} """))

        dummy_join_task1 = DummyOperator(
            task_id='join_task1')

        gmail_g2_jobs = []
        # 2. ingestion job for parallel running
        for job in gmail_g2_jobs_properties_list:
            gmail_g2_jobs.append(BashOperator(
                task_id=job + '_transformation_task',
                bash_command=f"""{ingestion_job_script} {job} 2048 1 10G 20G 256M true 3 60 40G {application_hive_db} {logfile_location} {job}{prop_file_suffix} {start_date} {end_date} {log_level} {yarn_queue_name} {keytab_location} {keytab_principal} {instance_date} """))

        dummy_join_task2 = DummyOperator(
            task_id='join_task2')

        gmail_g3_jobs = []
        # 3. ingestion job for parallel running
        for job in gmail_g3_jobs_properties_list:
            gmail_g3_jobs.append(BashOperator(
                task_id=job + '_transformation_task',
                bash_command=f"""{ingestion_job_script} {job} 2048 1 10G 20G 256M true 3 60 40G {application_hive_db} {logfile_location} {job}{prop_file_suffix} {start_date} {end_date} {log_level} {yarn_queue_name} {keytab_location} {keytab_principal} {instance_date} """))

        dummy_join_task3 = DummyOperator(
            task_id='join_task3')

        # 4. allocation job1
        gmail_g4_allocation_job = BashOperator(
            task_id=gmail_g4_allocation_job_property + '_allocation_task1',
            bash_command=f"""{allocation_job_script} {gmail_g4_allocation_job_property} 2048 1 10G 20G 256M true 3 60 40G {logfile_location} {gmail_g4_allocation_job_property}{prop_file_suffix} {gmail_g4_allocation_master} {gmail_g4_allocation_output} {application_hive_db} {start_date} {end_date} {log_level} {yarn_queue_name} {keytab_location} {keytab_principal} {instance_date} """)

        # 5. post allocation ingestion job1
        gmail_g5_job = BashOperator(
            task_id=gmail_g5_job_property + '_transformation_task',
            bash_command=f"""{ingestion_job_script} {gmail_g5_job_property} 2048 1 10G 20G 256M true 3 60 40G {application_hive_db} {logfile_location} {gmail_g5_job_property}{prop_file_suffix} {start_date} {end_date} {log_level} {yarn_queue_name} {keytab_location} {keytab_principal} {instance_date} """)

        # 6. allocation job2
        gmail_g6_allocation_job = BashOperator(
            task_id=gmail_g6_allocation_job_property + '_allocation_task2',
            bash_command=f"""{allocation_job_script} {gmail_g6_allocation_job_property} 2048 1 10G 20G 256M true 3 60 40G {logfile_location} {gmail_g6_allocation_job_property}{prop_file_suffix} {gmail_g6_allocation_master} {gmail_g6_allocation_output} {application_hive_db} {start_date} {end_date} {log_level} {yarn_queue_name} {keytab_location} {keytab_principal} {instance_date} """)

        # 7. post allocation ingestion job2
        gmail_g7_job = BashOperator(
            task_id=gmail_g7_job_property + '_transformation_task',
            bash_command=f"""{ingestion_job_script} {gmail_g7_job_property} 2048 1 10G 20G 256M true 3 60 40G {application_hive_db} {logfile_location} {gmail_g7_job_property}{prop_file_suffix} {start_date} {end_date} {log_level} {yarn_queue_name} {keytab_location} {keytab_principal} {instance_date} """)

        # Set Task Group's  dependencies
        gmail_g1_jobs >> dummy_join_task1 >> gmail_g2_jobs >> dummy_join_task2 >> gmail_g3_jobs >> dummy_join_task3 >> gmail_g4_allocation_job >> gmail_g5_job >> gmail_g6_allocation_job >> gmail_g7_job

    # End gmail DPP Task Group definition

    combined_dpp_job = BashOperator(
        task_id=combined_etl_job_property + '_transformation_task',
        bash_command=f"""{ingestion_job_script} {combined_etl_job_property} 2048 1 10G 20G 256M true 3 60 40G {application_hive_db} {logfile_location} {combined_etl_job_property}{prop_file_suffix} {start_date} {end_date} {log_level} {yarn_queue_name} {keytab_location} {keytab_principal} {instance_date} """)

dummy_start_task >> [gmail_dpp_group, rona_dpp_group] >> combined_dpp_job >> dummy_end_task
