import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook

logger = logging.getLogger(__name__)
# Dynamic variables
runtime_args = Variable.get("dpp_canada_data_quality_agg", deserialize_json=True)
global_conf = runtime_args['global']
email_list_alerts = global_conf['email_list_alerts']
projectDeploymentDir=global_conf['projectDeploymentDir']
hive_db=global_conf['hive_db']
hive_hdfs_path=global_conf['hive_hdfs_path']
hive_table=global_conf['hive_table']
ddl_script_path=global_conf['ddl_script_path']
sourceJDBCURL=global_conf['sourceJDBCURL']
applicationUserName=global_conf['applicationUserName']
csv_path=global_conf['csv_agg_path']
script_path=projectDeploymentDir+global_conf['script_path']
sql_query=global_conf['hive_agg_query']
copy_local_path=global_conf['copy_local_agg']
elastic_script=global_conf['elastic_script_agg']

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

default_args = {
    'owner': 'Merch Assortment Canada Team',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 9),
    # 'end_date': datetime(2021, 7, 30),
    'email': email_list_alerts,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback': slack_notification
}

def _first_function_execute(**kwargs):
    print(kwargs['key1'])

with DAG('dpp_canada_aggregation_data_quality', default_args=default_args, schedule_interval='@once', catchup=False, template_searchpath=projectDeploymentDir) as dag:
    dummy_start_task = DummyOperator(
        task_id='start_task'
    )
    #first_function_execute1 = PythonOperator(
    #    task_id="first_function_execute1",
    #    python_callable=_first_function_execute,
        #op_kwargs={'key1': '/usr/bin/beeline -u '+sourceJDBCURL+' -n ' +applicationUserName+' -e ' +"\""+query+"\""}
    #    op_kwargs={'key1':'/usr/bin/beeline -u '+sourceJDBCURL+' -n ' +applicationUserName+' --hivevar db_name='+hive_db+' --hivevar hive_table='+hive_table+' --hivevar hive_hdfs_path='+hive_hdfs_path+'/'+hive_db+'/'+hive_table+' -f '+projectDeploymentDir+ddl_script_path}
    #)
    ddl_create=BashOperator(
        task_id="ddl_create",
        bash_command="kinit -kt /lowes/servicekeys/assortmnt_merch/svcmapprdrw.keytab svcmapprdrw@gmail.com; /usr/bin/beeline -u "+sourceJDBCURL+" -n " +applicationUserName+" --hivevar hive_db="+hive_db+" --hivevar hive_table="+hive_table+" --hivevar hive_hdfs_path="+hive_hdfs_path+"/"+hive_db+"/"+hive_table+" -f "+projectDeploymentDir+ddl_script_path
    )
    dummy_end_task = DummyOperator(
        task_id='end_task')
    with TaskGroup(group_id='lowes_data', tooltip="Tasks for lowes aggregation") as lowes_data:
        query=global_conf['lowes_query']
        insert_data=BashOperator(
                    task_id="insert_data",
                    bash_command='kinit -kt /lowes/servicekeys/assortmnt_merch/svcmapprdrw.keytab svcmapprdrw@gmail.com; /usr/bin/beeline -u '+sourceJDBCURL+' -n ' +applicationUserName+' -e ' +"\""+query+"\""
                )


    with TaskGroup(group_id='rona_data', tooltip="Tasks for rona aggregation") as rona_data:
        query=global_conf['rona_query']
        insert_data=BashOperator(
                    task_id="insert_data",
                    bash_command='kinit -kt /lowes/servicekeys/assortmnt_merch/svcmapprdrw.keytab svcmapprdrw@gmail.com; /usr/bin/beeline -u '+sourceJDBCURL+' -n ' +applicationUserName+' -e ' +"\""+query+"\""
                )


    with TaskGroup(group_id='spark_submit',tooltip="Task for pyspark to write the data to csv") as spark_submit:
        pyspark=BashOperator(
            task_id="pyspark",
            bash_command=f"""kinit -kt /lowes/servicekeys/assortmnt_merch/svcmapprdrw.keytab svcmapprdrw@gmail.com; \
                            /usr/bin/spark-submit \
                            --master yarn \
                            --deploy-mode client \
                            {script_path} {sql_query} {csv_path}"""
        )
    with TaskGroup(group_id='Rename_file_name',tooltip="rename the pyspark generated part file") as Rename_file_name:
        rename=BashOperator(
            task_id="rename",
            bash_command=f"""kinit -kt /lowes/servicekeys/assortmnt_merch/svcmapprdrw.keytab svcmapprdrw@gmail.com; \
                            hdfs dfs -mv {csv_path}/*.csv {csv_path}/aggregate.csv; \
                            hdfs dfs -copyToLocal -f {csv_path}/*.csv {copy_local_path}
                            """
        )

    with TaskGroup(group_id='push_to_elastic',tooltip="push the data to elastic search from csv") as push_to_elastic:
        push=BashOperator(
            task_id="push",
            bash_command=f"""kinit -kt /lowes/servicekeys/assortmnt_merch/svcmapprdrw.keytab svcmapprdrw@gmail.com; \
            python3 {elastic_script}"""
        )

dummy_start_task >> ddl_create >> lowes_data >> rona_data >> spark_submit >> Rename_file_name >> push_to_elastic >> dummy_end_task
