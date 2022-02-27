import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook

logger = logging.getLogger(__name__)
# Dynamic variables
runtime_args = Variable.get("gmail_canada_sqoop_import_job", deserialize_json=True)
global_conf = runtime_args['global']
environment=global_conf['environment']
projectDeploymentDir = global_conf['projectDeploymentDir']
keytab_location = global_conf['keytab_location']
keytab_principal = global_conf['keytab_principal']
email_list_alerts = global_conf['email_list_alerts']
application_hive_db = global_conf['application_hive_db']
hive_table_base_path = global_conf['hive_table_base_path']
yarn_queue_name = global_conf['yarn_queue_name']
localLogPath=global_conf['localLogPath']
log_level = global_conf['log_level']
sourceJDBCURL=global_conf['sourceJDBCURL']
jobTracker=global_conf['jobTracker']
nameNode=global_conf['nameNode']
targetNameNode=global_conf['targetNameNode']
hostName=global_conf['hostName']
targetHostName=global_conf['targetHostName']
targetJDBCURL=global_conf['targetJDBCURL']
queueName=global_conf['queueName']
logRetentionDuration=global_conf['logRetentionDuration']
addPartitionsScript=projectDeploymentDir+global_conf['addPartitionsScript']
sqoopValidationScript=projectDeploymentDir+global_conf['sqoopValidationScript']
sqoopOptionsFile=projectDeploymentDir+global_conf['sqoopOptionsFile']
userLookupScript=projectDeploymentDir+global_conf['userLookupScript']
loggingScript=projectDeploymentDir+global_conf['loggingScript']
ingestionCleanupScript=projectDeploymentDir+global_conf['ingestionCleanupScript']
sqoopIngestionScript=projectDeploymentDir+global_conf['sqoopIngestionScript']
sourceSystem=global_conf['sourceSystem']
hdfsBaseDataLocation=global_conf['hdfsBaseDataLocation']
tableDataLocation=global_conf['tableDataLocation']
stageTableDataLocation=global_conf['stageTableDataLocation']
workflowUserMappingFile=projectDeploymentDir+global_conf['workflowUserMappingFile']
applicationUserName=global_conf['applicationUserName']
triggerLocation=global_conf['triggerLocation']
partitionColValue=datetime.today().strftime('%Y-%m-%d')
instanceDate=datetime.today().strftime('%Y-%m-%d')
decrypt_script=global_conf['decrypt_script']
private_key=global_conf['private_key']
teradata_pswd=global_conf['teradata_pswd']
# sqoop etl properties
job = 'table_f0543_ic'

def slack_notification(context):
    slack_webhook_token = BaseHook.get_connection('gmail_canada_slack_conn_id').password
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
        http_conn_id='gmail_canada_slack_conn_id',
        webhook_token=slack_webhook_token,
        message=slack_msg)
    return failed_alert.execute(context=context)

# create default args for DAG
# -------------------------------------
default_args = {
    'owner': 'Sqoop_job_testing',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 9),
    # 'end_date': datetime(2021, 7, 30),
    'email': email_list_alerts,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback': slack_notification

}

def _table_type(**kwargs):
    if kwargs['key1']=="DP":
        return 'partition_distcp_node'
    return 'nonpartition_distcp_node'


def _dummy_test(**kwargs):
    if environment =='prod':
        return 'distcp_type'
    return 'cleanup'

with DAG('sqoop_job_dag', default_args=default_args, schedule_interval='00 04 * * *', catchup=False, template_searchpath=projectDeploymentDir) as dag:
    dummy_start_task = DummyOperator(
        task_id='start_task'
    )
    dummy_end_task = DummyOperator(
        task_id='end_task')

    tableType=global_conf[job]['tableType']
    tableLoadType=global_conf[job]['tableLoadType']
    tableSensitivity=global_conf[job]['tableSensitivity']
    sqoopSelectQuery=global_conf[job]['sqoopSelectQuery']
    fieldsTerm=global_conf[job]['fieldsTerm']
    sourceRowCountQuery=global_conf[job]['sourceRowCountQuery']
    numMappers=global_conf[job]['numMappers']
    compCodec=global_conf[job]['compCodec']
    sourceDBName=global_conf[job]['sourceDBName']
    sourceTableName=global_conf[job]['sourceTableName']
    hiveDBName=global_conf[job]['hiveDBName']
    hiveTableName=global_conf[job]['hiveTableName']
    splitByColName=global_conf[job]['splitByColName']
    partitionColName=global_conf[job]['partitionColName']
    sqoopWhereColName=global_conf[job]['sqoopWhereColName']
    hiveStagedDBName=global_conf[job]['hiveStagedDBName']
    hiveStagedTableName=global_conf[job]['hiveStagedTableName']
    hiveTempTableName=global_conf[job]['hiveTempTableName']
    workflowName=global_conf[job]['workflowName']+hiveTableName
    triggerLocation=triggerLocation+hiveTableName #confirm this?
    localLogPath=localLogPath+hiveTableName #confirm this?
    noOfDays=global_conf[job]['noOfDays']
    acidReplicationFlag=global_conf[job]['acidReplicationFlag']
    ddl_script_path=global_conf[job]['ddl_script_path']
    cleanUpLog=global_conf[job]['cleanUpLog']

    ddl_hive_table=BashOperator(
                    task_id="ddl_create",
                    bash_command="kinit -kt /gmail/servicekeys/assortmnt_merch/svcmapprdrw.keytab svcmapprdrw@gmail.COM;/usr/bin/beeline -u "+sourceJDBCURL+" -n " +applicationUserName+" --hivevar hive_db="+hiveDBName+" --hivevar hive_table="+hiveTableName+" --hivevar hive_hdfs_path="+hdfsBaseDataLocation+"/"+hiveTableName+" -f "+projectDeploymentDir+ddl_script_path
                )
    sqoop = BashOperator(
                    task_id=job + '_sqoopIngestion',
                    bash_command=f"""{sqoopIngestionScript} {sourceSystem} {tableType} {sourceDBName} {sourceTableName} {hiveDBName} {hiveTableName} {hiveStagedDBName} {hiveStagedTableName} {tableDataLocation} {stageTableDataLocation} {splitByColName} {numMappers} {fieldsTerm} {sqoopWhereColName} {partitionColName} {workflowName} {queueName} {sqoopSelectQuery} {projectDeploymentDir} {localLogPath} {sourceJDBCURL} {environment} {tableLoadType} {instanceDate} {sqoopValidationScript} {sqoopOptionsFile} {workflowUserMappingFile} {userLookupScript} {loggingScript} {applicationUserName} {sourceRowCountQuery} {triggerLocation} {acidReplicationFlag} {hiveTempTableName} {noOfDays} {tableSensitivity} {partitionColValue} {ddl_script_path} {decrypt_script} {private_key} {teradata_pswd} {hostName} """)

    branch_task = BranchPythonOperator(
                    task_id='decision-to-distcp',
                    python_callable=_dummy_test,
                    op_kwargs={'key1': 'teradata_import_job.','key2':job},
                    do_xcom_push=False)

    distcp_type = BranchPythonOperator(task_id='distcp_type',
                                       python_callable=_table_type,
                                       op_kwargs={'key1':tableType},
                                       do_xcom_push=False
                                       )

    nonpartition_distcp_node = BashOperator(
                    task_id='nonpartition_distcp_node',
                    bash_command="kinit -kt /gmail/servicekeys/assortmnt_merch/svcmapprdrw.keytab svcmapprdrw@gmail.COM; hadoop distcp -pb -delete -overwrite -m "+numMappers+" "+nameNode+hdfsBaseDataLocation+hiveTableName+" "+targetNameNode+hdfsBaseDataLocation+hiveTableName
                    )
    partition_distcp_node = BashOperator(
                    task_id='partition_distcp_node',
                    bash_command="kinit -kt /gmail/servicekeys/assortmnt_merch/svcmapprdrw.keytab svcmapprdrw@gmail.COM; hadoop distcp -skipcrccheck -delete -update -m "+numMappers+" "+nameNode+hdfsBaseDataLocation+hiveTableName+" "+targetNameNode+hdfsBaseDataLocation+hiveTableName
                    )

    addpartition = BashOperator(
                    task_id='addpartition',
                    bash_command=f"""{addPartitionsScript} {applicationUserName} {targetHostName} {hiveDBName} {hiveTableName} {targetJDBCURL} {partitionColName} {localLogPath} {loggingScript} {instanceDate} {tableType} {environment} """,
                    trigger_rule='one_success'
                    )
    cleanup = BashOperator(
                    task_id='cleanup',
                    bash_command=f"""{ingestionCleanupScript} {localLogPath} {logRetentionDuration} {loggingScript} {cleanUpLog} {instanceDate} """
                    )

dummy_start_task >> ddl_hive_table >> sqoop >> branch_task >> [cleanup, distcp_type]
distcp_type >> [nonpartition_distcp_node, partition_distcp_node] >> addpartition >> cleanup >> dummy_end_task
