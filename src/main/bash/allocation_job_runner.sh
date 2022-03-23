#!/usr/bin/env bash

#/gmail/apps/dpp-canada-app/bash/generic/allocation_job_runner.sh DppCanadaAllocation batch 2048 1 10G 20G 256M true 3 60 20G "/gmail/logs/dpp-canada-app/" gmail_allocation_control.conf lca_src_sales_master_round lca_src_allocation dpp_canada 2018-10-02 2019-10-02 INFO Merch_Assortment /gmail/servicekeys/assortmnt_merch/svcmapprdrw.keytab svcmapprdrw@gmail.COM
# Variables
export KRB5CCNAME=${HOME}/krb5cc_svcmapprdrw
kinit -kt /gmail/servicekeys/assortmnt_merch/svcmapprdrw.keytab svcmapprdrw@gmail.COM

COMPONENT_NAME=${1}
echo "Component Name : "${COMPONENT_NAME}
MEMORY_OVERHEAD=${2}
echo "Memory Overhead : "${MEMORY_OVERHEAD}
DRIVER_CORES=${3}
echo "Driver Cores : "${DRIVER_CORES}
DRIVER_MEMORY=${4}
echo "Driver Memory : "${DRIVER_MEMORY}
DRIVER_MAX_RESULT_SIZE=${5}
echo "Driver Max Result Size : "${DRIVER_MAX_RESULT_SIZE}
KRYOSERIALIZER_BUFFER_MAX=${6}
echo "Kryo Serializer Buffer Max : "${KRYOSERIALIZER_BUFFER_MAX}
ORC_ENABLED=${7}
echo "Orc Enabled : "${ORC_ENABLED}
EXECUTOR_CORES=${8}
echo "Executor Cores : "${EXECUTOR_CORES}
EXECUTOR_INSTANCES_NUM=${9}
echo "Executor Instances : "${EXECUTOR_INSTANCES_NUM}
EXECUTOR_MEMORY_NUM=${10}
echo "Exeuctor Memory : "${EXECUTOR_MEMORY_NUM}
LOGFILE_LOCAL_BASE_DIR=${11}
echo "Log File Base Directory : "${LOGFILE_LOCAL_BASE_DIR}
ALC_FILE_NAME=${12}
echo "Allocation Mapping File Name : "${ALC_FILE_NAME}
ALC_MASTER_DATASET=${13}
echo "Allocation Master Table Name : "${ALC_MASTER_DATASET}
ALLOCATED_OUTPUT_TABLE_NAME=${14}
echo "Allocation Output Table Name : "${ALLOCATED_OUTPUT_TABLE_NAME}
DPP_HIVE_DB_NAME=${15}
echo "Hive Database : "${DPP_HIVE_DB_NAME}
START_DATE=${16}
echo "Start Date : "${START_DATE}
END_DATE=${17}
echo "End Date : "${END_DATE}
LOG_LEVEL=${18}
echo "Log Level : "${LOG_LEVEL}
QUEUE_NAME=${19}
echo "Queue Name : "${QUEUE_NAME}
KEYTAB_LOCATION=${20}
echo "Keytab File Location : "${KEYTAB_LOCATION}
KEYTAB_PRINCIPAL=${21}
echo "Keytab Principal : "${KEYTAB_PRINCIPAL}
EXECUTION_DATE=${22}
echo "Execution Date : "${EXECUTION_DATE}

####To generate the Year filter for Aggregation script
START_YEAR=`echo $START_DATE | awk -F\- '{print $1}'`
END_YEAR=`echo $END_DATE | awk -F\- '{print $1}'`
delim=""
joined_arg=""
for ((i=$START_YEAR; i<=$END_YEAR; i++))
do
  joined_arg="$joined_arg$delim$i"
  delim=","
done
YEAR_ARG="("$joined_arg")"
echo "Year filter for Aggregation script==="${YEAR_ARG}

######################################################## DATA PREP SCRIPT ###################################################
#INSTANCE_DATE=$(date '+%Y-%m-%d-%H-%M-%S')
mkdir -p ${LOGFILE_LOCAL_BASE_DIR}/${EXECUTION_DATE}
chmod 777 ${LOGFILE_LOCAL_BASE_DIR}/${EXECUTION_DATE}
spark_log_file=${LOGFILE_LOCAL_BASE_DIR}/${EXECUTION_DATE}/spark_logs_${COMPONENT_NAME}.log
function get_canonical_dir() {
  target="$1"
  canonical_name=`readlink -f ${target} 2>/dev/null`
  if [[ $? -eq 0 ]]; then
    canonical_dir=`dirname $canonical_name`
    echo ${canonical_dir}
    return
  fi

  # Mac has no readlink -f
  cd `dirname ${target}`
  target=`basename ${target}`

  # chase down the symlinks
  while [ -L ${target} ]; do
    target=`readlink ${target}`
    cd `dirname ${target}`
    target=`basename ${target}`
  done

  canonical_dir=`pwd -P`
  ret=${canonical_dir}
  echo $ret
}

bin=$(get_canonical_dir "$0")
BASE_DIR="`echo $bin | sed -e 's/\/bash\/generic//'`"
CONFIG_DIR=$BASE_DIR/config
JAR_DIR=$BASE_DIR/lib
export LIBJARS=`echo "$JAR_DIR"/*.jar | sed 's/ /,/g'`

JAR_FILE=`ls $JAR_DIR/DppCanada-*-SNAPSHOT.jar`
CLASS_NAME=com.gmail.bigdata.dpp.driver.AllocationJobDriver




{
/usr/bin/spark-submit --verbose \
--master yarn \
--name dpp_canada_${COMPONENT_NAME}_app \
--deploy-mode cluster \
--principal ${KEYTAB_PRINCIPAL} \
--keytab ${KEYTAB_LOCATION} \
--conf spark.yarn.tags=Dpp_Canada_Allocation_Job \
--conf spark.yarn.queue=${QUEUE_NAME} \
--conf spark.executor.memoryOverhead=${MEMORY_OVERHEAD} \
--conf spark.driver.cores=${DRIVER_CORES} \
--conf spark.driver.memory=${DRIVER_MEMORY} \
--conf spark.driver.maxResultSize=${DRIVER_MAX_RESULT_SIZE} \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.kryoserializer.buffer.max=${KRYOSERIALIZER_BUFFER_MAX} \
--conf spark.sql.orc.enabled=${ORC_ENABLED} \
--conf spark.executor.cores=${EXECUTOR_CORES} \
--conf spark.memory.storageFraction=0.3 \
--conf spark.executor.instances=${EXECUTOR_INSTANCES_NUM} \
--conf spark.executor.memory=${EXECUTOR_MEMORY_NUM} \
--conf spark.sql.sources.partitionOverwriteMode="dynamic" \
--conf spark.sql.broadcastTimeout=2000 \
--conf spark.sql.autoBroadcastJoinThreshold=3098576 \
--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=./log4j-driver.properties -Dvm.logging.level=${LOG_LEVEL} -Dvm.logging.name=${COMPONENT_NAME} -Dconfig.file=./${ALC_FILE_NAME}" \
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=./log4j-executor.properties -Dvm.logging.level=${LOG_LEVEL} -Dvm.logging.name=${COMPONENT_NAME} -Dconfig.file=./${ALC_FILE_NAME}" \
--files "${CONFIG_DIR}/allocation_mapping/${ALC_FILE_NAME}#${ALC_FILE_NAME},$CONFIG_DIR/logging_properties/log4j-driver.properties#log4j-driver.properties,$CONFIG_DIR/logging_properties/log4j-executor.properties#log4j-executor.properties" \
--jars $LIBJARS \
--class ${CLASS_NAME} \
  ${JAR_FILE} \
--componentName ${COMPONENT_NAME} \
--allocationMasterDataset ${ALC_MASTER_DATASET} \
--allocationTableName ${ALLOCATED_OUTPUT_TABLE_NAME} \
--dbName ${DPP_HIVE_DB_NAME} \
--yearRange ${YEAR_ARG}
} >> ${spark_log_file} 2>&1

status=$?
applicationId=$(cat ${spark_log_file} | grep "Submitted application" | cut -d' ' -f7)

if [[ -z "$applicationId" ]];then
     echo "ERROR   Error occurred while running spark-code." >> ${spark_log_file}
     exit 1
   else
    sleep 30s
    echo "Extracted applicationId : $applicationId" >> ${spark_log_file}

    echo "Extracting driver log to : ${LOGFILE_LOCAL_BASE_DIR}/${EXECUTION_DATE}/${COMPONENT_NAME}-$applicationId-driver.log" >> ${spark_log_file}
    yarn logs --applicationId $applicationId -log_files ${COMPONENT_NAME}-driver.log > ${LOGFILE_LOCAL_BASE_DIR}/${EXECUTION_DATE}/${COMPONENT_NAME}-$applicationId-driver.log

    echo "Extracting executor log to : ${LOGFILE_LOCAL_BASE_DIR}/${EXECUTION_DATE}/${COMPONENT_NAME}-$applicationId-executor.log" >> ${spark_log_file}
    yarn logs --applicationId $applicationId -log_files ${COMPONENT_NAME}-executor.log > ${LOGFILE_LOCAL_BASE_DIR}/${EXECUTION_DATE}/${COMPONENT_NAME}-$applicationId-executor.log

    if [[ $status != 0 ]];then
       echo "ERROR   Error occurred while running spark-code." >> ${spark_log_file}
       exit 1
    fi
fi






