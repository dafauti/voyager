#!/usr/bin/env bash
export KRB5CCNAME=${HOME}/krb5cc_svcmapprdrw
kinit -kt /gmail/servicekeys/assortmnt_merch/svcmapprdrw.keytab svcmapprdrw@gmail.COM
#######################################################################################################################
#Script Name : addPartitions.sh
#Description : Script to add partition on given hive table by running msck repair command also getting statistics of table to metastore by running Analyze command.
#Scenario    : Partitioned Tables
########################################################################################################################
set -x

#--------------------------------------------------------------------------------------------------------------
# Arguments :
#--------------------------------------------------------------------------------------------------------------

user=$1
edgeNode=$2
hiveDb=$3
hiveTable=$4
JdbcURL=$5
partitionCol=$6
localLogPath=$7
loggingScript=$8
instanceDate=${9}
logName=${10}
#--------------------------------------------------------------------------------------------------------------
# Sourcing Logging utility script having functions : infoLog, errorLog, debugLog, warningLog
#--------------------------------------------------------------------------------------------------------------



logFileNameVar=$logName

source ${loggingScript} ${localLogPath} ${logFileNameVar} ${instanceDate}

logFileName=$(logFileName)

chmod 755 $logFileName


infoLog "--------------PARAMETERS PASSED FROM OOZIE -------------------"
infoLog "user                           : $user"
infoLog "edgeNode                       : $edgeNode"
infoLog "DB name                        : $hiveDb"
infoLog "TableName                      : $hiveTable"
infoLog "Log Name                       : $logName"
infoLog "JdbcURL                        : $JdbcURL"
infoLog "partitionCol                   : $partitionCol"
infoLog "localLogPath                   : $localLogPath"
infoLog "loggingScript                  : $loggingScript"
infoLog "instanceDate                   : $instanceDate"
infoLog "logName                        : $logName"



infoLog "Adding partitions to hive table ${hiveDb}.${hiveTable} by running msck repair"
ssh ${user}@${edgeNode} "/usr/bin/beeline -u '${JdbcURL}' -n ${user} -e 'msck repair table ${hiveDb}.${hiveTable};' " >> ${logFileName} 2>&1
    if [[ $? -ne 0 ]];then
        errorLog "Error while performing Msck repair on table ${hiveDb}.${hiveTable} "
              exit 1;
    else
              infoLog "Msck repair performed successfully on table ${hiveDb}.${hiveTable}"
    fi

infoLog "Getting statistics of partitioned hive table ${hiveDb}.${hiveTable} by running Analyze command on partitions"
ssh ${user}@${edgeNode} "/usr/bin/beeline -u '${JdbcURL}' -n ${user} -e 'analyze table ${hiveDb}.${hiveTable} PARTITION($partitionCol) compute statistics;' " >> ${logFileName} 2>&1
    if [[ $? -ne 0 ]];then
        errorLog "Error while performing Analyse on table ${hiveDb}.${hiveTable} "
                  exit 1;
              else
                  infoLog "Analyse performed successfully on table ${hiveDb}.${hiveTable}"
    fi
