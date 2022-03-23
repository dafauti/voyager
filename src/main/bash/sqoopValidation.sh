#!/usr/bin/env bash
export KRB5CCNAME=${HOME}/krb5cc_svcmapprdrw
kinit -kt /gmail/servicekeys/assortmnt_merch/svcmapprdrw.keytab svcmapprdrw@gmail.COM
#Script Name : sqoopIngestion.sh
#Version #   : 1.0
#Description : Script which calls functions which returns source system table(teradata, orcalse, DB2) and Hive table row counts
########################################################################################################################
#set -x

#--------------------------------------------------------------------------------------------------------------
# Variable initialization
#--------------------------------------------------------------------------------------------------------------

SqoopUserName=$1
Password=$2
HadoopUserName=$3
SqoopOptionFile=$4
HiveDBName=$5
HiveTableName=$6
BeelineJdbcURL=$7
SourceRowCountQuery="$8"
loggingScript=$9
localLogPath=${10}
instanceDate=${11}

#--------------------------------------------------------------------------------------------------------------
# Logging Script : Sourcing Logging utility script having functions : infoLog, errorLog, debugLog, warningLog
#--------------------------------------------------------------------------------------------------------------

#source ${loggingScript} ${localLogPath} ${HiveTableName} ${instanceDate}

#logFileName=$(logFileName)

#--------------------------------------------------------------------------------------------------------------
# Function : methods for source systems and Hive table row count calculation and return the results
#--------------------------------------------------------------------------------------------------------------

sqoopTableRecordCount() {

    #row_count=$(sqoop eval --options-file $SqoopOptionFile --username $SqoopUserName --password $Password --query "${SourceRowCountQuery}" )
    row_count=$(sqoop eval --options-file $SqoopOptionFile --username $SqoopUserName --password $Password --query "${SourceRowCountQuery}" )
    source_row_count=$(echo ${row_count} | awk -F[\|] '{print $4}' | sed "s/^[[:space:]]//g" | sed "s/[[:space:]]$//g")
    echo ${source_row_count}
}

hiveTableRecordCount() {

    row_count=$(/usr/bin/beeline --silent=true -u ${BeelineJdbcURL} -n ${HadoopUserName} -e "USE ${HiveDBName};SELECT COUNT(*) FROM ${HiveDBName}.${HiveTableName} ;")

    target_row_count=$(echo ${row_count} | awk -F[\|] '{print $4}' | sed "s/^[[:space:]]//g" | sed "s/[[:space:]]$//g")

    echo ${target_row_count}
}

hiveFinalTableRecordCount() {

    row_count=$(/usr/bin/beeline --silent=true -u ${BeelineJdbcURL} -n ${HadoopUserName} -e "USE ${HiveDBName};SELECT COUNT(*) FROM ${HiveDBName}.${HiveTableName} ;")

    hive_final_row_count=$(echo ${row_count} | awk -F[\|] '{print $4}' | sed "s/^[[:space:]]//g" | sed "s/[[:space:]]$//g")

    echo ${hive_final_row_count}
}
