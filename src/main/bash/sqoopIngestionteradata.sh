#!/bin/bash
ssh -i ${40} ${30}@${42}.gmail.com
export KRB5CCNAME=${HOME}/krb5cc_svcmapprdrw
kinit -kt /gmail/servicekeys/assortmnt_merch/svcmapprdrw.keytab svcmapprdrw@gmail.com
klist
whoami

#######################################################################################################################
#Script Name : sqoopIngestion.sh
#Version #   : 1.0
#Author Name : Anjul Tiwari
#Date        : October 20, 2017
#Description : Script to run the sqoop job which imports selected columns from partitioned/Non-partitioned oracle, teradata, DB2 tables and runs the script validations
#Scenario    : Full Load / Partial Load (Date range based load)
########################################################################################################################
set -x

#--------------------------------------------------------------------------------------------------------------
# Ingestion methods for different source systems
#--------------------------------------------------------------------------------------------------------------

# Source system : teradata
# Note: This should be independent of any declaration, thats when it will be called generic
teradata_sqoop(){

    sqoop import \
        -Dmapreduce.job.queuename=$1 \
        -Dmapreduce.task.timeout=3000000 \
        -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
    	  --options-file $2 \
        --username $3 --password $4 \
        --query "$5" \
        --hcatalog-database $6 \
        --hcatalog-table $7 \
        --split-by $8 -m $9 \
        --fields-terminated-by ${10} \
        --hive-delims-replacement " " \
        --fetch-size=100000 ${12} >> ${11} 2>&1;

        local status_code=$?
        echo $status_code

}


oracle_sqoop(){

    sqoop import \
        -Dmapreduce.job.queuename=$1 \
        -Dmapreduce.task.timeout=3000000 \
        -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
        --options-file $2 \
        --username $3 --password $4 \
        --query "$5" \
        --relaxed-isolation \
        --hcatalog-database $6 \
        --hcatalog-table $7	\
        --split-by $8 -m $9 \
        --fields-terminated-by ${10} \
        --fetch-size=100000 ${12} >> ${11} 2>&1;

        local status_code=$?
        echo $status_code

}

db2_sqoop(){

    sqoop import \
        -Dmapreduce.job.queuename=$1 \
        -Dmapreduce.task.timeout=3000000 \
        -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
        --options-file $2 \
        --username $3 --password $4 \
        --query "$5" \
        --relaxed-isolation \
        --hcatalog-database $6 \
        --hcatalog-table $7	\
        --split-by $8 -m $9 \
        --fields-terminated-by ${10} \
        --fetch-size=100000 ${12} >> ${11} 2>&1;


        local status_code=$?
        echo $status_code
}

db2_sqoop_UR(){

    sqoop import \
        -Dmapreduce.job.queuename=$1 \
        -Dmapreduce.task.timeout=3000000 \
        -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
        --options-file $2 \
        --username $3 --password $4 \
        --query "$5" \
        --hcatalog-database $6 \
        --hcatalog-table $7	\
        --split-by $8 -m $9 \
        --fields-terminated-by ${10} \
        --fetch-size=100000 \
        --boundary-query "${13}" \
        --connection-param-file ${14} ${12} ${15}>> ${11} 2>&1;

        local status_code=$?
        echo $status_code
}


#--------------------------------------------------------------------------------------------------------------
# Ingestion methods for different source systems ends here
#--------------------------------------------------------------------------------------------------------------

source_sqoop_validations()
{

#--------------------------------------------------------------------------------------------------------------
# Validation Script : Sourcing Sqoop Row count validation Script
#--------------------------------------------------------------------------------------------------------------

source ${1} ${2} ${3} ${4} ${5} ${6} ${7} ${8} "${9}" ${10} ${11} ${12}


}

#--------------------------------------------------------------------------------------------------------------
# Ingestion specific parameter initialization
# These parameters are flowing from job.properties -> coordinator.xml -> workflow.xml -> current script
#--------------------------------------------------------------------------------------------------------------

# Source DB, Target DB and Staging DB
sourceSystem=$1                 # Name of source system e.g. oracle, teradata, db2
tableType=$2                    # Table type : partitioned(P) or non-partitioned(NP)
sourceDBName=$3                 # Source system db name
sourceTableName=$4              # Source system table name
hiveDBName=$5                   # Target db name
hiveTableName=$6                # Target table name
hiveStagedDBName=$7             # Stage db name
hiveStagedTableName=$8          # Stage table name

# Sqoop query related params
tableDataLocation=$9            # Hdfs location of Target Hive Table
stageTableDataLocation=${10}            # Hdfs location of Target Hive Table
splitByColName=${11}            # Column name (numeric type) to split sqoop query on
numMappers=${12}                # Number of Mappers needed
fieldsTerm=${13}                # Field terminator
sqoopWhereColName=${14}
partitionColName=${15}
workflowName=${16}              # Workflow Name
queueName=${17}                 # Queue name
sqoopSelectQuery=${18}              # Sqoop columns

# Project Path, Log Path, jdbc url and environment
projectDeploymentDir=${19}      # Project Deployment dir location on Edge node
localLogPath=${20}              # Local log dir
sourceJdbcURL=${21}             # Beeline connection string
environment=${22}               # Environment name : dec, qa, prod
tableLoadType=${23}             # Source Table load type : Full, partial
instanceDate=${24}              # Instance date from coordinator parameter

#Utilities-Based parameters
sqoopValidationScript=${25}     # Validation script to check if source and target table counts match
sqoopOptionsFile=${26}          # Connection driver for sqoop to connect to source system
workflowUserMappingFile=${27}   # Script which contains all the User name and workflow names
userLookupScript=${28}          # Script which gets the user name wrt workflow name
loggingScript=${29}             # Script to have all logging functions

applicationUserName=${30}       # hadoop username : hdpbatch
sourceRowCountQuery=${31}       # Row Count Query on Source System table.
triggerLocation=${32}           # hdfs location of trigger directory created by zeke jobs
acidReplicationFlag=${33}       # Whether table is Acid or not table type
hiveTempTableName=${34}         # Acid temp table name
noOfDays=${35}                  # No of days for incremental pull
tableSensitivity=${36}          # like gmail_tables, cust_tables
partitionColValue=${37}          # like gmail_tables, cust_tables
ddl_script_path=${38}
decrypt_script=${39}
teradata_pswd=${41}

successFlag=$(date -d "${instanceDate}" +%Y%m%d)

#--------------------------------------------------------------------------------------------------------------
# Logging Script : Sourcing Logging utility script having functions : infoLog, errorLog, debugLog, warningLog
#--------------------------------------------------------------------------------------------------------------

source ${loggingScript} ${localLogPath} ${hiveTableName} ${instanceDate}

logFileName=$(logFileName)

infoLog "--Start of Sqoop Ingestion Job--"


#--------------------------------------------------------------------------------------------------------------
# Sqoop ingestion : Starts here.
#--------------------------------------------------------------------------------------------------------------


infoLog "Source System          : ${sourceSystem}"
infoLog "table type             : ${tableType}"
infoLog "Source DB Name         : ${sourceDBName}"
infoLog "Source Table Name      : ${sourceTableName}"
infoLog "Hive DB Name           : ${hiveDBName}"
infoLog "Hive Table Name        : ${hiveTableName}"
infoLog "Hive Staged DB Name    : ${hiveStagedDBName}"
infoLog "tableDataLocation       : ${tableDataLocation}"
infoLog "stageTableDataLocation : ${stageTableDataLocation}"
infoLog "splitByColName         : ${splitByColName}"
infoLog "numMappers             : ${numMappers}"
infoLog "fieldsTerm             : ${fieldsTerm}"
infoLog "sqoopWhereColName      : ${sqoopWhereColName}"
infoLog "partitionColName       : ${partitionColName}"
infoLog "workflowName           : ${workflowName}"
infoLog "queueName              : ${queueName}"
infoLog "sqoopSelectQuery        : ${sqoopSelectQuery}"
infoLog "projectDeploymentDir   : ${projectDeploymentDir}"
infoLog "localLogPath           : ${localLogPath}"
infoLog "sourceJdbcURL          : ${sourceJdbcURL}"
infoLog "environment            : ${environment}"
infoLog "tableLoadType          : ${tableLoadType}"
infoLog "instanceDate           : ${instanceDate}"
infoLog "applicationUserName    : ${applicationUserName}"
infoLog "sourceRowCountQuery    : ${sourceRowCountQuery}"
infoLog "triggerLocation        : ${triggerLocation}"
infoLog "acidReplicationFlag    : ${acidReplicationFlag}"
infoLog "hiveTempTableName      : ${hiveTempTableName}"
infoLog "noOfDays               : ${noOfDays}"
infoLog "tableSensitivity       : ${tableSensitivity}"
infoLog "partitionColValue      : ${partitionColValue}"

#--------------------------------------------------------------------------------------------------------------
# Sqoop ingestion : Pre-check of existence of required files
#--------------------------------------------------------------------------------------------------------------
infoLog "User Lookup Script     : ${userLookupScript}"
infoLog "Workflow User File     : ${workflowUserMappingFile}"
infoLog "SqoopValidation Script : ${sqoopValidationScript}"
infoLog "SqoopOptions File      : ${sqoopOptionsFile}"
infoLog "Logging Script         : ${loggingScript}"
infoLog "Log File Name          : ${logFileName}"

infoLog "Validating the pre-requisite files existences."

if [[ -e  "${userLookupScript}" && -e "${workflowUserMappingFile}" && -e "${sqoopValidationScript}" && -e "${sqoopOptionsFile}" && -e "${loggingScript}" ]];then
    infoLog "All pre-requisite files exist."
    runScript="YES"
else
    errorLog "Some pre-requisite files are missing. Hence cannot proceed further.."
  	runScript="NO"
fi

#--------------------------------------------------------------------------------------------------------------
# Sqoop ingestion : Pre validation and directory preparation
#--------------------------------------------------------------------------------------------------------------

if [ "$runScript" == "YES" ];then
	infoLog "Checking whether number of arguments passed are correct."
	if [ $# == 0 ];then
		exitCode=$(errorLog "No arguments are provided.")
	elif [ $# -lt 37 ];then
		exitCode=$(errorLog "Have received $# arguments,expecting minimum 37 no.of arguments")
	else
        infoLog "Dropping Managed stage table ${hiveStagedDBName}.${hiveStagedTableName} if exists"
        /usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName} -e  "DROP TABLE IF exists ${hiveStagedDBName}.${hiveStagedTableName};"
        /usr/bin/beeline -u ${sourceJdbcURL} -n  ${applicationUserName} --hivevar hive_db=${hiveStagedDBName} --hivevar hive_table=${hiveStagedTableName} --hivevar hive_hdfs_path=${stageTableDataLocation}/${hiveStagedTableName} -f ${projectDeploymentDir}${ddl_script_path}
         if [[ $? -ne 0 ]];
				      then
				      errorLog "Error creating hive table  ${hiveStagedDBName}.${hiveStagedTableName}"
				      exit 1;
				 fi
        infoLog "Convert external table to managed table"
        /usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName} -e  "ALTER TABLE  ${hiveStagedDBName}.${hiveStagedTableName} SET TBLPROPERTIES('EXTERNAL'='False');"
        if [ "$environment" == "dev" ]
            then
              userName=FPASYS
              password=$(cat /gmail/apps/edl_rdbms_ingestion/src/main/utilities/connectors/teradata/teradataconnector/dev/password.txt)
        else
            klist
            infoLog "crednetial details ${decrypt_script} ${teradata_pswd}  $userName"
            userName=$( ${userLookupScript} -f ${workflowUserMappingFile} -t ${workflowName} )
            password=$(sudo -u hdpenc ${decrypt_script} -f ${teradata_pswd} -u $userName )
        fi

       #--------------------------------------------------------------------------------------------------------------
       # Sqoop Query Generation  :Generating sqoop query
       #--------------------------------------------------------------------------------------------------------------
        ###below vairable is used in partition insert overwrite because of where condition added to sqoopSelectQuery in further steps
        sqoopSelectQueryInitial="${sqoopSelectQuery},${partitionColName}"
        infoLog "Further Sqoop conditions are decided based on load type "

        #--------------------------------------------------------------------------------------------------------------
        # Validation Script : Sourcing Sqoop Row count validation Script
        #--------------------------------------------------------------------------------------------------------------
        if [[ ${tableLoadType} == 'FULL' &&  "${tableType}" == 'NP' ]]
        then

            source_sqoop_validations ${sqoopValidationScript} ${userName} ${password} ${applicationUserName} ${sqoopOptionsFile} ${hiveStagedDBName} ${hiveStagedTableName} ${sourceJdbcURL} "${sourceRowCountQuery}" ${loggingScript} ${localLogPath} ${instanceDate}
            sqoopSelectQuery="${sqoopSelectQuery} from ${sourceDBName}.${sourceTableName} where \$CONDITIONS" >> ${logFileName} 2>&1;
        elif [[ ${tableLoadType} == 'PARTIAL' &&  "${tableType}" == 'NP' ]]
        then
            infoLog " Taking Hive record count before sqooping the data from source "
            hiveRecordCount=$(/usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName}  -e "select count(*) from ${hiveDBName}.${hiveTableName}") >> ${logFileName} 2>&1;
            hiveRecordCount=$(/usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName}  -e "select count(*) from ${hiveDBName}.${hiveTableName}")
            infoLog "Hive table record count before sqoop is : $hiveRecordCount "
            infoLog "Taking Maximum Timestamp from  table ${hiveDBName}.${hiveTableName} based on ${sqoopWhereColName}"
            maxTS=$(/usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName} -e "select max(${sqoopWhereColName}) from ${hiveDBName}.${hiveTableName}") >> ${logFileName} 2>&1;
            maxTS=$(/usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName} -e "select max(${sqoopWhereColName}) from ${hiveDBName}.${hiveTableName}")
            maxTS_F=$( echo $maxTS | awk -F[\|] '{print $4}' | awk '{print $1" "$2}' ) >> ${logFileName} 2>&1;
            maxTS_NULL=$( echo $maxTS | awk -F[\|] '{print $4}' | awk '{print $1}' ) >> ${logFileName} 2>&1;


            if [[ "$maxTS_NULL" == 'NULL' ]];then
                errorLog "Found Invalid  $maxTS in Hive table ${hiveDBName}.${hiveTableName}, please recheck the Hive Table  Column max value" >> ${logFileName} 2>&1;
                errorLog "Found Invalid  $maxTS in Hive table ${hiveDBName}.${hiveTableName}, please recheck the Hive Table  Column max value"
                errorLog "Can not proceed to next step, exiting the job with exit code 1" >> ${logFileName} 2>&1;
                exit 1;
            fi
            maxHiveTs=$(date -d "${maxTS_F} 1 hours ago" +"%Y-%m-%d %H:%M:%S.%2N")
            infoLog "Maximum Time stamp considered for ${hiveDBName}.${hiveTableName} is: "${maxTS_F}" "
            whereClause=" where $sqoopWhereColName <= TO_DATE('$maxHiveTs', 'YYYY-MM-DD HH24:MI:SS.FF') and $sqoopWhereColName >= TO_DATE('$maxHiveTs', 'YYYY-MM-DD HH24:MI:SS.FF') " >> ${logFileName} 2>&1;
            whereClause=" where $sqoopWhereColName <= TO_DATE('$maxHiveTs', 'YYYY-MM-DD HH24:MI:SS.FF') and $sqoopWhereColName >= TO_DATE('$maxHiveTs', 'YYYY-MM-DD HH24:MI:SS.FF') "
            sourceRowCountQuery="${sourceRowCountQuery} ${whereClause};"
            sqoopSelectQuery="${sqoopSelectQuery} from ${sourceDBName}.${sourceTableName} ${whereClause} AND \$CONDITIONS"
            source_sqoop_validations ${sqoopValidationScript} ${userName} ${password} ${applicationUserName} ${sqoopOptionsFile} ${hiveStagedDBName} ${hiveStagedTableName} ${sourceJdbcURL} "${sourceRowCountQuery}" ${loggingScript} ${localLogPath} ${instanceDate}

         elif  [[ ${tableLoadType} == 'FULL' &&  "${tableType}" == 'DP' ]]
         then
            infoLog "considering this as ${tableType} table "
            source_sqoop_validations ${sqoopValidationScript} ${userName} ${password} ${applicationUserName} ${sqoopOptionsFile} ${hiveStagedDBName} ${hiveStagedTableName} ${sourceJdbcURL} "${sourceRowCountQuery}" ${loggingScript} ${localLogPath} ${instanceDate}
            sqoopSelectQuery="${sqoopSelectQueryInitial} from ${sourceDBName}.${sourceTableName} where \$CONDITIONS " >> ${logFileName} 2>&1;
            infoLog "SOUCRE COUNT.  ${sqoopSelectQuery}"
        elif  [[ ${tableLoadType} == 'FULL' &&  "${tableType}" == 'SP' ]]
         then
            infoLog "If source table sqoop is not based on date range and hive table is partition."
		    infoLog "Then setting partition Key and Value of stage table before sqooping data into it "
		    hcatKeyValue="--hcatalog-partition-keys $partitionColName --hcatalog-partition-values $partitionColValue "
		    infoLog "Hcatalog partition key and value: $hcatKeyValue"
		    sqoopSelectQuery="${sqoopSelectQuery} from ${sourceDBName}.${sourceTableName} where \$CONDITIONS " >> ${logFileName} 2>&1;
		    source_sqoop_validations ${sqoopValidationScript} ${userName} ${password} ${applicationUserName} ${sqoopOptionsFile} ${hiveStagedDBName} ${hiveStagedTableName} ${sourceJdbcURL} "${sourceRowCountQuery}" ${loggingScript} ${localLogPath} ${instanceDate}

         elif  [[ ${tableLoadType} == 'PARTIAL' &&  "${tableType}" == 'DP' ]]
         then
            infoLog "Taking Maximum ${sqoopWhereColName} from hive table: ${hiveDBName}.${hiveTableName}"
            infoLog "considering this as ${tableType} table "
            maxTS=$(/usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName} -e "select max(${sqoopWhereColName})from ${hiveDBName}.${hiveTableName};")
            maxTS_F=$( echo $maxTS | awk -F[\|] '{print $4}' | awk '{print $1" "$2}' )
            maxTS_NULL=$( echo $maxTS | awk -F[\|] '{print $4}' | awk '{print $1}' )
            if [[ "$maxTS_NULL" == 'NULL' ]];then
                errorLog "Found Invalid  $maxTS in Hive table ${hiveDBName}.${hiveTableName}, please recheck the Hive Table  Column max value"
                errorLog "Can not proceed to next step, exiting the job with exit code 1"
                exit 1;
               elif [[ "$noOfDays" -gt 0 ]];then
               infoLog "As per business logic subtracting ${noOfDays} days from ${maxTS_F} "
               maxTS_F=$(date -d "${maxTS_F} ${noOfDays} day ago" +"%Y-%m-%d")
               infoLog "Final date considered for is ${maxTS_F} "
            fi
            whereClause=" where ${sqoopWhereColName} >'${maxTS_F}' "
            infoLog "considering maximum value for ${sqoopWhereColName} columns as:  ${maxTS_F} from ${hiveDBName}.${hiveTableName} hive table "
            sourceRowCountQuery="${sourceRowCountQuery} ${whereClause};"
            infoLog "Sqoop row count query is :${sourceRowCountQuery} "
            source_sqoop_validations ${sqoopValidationScript} ${userName} ${password} ${applicationUserName} ${sqoopOptionsFile} ${hiveStagedDBName} ${hiveStagedTableName} ${sourceJdbcURL} "${sourceRowCountQuery}" ${loggingScript} ${localLogPath} ${instanceDate}
            sqoopSelectQuery="${sqoopSelectQuery},${partitionColName} from ${sourceDBName}.${sourceTableName} ${whereClause} AND \$CONDITIONS"
          else
           exit 1;
           errorLog "Please select valid table loadtype and table type to proceed further"


        fi

        #--------------------------------------------------------------------------------------------------------------
        # Sqoop ingestion : Import starts here
        #--------------------------------------------------------------------------------------------------------------

       infoLog "Taking source table ${sourceDBName}.${sourceTableName} row count before sqoop import starts."
       infoLog "Calling function - sqoopTableRecordCount ${userName}  ${applicationUserName} ${sqoopOptionsFile} ${hiveStagedDBName} ${hiveStagedTableName} ${sourceJdbcURL} ${sourceRowCountQuery}"
       source_row_count=$(sqoopTableRecordCount ${userName} ${password} ${applicationUserName} ${sqoopOptionsFile} ${hiveStagedDBName} ${hiveStagedTableName} ${sourceJdbcURL} ${sourceRowCountQuery})

        if [[ -n ${source_row_count} ]]
        then
            infoLog "Source table ${sourceDBName}.${sourceTableName} row count at ${sourceSystem}: ${source_row_count}"
        else
            errorLog "Source table row count is not captured, job would fail, need to rerun it"
            errorLog "Exiting the script ..."
            exit 1;
        fi

        queryString=$(echo ${sqoopSelectQuery}) >> ${logFileName} 2>&1;
        infoLog "Generating query string based on load type"
        infoLog "QueryString:$queryString"
        infoLog "Initiating Sqoop Import for ${sourceDBName}.${sourceTableName} to ${hiveStagedDBName}.${hiveStagedTableName}"

        case "$sourceSystem" in
             "db2") exit_code=$(db2_sqoop ${queueName} ${sqoopOptionsFile} ${userName} ${password} "$queryString" ${hiveStagedDBName} ${hiveStagedTableName} ${splitByColName} ${numMappers} ${fieldsTerm} ${logFileName} ${connectionParamFile} ${additionalSqoopParameters} "$hcatKeyValue")
             ;;
             "db2_UR") exit_code=$(db2_sqoop_UR ${queueName} ${sqoopOptionsFile} ${userName} ${password} "$queryString" ${hiveStagedDBName} ${hiveStagedTableName} ${splitByColName} ${numMappers} ${fieldsTerm} ${logFileName} "$hcatKeyValue"  )
             ;;
             "teradata") exit_code=$(teradata_sqoop ${queueName} ${sqoopOptionsFile} ${userName} ${password} "$queryString" ${hiveStagedDBName} ${hiveStagedTableName} ${splitByColName} ${numMappers} ${fieldsTerm} ${logFileName} "$hcatKeyValue")
             ;;
             "oracle") exit_code=$(oracle_sqoop ${queueName} ${sqoopOptionsFile} ${userName} ${password} "$queryString" ${hiveStagedDBName} ${hiveStagedTableName} ${splitByColName} ${numMappers} ${fieldsTerm} ${logFileName} "$hcatKeyValue")
             ;;
        esac

        infoLog "Exit code from sqoop import command is :$exit_code"

        if [[ "$exit_code" -eq 0 ]]; then
		    infoLog "Matching row count on source table and hive table before processing inserting data to final hive table."
		    target_row_count=$(hiveTableRecordCount ${userName} ${password} ${applicationUserName} ${sqoopOptionsFile} ${hiveStagedDBName} ${hiveStagedTableName} ${sourceJdbcURL} ${sourceRowCountQuery})  >> ${logFileName} 2>&1;

            if [[ ! -n ${target_row_count} ]]
            then
                errorLog "Target table row count is not captured, job is failed"  >> ${logFileName} 2>&1;
                errorLog "Exiting the script ..." >> ${logFileName} 2>&1;
                exit 1;
            fi

		    if [[ "${target_row_count}" != "${source_row_count}" ]]
		    then
		        infoLog "Source table ${sourceDBName}.${sourceTableName} row count       : ${source_row_count}"
		        infoLog "Hive table ${hiveStagedDBName}.${hiveStagedTableName} row count : ${target_row_count}"
		        infoLog "There is data count mismatch between source table ${sourceDBName}.${sourceTableName} and hive table ${hiveStagedDBName}.${hiveStagedTableName} ."


		    else
		        infoLog "Source table ${sourceDBName}.${sourceTableName} row count       : ${source_row_count}"
		        infoLog "Hive table ${hiveStagedDBName}.${hiveStagedTableName} row count : ${target_row_count}"
		        infoLog "Data count is matching or target hive table ${hiveStagedDBName}.${hiveStagedTableName} is having more data than source table ${sourceDBName}.${sourceTableName} which is expected situation and hence proceeding further. "
                infoLog "Started inserting data from staging table into target $hiveDBName.$hiveTableName table."

              fi

               if [[ "${tableLoadType}" == 'FULL' && "${tableType}" == 'NP' ]];
                   then
                    infoLog "Hive Concatenate the Stage table before starting the copy to Final table"
                    /usr/bin/beeline --silent=true -u ${sourceJdbcURL} -n ${applicationUserName} -e "ALTER TABLE ${hiveStagedDBName}.${hiveStagedTableName} CONCATENATE ;" >> ${logFileName} 2>&1;
                    if [[ $? -ne 0 ]];
				             then
                                errorLog "Error Concatenate data from stage table: ${hiveStagedDBName}.${hiveStagedTableName}"
		        	            exit 1;
                             fi
                    infoLog "Started Copying the data (hadoop distcp -pb -delete -overwrite ${stageTableDataLocation}/${hiveStagedTableName} ${tableDataLocation}/${hiveTableName}) from non-partitioned staging table to non-partitioned final table"
                    hadoop distcp -pb -delete -overwrite ${stageTableDataLocation}/${hiveStagedTableName} ${tableDataLocation}/${hiveTableName} >> ${logFileName} 2>&1;
                            if [[ $? -ne 0 ]];
				             then
                                errorLog "Error Inserting data to ${hiveDBName}.${hiveTableName}"
		        	            exit 1;
                             fi
                        hiveFinalTableRecordCount=$(/usr/bin/beeline --silent=true -u ${sourceJdbcURL} -n ${applicationUserName}  -e "select count(*) from ${hiveDBName}.${hiveTableName}") >> ${logFileName} 2>&1;
                        infoLog "Hive stage table ${hiveStagedDBName}.${hiveStagedTableName} row count : ${target_row_count}"
		                infoLog "Hive final table ${hiveDBName}.${hiveTableName} row count : ${hiveFinalTableRecordCount}"
		                    if [[ "${target_row_count}" != "${source_row_count}" ]]
		                    then
		                         errorLog "Count mismatch between  ${hiveStagedDBName}.${hiveStagedTableName} and ${hiveDBName}.${hiveTableName} , Hence failing the job."
                                exit 1;
		                    else
		                        infoLog "Count validation is successful between ${hiveStagedDBName}.${hiveStagedTableName} and ${hiveDBName}.${hiveTableName} ."
                            fi
                        /usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName} -e "analyze table ${hiveDBName}.${hiveTableName} compute statistics; analyze table ${hiveDBName}.${hiveTableName} compute statistics for columns; " >> ${logFileName}  2>&1;
                        hadoop fs -touchz ${tableDataLocation}/${hiveTableName}/_${successFlag}.DONE >> ${logFileName} 2>&1;

                   elif [[ "${tableLoadType}" == 'PARTIAL' && "${tableType}" == 'NP' ]];
                   then
                         infoLog "Incremental logic starts here "
                         infoLog "Gathering statistics for final table before Acid Merge : ${hiveDBName}.${hiveTableName};" >> ${logFileName} 2>&1;
                         /usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName} -e "Analyze table ${hiveDBName}.${hiveTableName} compute statistics; analyze table ${hiveDBName}.${hiveTableName} compute statistics for columns; " >> ${logFileName}  2>&1;
                         infoLog " Proceeding with Acid merge " >> ${logFileName} 2>&1;
                            if [[ $? -ne 0 ]]; then
                            errorLog "Error in Incremental logic hence failing the job"
		        	        exit 1;
                             fi
                        hdpTimestamp=$(date --date '0 min ago' "+%Y-%m-%d %H:%M:%S")
                        infoLog "/usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName} -f ${projectDeploymentDir}/src/main/hive/dmls/${hiveTableName}_dml_scd_1.hql --hivevar hiveDBName=${hiveDBName} --hivevar hiveTableName=${hiveTableName} --hivevar hiveStagedDBName=${hiveStagedDBName} --hivevar hiveStagedTableName=${hiveStagedTableName} --hivevar timestamp='${hdpTimestamp}'"
                        /usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName} -f ${projectDeploymentDir}/src/main/hive/dmls/${hiveTableName}_dml_scd_1.hql --hivevar hiveDBName=${hiveDBName} --hivevar hiveTableName=${hiveTableName} --hivevar hiveStagedDBName=${hiveStagedDBName} --hivevar hiveStagedTableName=${hiveStagedTableName} --hivevar timestamp=${hdpTimestamp} >> ${logFileName}  2>&1;
                         if [[ $? -ne 0 ]];
				                        then
                                        errorLog "Error Mering data to  ${hiveDBName}.${hiveTableName}table"
		        	                    exit 1;
                                        fi
                        hiveFinalTableRecordCount=$(/usr/bin/beeline --silent=true -u ${sourceJdbcURL} -n ${applicationUserName}  -e "select count(*) from ${hiveDBName}.${hiveTableName}") >> ${logFileName} 2>&1;
                        infoLog "Incremental Data successful merged to ${hiveDBName}.${hiveTableName} and count after merge is :${hiveFinalTableRecordCount}."
                        infoLog "Collecting  stats for table: ${hiveDBName}.${hiveDBName} ."
                        /usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName} -e "analyze table ${hiveDBName}.${hiveTableName} compute statistics; analyze table ${hiveDBName}.${hiveTableName} compute statistics for columns; " >> ${logFileName}  2>&1;
                        hadoop fs -touchz ${tableDataLocation}/${hiveTableName}/_${successFlag}.DONE >> ${logFileName} 2>&1;

                    elif [[ "${tableLoadType}" == 'FULL' && "${tableType}" == 'DP' ]];
                    then
                         infoLog "Considering this as historical load for ${hiveDBName}.${hiveTableName}"
                         #infoLog "Started Copying the data (hadoop distcp -delete -overwrite ${tableDataLocation}/${hiveStagedTableName} ${tableDataLocation}/${hiveTableName}) from partitioned staging table to partitioned final table"
                         #hadoop distcp -delete -overwrite ${stageTableDataLocation}/${hiveStagedTableName} ${tableDataLocation}/${hiveTableName} >> ${logFileName} 2>&1;
                         ifoLog "check log ${logFileName}"
                         infoLog "Started inserting the data to  ${hiveDBName}.${hiveTableName} from partitioned staging table ${hiveStagedDBName}.${hiveStagedTableName} "
                         /usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName} -e "INSERT OVERWRITE TABLE ${hiveDBName}.${hiveTableName} PARTITION(${partitionColName}) ${sqoopSelectQueryInitial} from  ${hiveStagedDBName}.${hiveStagedTableName} ;" #>> ${logFileName} 2>&1;
                         #/usr/bin/beeline --silent=true -u 'jdbc:hive2://lxhdpmasprds001.gmail.com:2181,lxhdpmasprds002.gmail.com:2181,lxhdpmasprds003.gmail.com:2181/default;httpPath=cliservice;principal=hive/_HOST@gmail.com;serviceDiscoveryMode=zooKeeper;ssl=true;transportMode=http;zooKeeperNamespace=HttpHive' -n svcmapprdrw -e 'USE gmail_staging;SELECT COUNT(*) FROM gmail_staging.f0543_ic_staging ;'
                          if [[ $? -ne 0 ]];
				                        then
                                        errorLog "Error Inserting data to  ${hiveDBName}.${hiveTableName} table"
		        	                    exit 1;
                          fi
                         infoLog "Getting minimum of partition column in staging table"
                         minTStaging=$(/usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName} -e "select min(${partitionColName}) from ${hiveStagedDBName}.${hiveStagedTableName};")
                         minT_Parsed=$( echo $minTStaging | awk -F[\|] '{print $4}' | awk '{print $1" "$2}' )
                         infoLog "Dropping partition less than ${minT_Parsed} in Prod"
                         kinit -kt /gmail/servicekeys/assortmnt_merch/svcmapprdrw.keytab svcmapprdrw@gmail.com
                         /usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName} -e "USE ${hiveDBName};ALTER TABLE ${hiveDBName}.${hiveTableName} DROP IF EXISTS PARTITION (${partitionColName} < '${minT_Parsed}');" #>> ${logFileName} 2>&1;
                         #infoLog "Dropping partition less than ${minT_Parsed} in QA"
                         #ssh hdpbatch@lxhdpedgeqa002 "/usr/bin/beeline -u 'jdbc:hive2://lxhdpmastqa004:10001/default;transportMode=http;httpPath=cliservice?tez.queue.name=batch' -n ${applicationUserName} -e \"USE ${hiveDBName};ALTER TABLE ${hiveDBName}.${hiveTableName} DROP IF EXISTS PARTITION (${partitionColName} < '${minT_Parsed}');\"" >> ${logFileName} 2>&1;

                           if [[ $? -ne 0 ]];
				                        then
                                        errorLog "Error Inserting data to  ${hiveDBName}.${hiveTableName} table"
		        	                    exit 1;
                            fi
                         infoLog "Adding partitions to Metastore for table  ${hiveDBName}.${hiveTableName}"
                         /usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName} -e "MSCK REPAIR TABLE ${hiveDBName}.${hiveTableName};" >> ${logFileName} 2>&1;
                         hiveFinalTableRecordCount=$(/usr/bin/beeline --silent=true -u ${sourceJdbcURL} -n ${applicationUserName}  -e "select count(*) from ${hiveDBName}.${hiveTableName}") >> ${logFileName} 2>&1;
                         infoLog "Hive stage table ${hiveStagedDBName}.${hiveStagedTableName} row count : ${target_row_count}"
		                 infoLog "Hive final table ${hiveDBName}.${hiveTableName} row count : ${hiveFinalTableRecordCount}"
		                    if [[ "${target_row_count}" != "${source_row_count}" ]]
		                     then
		                        errorLog "Count mismatch between  ${hiveStagedDBName}.${hiveStagedTableName} and ${hiveDBName}.${hiveTableName} , Hence failing the job."
                                exit 1;
		                    else
		                        infoLog "Count validation is successful between ${hiveStagedDBName}.${hiveStagedTableName} and ${hiveDBName}.${hiveTableName} ."
                            fi

                   elif [[ "${tableLoadType}" == 'FULL' && "${tableType}" == 'SP' ]];
                   then
                            infoLog "Hive Concatenate the Stage table before starting the copy to Final table"
                            /usr/bin/beeline --silent=true -u ${sourceJdbcURL} -n ${applicationUserName} -e "ALTER TABLE ${hiveStagedDBName}.${hiveStagedTableName} PARTITION (${partitionColName}='${partitionColValue}') CONCATENATE ;" >> ${logFileName} 2>&1;
                            if [[ $? -ne 0 ]]; then
                                errorLog "Error in concatenating table ${hiveStagedDBName}.${hiveStagedTableName}"
                                errorLog "Exiting the script ..."
		        	        exit 1;
		        	        fi
		        	        infoLog "Started Copying the data (hadoop distcp -pb -delete -overwrite ${stageTableDataLocation}/${hiveStagedTableName}/${partitionColName}=${partitionColValue} ${tableDataLocation}/${hiveTableName}/${partitionColName}=${partitionColValue}) from partitioned staging table to partitioned final table"
                            hadoop distcp -pb -delete -overwrite ${stageTableDataLocation}/${hiveStagedTableName}/${partitionColName}=${partitionColValue} ${tableDataLocation}/${hiveTableName}/${partitionColName}=${partitionColValue} >> ${logFileName} 2>&1;
                            if [[ $? -ne 0 ]]; then
                                errorLog "Error inserting data from  table ${hiveStagedDBName}.${hiveStagedTableName}"
                                errorLog "Exiting the script ..."
		        	        exit 1;
		        	        fi
                            /usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName} -e "MSCK REPAIR TABLE ${hiveDBName}.${hiveTableName};" >> ${logFileName} 2>&1;
                            infoLog "Collecting  stats for table: ${hiveDBName}.${hiveDBName} ."
                            infoLog "Collecting  stats for table: ${hiveDBName}.${hiveDBName} "
                            /usr/bin/beeline  -u ${sourceJdbcURL} -n ${applicationUserName} -e "Analyze table ${hiveDBName}.${hiveTableName} partition (${partitionColName}) compute statistics;" >> ${logFileName} 2>&1;
		                    hadoop fs -touchz ${tableDataLocation}/${hiveTableName}/${partitionColName}=${partitionColValue}/_${successFlag}.DONE >> ${logFileName} 2>&1;

                    elif [[ "${tableLoadType}" == 'PARTIAL' && "${tableType}" == 'DP' ]];
                    then
                        infoLog "Considering this as ${tableLoadType} load for ${hiveDBName}.${hiveTableName} table"
                        infoLog "Considering this as Incremental load for ${hiveDBName}.${hiveTableName} partition table"
                        infoLog "Started inserting data  from ${hiveStagedDBName}.${hiveStagedTableName} table to ${hiveDBName}.${hiveTableName}"
                         infoLog "Finding existing partitions in the Stage table so that before same partitions can be copied to final table."
                         stage_partitions=$(/usr/bin/beeline --silent=true --showHeader=false --outputformat=tsv2 -u ${sourceJdbcURL} -n ${applicationUserName} -e "show partitions ${hiveStagedDBName}.${hiveStagedTableName} ;")
                         #/usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName} -e "INSERT OVERWRITE TABLE ${hiveDBName}.${hiveTableName} PARTITION(${partitionColName}) ${sqoopSelectQueryInitial} from  ${hiveStagedDBName}.${hiveStagedTableName} ;" >> ${logFileName} 2>&1;
                         for partition in ${stage_partitions}
                           do
                                hadoop distcp -delete -overwrite ${stageTableDataLocation}/${hiveStagedTableName}/${partition} ${tableDataLocation}/${hiveTableName}/${partition} >> ${logFileName} 2>&1;
                          done

                    if [ "$?" -ne "0" ];then
                        exitCode=$( infoLog "copyTableExitCode: $?")
			            errorLog "Error occurred while trying to distcp internal stage table ${hiveStagedTableName} data to final table ${hiveTableName}"
			            errorLog "Exiting the script.."
			            exit 1;
			        else
                        HIVE_CMD=`echo "MSCK REPAIR TABLE $hiveDBName.$hiveTableName ; ANALYZE TABLE $hiveDBName.$hiveTableName PARTITION (${partitionColName}) COMPUTE STATISTICS;"`  >> ${logFileName} 2>&1;
		            fi
                           if [[ $? -ne 0 ]];
				                        then
                                        errorLog "Error Inserting data to  ${hiveDBName}.${hiveTableName} table"
		        	                    exit 1;
                            fi
                         infoLog "Adding partitions to Metastore for table  ${hiveDBName}.${hiveTableName}"
                         /usr/bin/beeline -u ${sourceJdbcURL} -n ${applicationUserName} -e "MSCK REPAIR TABLE ${hiveDBName}.${hiveTableName};" >> ${logFileName} 2>&1;
                         hadoop fs -touchz ${tableDataLocation}/${hiveTableName}/_${successFlag}.DONE

                    fi
                        infoLog "Droping internal stage table ${hiveStagedDBName}.${hiveStagedTableName} after successful sqoop import of data."
                        /usr/bin/beeline  -u ${sourceJdbcURL} -n ${applicationUserName} -e "DROP TABLE IF EXISTS ${hiveStagedDBName}.${hiveStagedTableName}" >> ${logFileName}

		            else
		            infoLog "Exit code from sqoop import command is :$exit_code"
		            errorLog "Sqoop query failed and data is not loaded to ${hiveStagedTableName} hence cannot proceed for inserting data to ${hiveTableName} "
		            errorLog "Exiting the script ..."
		             exit 1;
		fi
	fi
else
    exitCode=$(errorLog "Cannot execute the file")
fi

if [[ ! -n ${exitCode} ]];then
	infoLog "Successfully completed sqoop job and validation along with stats collections."
	infoLog "--End Sqoop Script--"
else
	#Status true meaning validation failure
	errorLog "Job fail status=true :: Job Failed"
	errorLog "Exit Code:$exitCode"
	errorLog "Exiting the script ..."
	exit 1
fi

#--------------------------------------------------------------------------------------------------------------
# Removing the trigger location After  start of Sqoop job
#--------------------------------------------------------------------------------------------------------------

if $( hadoop fs -test -d ${triggerLocation} ); then
    infoLog "Trigger flag dir ${triggerLocation} is present and removing it after sqoop job is completed"
    hadoop fs -rm -r ${triggerLocation} >> ${logFileName}
else
    infoLog "Trigger flag dir ${triggerLocation} does not present, now ending  sqoop job.."
fi
infoLog "#########################################Sucessful###########################################"
infoLog "<--------------------------------Sqoop Ingestion Ends Here------------------------------------->"



exit 0;
