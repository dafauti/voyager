#!/usr/bin/env bash

######################################################################################################################
#            						Cleaning Up old files
#   Date:  Jan 25, 2020
#   Description: This script removes the old files which are more than parameterized number of days from edgenode.
#                This script calculates the difference between current time and creation time of existing files and -
#                based on that if difference is more than parameterized number of days, it deletes files.
######################################################################################################################
set -x
export KRB5CCNAME=${HOME}/krb5cc_svcmapprdrw
kinit -kt /gmail/servicekeys/assortmnt_merch/svcmapprdrw.keytab svcmapprdrw@gmail.COM
#--------------------------------------------------------------------------------------------------------------
# Arguments :
#--------------------------------------------------------------------------------------------------------------

Local_Log_Path=$1
cleanup_before_days=$2
loggingScript=$3
logName=$4
instanceDate=$5
clenaUpLogName=${logName}_cleanUp

#--------------------------------------------------------------------------------------------------------------
# Sourcing Logging utility script having functions : infoLog, errorLog, debugLog, warningLog.
#--------------------------------------------------------------------------------------------------------------

source ${loggingScript} ${Local_Log_Path} ${clenaUpLogName} ${instanceDate}
#logFileName=$(logFileName)

#--------------------------------------------------------------------------------------------------------------
# Copying Local Log file to Hdfs location
#--------------------------------------------------------------------------------------------------------------

#hadoop fs -put ${logFileName} ${Hdfs_Log_Path}

#--------------------------------------------------------------------------------------------------------------
# Removing Log Files which are older than given log retention period.
#--------------------------------------------------------------------------------------------------------------

infoLog "Removing older local log files"
infoLog "Number of days before log need to be purged : ${cleanup_before_days}"
infoLog "Local Log path : ${Local_Log_Path}"

for dir in ${Local_Log_Path}
do
  find ${dir} -name "*${logName}*.log" -type f -mtime +${cleanup_before_days} -exec rm -f {} \;
  if [[ $? -ne 0 ]]
    then
      errorLog "File purging process failed."
    else
      infoLog "File purging process successful."
  fi
done
infoLog "--End Cleanup Script--"
exit 0;
