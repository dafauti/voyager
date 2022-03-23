#!/bin/sh -e
#--------------------------------------------------------------------------------------------------------------
# Logger : Variable initialization
#--------------------------------------------------------------------------------------------------------------
export KRB5CCNAME=${HOME}/krb5cc_svcmapprdrw
kinit -kt /gmail/servicekeys/assortmnt_merch/svcmapprdrw.keytab svcmapprdrw@gmail.COM
local_log_dir=$1
table_name=$2
instanceDate=$3
script_name=`basename "$0"`
script_name="${script_name%.*}"
log_file_name=${table_name}_${instanceDate}.log
local_log_file=${local_log_dir}/${log_file_name}

#--------------------------------------------------------------------------------------------------------------
# Logger : methods for various levels of log
#--------------------------------------------------------------------------------------------------------------


timestamp ()
{
    date "+%y/%m/%d %H:%M:%S"
}

infoLog(){

    local msg="$1"
    echo "`timestamp` INFO ${script_name}: $msg" >> $local_log_file
}

debugLog(){

    local msg="$1"
    echo "`timestamp` DEBUG ${script_name}: $msg" >> $local_log_file
}

errorLog(){

    local msg="$1"
    echo "`timestamp` ERROR ${script_name}: $msg" >> $local_log_file
}

warningLog(){

    local msg="$1"
    echo "`timestamp` WARNING ${script_name}: $msg" >> $local_log_file
}

logFileName(){

    local logName=$local_log_file
    echo "$logName"
}
#--------------------------------------------------------------------------------------------------------------
# Logger : Creating local log directory if it doesn't exists
#--------------------------------------------------------------------------------------------------------------

if [ ! -d ${local_log_dir} ]; then
    mkdir -p "${local_log_dir}";
    infoLog "Local Log Dir ${local_log_dir} created"
    touch $local_log_file
    infoLog "Local Log File ${local_log_file} created"
fi

if [ ! -f $local_log_file ]; then
    touch $local_log_file
    infoLog "Local Log File ${local_log_file} created"
fi

#--------------------------------------------------------------------------------------------------------------
# Logger : Ends here.
#--------------------------------------------------------------------------------------------------------------
