#!/bin/bash

#SCRIPT_PATH=/platformscripts/default_queue_check

SCRIPT_PATH=`dirname "$0"`;
SCRIPT_PATH=`eval "cd \"$SCRIPT_PATH\" && pwd"`


yarn application -list -appStates NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING,FINISHED |awk '$5 == "default" {print $1","$4","$5}' > $SCRIPT_PATH/output.csv

/usr/bin/sed "s/$/,$(date +"%Y-%m-%d")/" $SCRIPT_PATH/output.csv > $SCRIPT_PATH/finalout.csv

echo "User_Name,Job_count" >> $SCRIPT_PATH/job_count.csv
yarn application -list -appStates NEW, NEW_SAVING, SUBMITTED, ACCEPTED, RUNNING, FINISHED | awk '$5 == "default" {print $4}' | sort | uniq -c | awk '{ print $2 "," $1}' >> $SCRIPT_PATH/job_count.csv

psql -h lxhdpnifprdw001 -p 5432 -U grafana monitoring << EOF
\COPY default_queue_check(APPLICATION_ID,USER_NAME,QUEUE,DATE) FROM '$SCRIPT_PATH/finalout.csv' DELIMITER ',' CSV HEADER;
EOF

/usr/bin/python3.6 $SCRIPT_PATH/convert_to_html.py

( echo "TO: dafauti13sonu@gmail.com";  echo "MIME-Version: 1.0"  ; echo "Subject: Jobs Running in Default Queue"; echo "Content-Type: text/html" ; cat $SCRIPT_PATH/myhtml.html;) | sendmail -t

sleep 10
rm $SCRIPT_PATH/job_count.csv
rm $SCRIPT_PATH/output.csv
rm $SCRIPT_PATH/finalout.csv
rm $SCRIPT_PATH/myhtml.html
