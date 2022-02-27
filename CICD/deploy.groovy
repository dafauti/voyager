import java.text.SimpleDateFormat
node("Linux_Slave"){
    stage("kinit") {
        if(environment == "FEATURE")
            withCredentials([sshUserPrivateKey(credentialsId: "svcmapprdrwsa", keyFileVariable: 'keyfile', passphraseVariable: '', usernameVariable: 'user')]) {
                echo "call build job ${keyfile} ${user}"
                //            sh "ssh -o StrictHostKeyChecking=no ${user}@lxhdpnifprds001.gmail.com whoami"
                sh "ssh -i ${keyfile} ${user}@lxhdpnifprds001.gmail.com kinit -kt /lowes/servicekeys/assortmnt_merch/svcmapprdrw.keytab svcmapprdrw@gmail.com"
                sh "ssh -i ${keyfile} ${user}@lxhdpnifprds001.gmail.com klist"
            }
    }
    stage("deploy")
            {
                if(environment=="DEV")
                    deployCode("hdpbatchDTQ", "/lowes/apps/merch/assort/dpp-canada", "lxhdpmastdev002", "dev","/airflow/dags/merch/dpp-canada/","/lowes/logs/merch/assort/dpp_canada/$branch")
                else if(environment == "QA")
                    deployCode("hdpbatchQA", "/lowes/apps/merch/assort/dpp-canada", "lxhdpedgeqa002", "qa","/airflow/dags/merch/dpp-canada/","/lowes/logs/merch/assort/dpp_canada/$branch")
                else if(environment == "PROD")
                    deployCode("svcmapprdrwws", "/lowes_daci/apps/merch/assort/dpp-canada/$branch", "lxhdpnifprdw001", "prod","/airflow/dags/merch/dpp-canada/","/lowes_daci/logs/merch/assort/dpp_canada/$branch")
                else if(environment == "FEATURE")
                    deployCode("svcmapprdrwsa", "/lowes/apps/merch/assort/dpp-canada/$branch", "lxhdpnifprds001", "feature","/airflow/dags/merch/dpp-canada","/lowes/logs/merch/assort/dpp_canada/$branch")

            }
}

def deployCode(String credID, String deploymentLocation, String server, String mode, String dagLocation, String logLocation){

    withCredentials([sshUserPrivateKey(credentialsId: credID, keyFileVariable: 'keyfile', passphraseVariable: '', usernameVariable: 'user')]) {
        version = getPomDetails(getCMDResults("cat ${parentPath}/pom.xml"),"version")
        artifactid = getPomDetails(getCMDResults("cat ${parentPath}/pom.xml"),"artifactid")

        sh "ssh -i ${keyfile} ${user}@${server}.gmail.com mkdir -p ${deploymentLocation}/"
        sh "ssh -i ${keyfile} ${user}@${server}.gmail.com rm -rf ${deploymentLocation}/*"
        sh "rsync -rpu --delete-delay -e 'ssh -i ${keyfile}' ${parentPath}/target/${artifactid}-${version}-deploy.tar.gz  ${user}@${server}.gmail.com:${deploymentLocation}/"
        sh "ssh -i ${keyfile} ${user}@${server}.gmail.com tar -xvf ${deploymentLocation}/${artifactid}-${version}-deploy.tar.gz -C ${deploymentLocation}/"
        sh "ssh -i ${keyfile} ${user}@${server}.gmail.com rm -rf ${deploymentLocation}/${artifactid}-${version}-deploy.tar.gz"
        sh "ssh -i ${keyfile} ${user}@${server}.gmail.com mkdir -p ${dagLocation}"
        sh "ssh -i ${keyfile} ${user}@${server}.gmail.com mkdir -p ${logLocation}"
        sh "ssh -i ${keyfile} ${user}@${server}.gmail.com mkdir -p ${logLocation}/app_log"
        sh "ssh -i ${keyfile} ${user}@${server}.gmail.com chmod 777 ${logLocation}"
        sh "ssh -i ${keyfile} ${user}@${server}.gmail.com chmod 777 ${logLocation}/app_log"
        sh "ssh -i ${keyfile} ${user}@${server}.gmail.com rm -rf ${dagLocation}/*"
        //sh "ssh -i ${keyfile} ${user}@${server}.gmail.com rm -rf ${dagLocation}/data_quality_framework_agg.py"
        //sh "ssh -i ${keyfile} ${user}@${server}.gmail.com cp ${deploymentLocation}/dpp-canada-app/airflow_dags/data_quality/data_quality_framework_agg.py ${dagLocation}"

        //sh "ssh -i ${keyfile} ${user}@${server}.gmail.com rm -rf ${dagLocation}/data_quality_framework_combined.py"
        //sh "ssh -i ${keyfile} ${user}@${server}.gmail.com cp ${deploymentLocation}/dpp-canada-app/airflow_dags/data_quality/data_quality_framework_combined.py ${dagLocation}"


        //sh "ssh -i ${keyfile} ${user}@${server}.gmail.com rm -rf ${dagLocation}/calculate_combined_dpp_dag.py"
        //sh "ssh -i ${keyfile} ${user}@${server}.gmail.com cp ${deploymentLocation}/dpp-canada-app/airflow_dags/dpp/calculate_combined_dpp_dag.py ${dagLocation}"


        sh "ssh -i ${keyfile} ${user}@${server}.gmail.com cp ${deploymentLocation}/dpp-canada-app/airflow_dags/dpp/* ${dagLocation}"
        sh "ssh -i ${keyfile} ${user}@${server}.gmail.com cp ${deploymentLocation}/dpp-canada-app/airflow_dags/sqoop_ingestion/teradata/* ${dagLocation}"
        sh "ssh -i ${keyfile} ${user}@${server}.gmail.com cp ${deploymentLocation}/dpp-canada-app/airflow_dags/sqoop_ingestion/db2/* ${dagLocation}"
        sh "ssh -i ${keyfile} ${user}@${server}.gmail.com cp ${deploymentLocation}/dpp-canada-app/airflow_dags/data_quality/* ${dagLocation}"

    }

}



def getCMDResults(String cmd) {
    result = sh(script: cmd, returnStdout: true).trim()
    return result
}

@NonCPS
def getPomDetails(String jsonText,String detail) {
    final slurper = new XmlSlurper().parseText(jsonText)

    if (detail.equalsIgnoreCase("version"))
        return slurper.version.toString()
    else if(detail.equalsIgnoreCase("groupId"))
        return slurper.groupId.toString()
    else if(detail.equalsIgnoreCase("artifactId"))
        return slurper.artifactId.toString()
}


def getCurrentDate(){
    def date = new Date()
    def sdf = new SimpleDateFormat("yyyy-MM-dd")
    return sdf.format(date)
}
