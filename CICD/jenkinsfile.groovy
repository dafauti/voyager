import hudson.model.Result
import jenkins.model.CauseOfInterruption.UserInterruption




//Only one build should be running at a time for any branch. 
abortPreviousBuilds()

node("Linux_Slave"){
	childJobLocation = ""
	tempstring = JOB_NAME.split("/")
	for(int i = 0; i < tempstring.length -2;i++){
		childJobLocation += tempstring[i] + "/"
	}
	if(childJobLocation.length() > 0)
		childJobLocation = childJobLocation.substring(0, childJobLocation.length() -1)
	else
		throw new Exception("You can't have child jobs located on the main level")
	repo_url = scm.userRemoteConfigs[0].url
	echo "Length of the url split is: " + repo_url.split("/").length.toString()
	repoSplit = repo_url.split("/")
	repoName = repoSplit[repoSplit.length - 1].split("\\.")[0]
	jobPath = getCMDResults("pwd")
	echo jobPath = jobPath
	sh "rm -rf *"
	git branch: env.Branch_Name, changelog: false, credentialsId: "6a7454db-f6b5-427e-af69-2d4a6186cda4", poll: false, url: "${repo_url}"
	sendEmail = true

    GIT_COMMIT_HASH = sh (script: "git log -n 1 --pretty=format:'%H'", returnStdout: true)
    lastCommitterEmail = sh (script: '''git log -1 |grep Author | awk -F\\< '{print $NF}' ''', returnStdout: true)
    lastCommitterEmail = lastCommitterEmail.substring(0, lastCommitterEmail.length() - 2)
    echo lastCommitterEmail
    try{
        branches = getCMDResults("git branch -a --contains ${GIT_COMMIT_HASH} | grep /development\$")
        if(branches.contains("development")){
            sendEmail = false
        }
    } catch(Exception e){}
}


build = false;
devDeploy = false;
qaDeploy = false;
uatDeploy = false;
xatDeploy = false;
prodDeploy = false;
hotfixDeploy = false;
featureDeploy = false;



switch (env.Branch_Name) {
	case "development":
		sonarBranchTarget = "master"
		build = true;
		qaDeploy=true;
		break;
	case "master":
		sonarBranchTarget="master"
		build = false;
		break;
	case ~/^feature-(.*)/:
		sonarBranchTarget="master"
		build = true;
		featureDeploy=true;
		break;
	case ~/^release-(.*)/:
		sonarBranchTarget="master"
		build = true;
		prodDeploy = true;
		break
	case ~/^hotfix-(.*)/:
		sonarBranchTarget="master"
		build = false;
		hotfixDeploy = false;
		prodDeploy = false
		break
	default:
		echo "Branch Name ${env.Branch_Name} does not follow standard"
		currentBuild.result = 'ABORTED'
		error("Stopping due to incorrect branch name ${env.Branch_Name}")
}

if(build){
	try{
		updateJobStatus(GIT_COMMIT_HASH,"INPROGRESS")
		stage ('Build') {
			echo "call build job"
			echo "childJobLocation ${childJobLocation}"
			echo "parentPath ${jobPath}"
			echo "branch ${env.Branch_Name}"


			build job: "${childJobLocation}/ChildJobs/build", propagate: true, wait: true,parameters:[
			string(name: 'parentPath', value: "${jobPath}"),
			string(name: 'branch', value: "${env.Branch_Name}")]
			
		}
		stage('Sonar Scan'){
			build job: "taps/SonarScripted", propagate: true, wait: false,parameters: 
				[string(name: 'RepoURL', value: "${repo_url}"),
				string(name: 'BranchName', value: "${env.Branch_Name}"),
				string(name: '-Dsonar.sources', value: "${jobPath}"),
				string(name: '-DsonarBranchTarget', value: "${sonarBranchTarget}"),
                string(name: '-Dsonar.exclusions', value: "CICD/**")]
				
		}
		buildResult = "SUCCESSFUL"
	}
	catch(Exception e) {
    	buildResult = "FAILED"
  	}
  	finally {
    	updateJobStatus(GIT_COMMIT_HASH, buildResult);
      	if(buildResult == "FAILED")
    		throw new Exception("The build has failed. Please check the build logs")
  	}
}
if(featureDeploy){
	stage("Feature Deploy"){
		echo "Deploying feature branch to QA"
		build job: "${childJobLocation}/ChildJobs/deploy", propagate: true, wait: true,parameters:[
				string(name: 'parentPath', value: "${jobPath}"),
				string(name: 'branch', value: "${env.Branch_Name}"),
				string(name: 'environment', value: "FEATURE")]
	}
}

if(devDeploy){
	stage("develop Deploy"){
		echo "Deploy to DEV"
		build job: "${childJobLocation}/ChildJobs/deploy", propagate: true, wait: true,parameters:[
				string(name: 'parentPath', value: "${jobPath}"),
				string(name: 'branch', value: "${env.Branch_Name}"),
				string(name: 'environment', value: "DEV")]
	}
}
if(qaDeploy){
	/*stage("QA Deploy Approval"){
	    input message: 'Proceed with QA deploy?', ok: 'Yes'
	}*/
	stage("QA Deploy"){
		echo "Deploy to QA"
      build job: "${childJobLocation}/ChildJobs/deploy", propagate: true, wait: true,parameters:[
				string(name: 'parentPath', value: "${jobPath}"),
				string(name: 'branch', value: "${env.Branch_Name}"),
				string(name: 'environment', value: "QA")]
	}
	/*stage("QA Smoke Test"){
		build job: "${childJobLocation}/ChildJobs/test", propagate: true, wait: true,parameters:[
				string(name: 'parentPath', value: "${jobPath}"),
				string(name: 'branch', value: "${env.Branch_Name}"),
				string(name: 'environment', value: "QA")]
	}
	stage("QA Signoff"){
		input message: 'Has Passed QA testing?', ok: 'Yes'
	}*/
}
if(uatDeploy){
	stage("UAT Deploy Approval"){
		input message: 'Proceed with UAT deploy?', ok: 'Yes'
	}
	stage("UAT Deploy"){
		echo "UAT Deploy"
		build job: "${childJobLocation}/ChildJobs/deploy", propagate: true, wait: true,parameters:[
				string(name: 'parentPath', value: "${jobPath}"),
				string(name: 'branch', value: "${env.Branch_Name}"),
				string(name: 'environment', value: "UAT")]
	}
	stage("UAT Smoke Test"){
		echo "UAT Smoke Test"
		build job: "${childJobLocation}/ChildJobs/test", propagate: true, wait: true,parameters:[
				string(name: 'parentPath', value: "${jobPath}"),
				string(name: 'branch', value: "${env.Branch_Name}"),
				string(name: 'environment', value: "UAT")]
	}
	stage("UAT Signoff"){
		input message: 'Has Passed UAT testing?', ok: 'Yes'
	}
}
if(xatDeploy){
	stage("XAT Deploy Approval"){
		input message: 'Proceed with XAT deploy?', ok: 'Yes'
	}
	stage("XAT Deploy"){
		echo "XAT Deploy"
		build job: "${childJobLocation}/ChildJobs/deploy", propagate: true, wait: true,parameters:[
				string(name: 'parentPath', value: "${jobPath}"),
				string(name: 'branch', value: "${env.Branch_Name}"),
				string(name: 'environment', value: "XAT")]
	}
	stage("XAT Smoke Test"){
		echo "XAT Smoke Test"
      build job: "${childJobLocation}/ChildJobs/test", propagate: true, wait: true,parameters:[
				string(name: 'parentPath', value: "${jobPath}"),
				string(name: 'branch', value: "${env.Branch_Name}"),
				string(name: 'environment', value: "XAT")]
	}
	stage("XAT Signoff"){
		input message: 'Has Passed XAT testing?', ok: 'Yes'
	}
}
if(hotfixDeploy){
	stage("Hotfix Deploy Approval"){
		input message: 'Proceed with hotfixDeploy deploy?', ok: 'Yes'
	}
	stage("HOTFIX Deploy"){
		echo "HOTFIX Deploy"
		build job: "${childJobLocation}/ChildJobs/deploy", propagate: true, wait: true,parameters:[
				string(name: 'parentPath', value: "${jobPath}"),
				string(name: 'branch', value: "${env.Branch_Name}"),
				string(name: 'environment', value: "HOTFIX")]
	}
	stage("HOTFIX Smoke Test"){
		echo "HOTFIX Smoke Test"
		build job: "${childJobLocation}/ChildJobs/test", propagate: true, wait: true,parameters:[
				string(name: 'parentPath', value: "${jobPath}"),
				string(name: 'branch', value: "${env.Branch_Name}"),
				string(name: 'environment', value: "HOTFIX")]
	}
	stage("HOTFIX Signoff"){
		input message: 'Has Passed HOTFIX testing?', ok: 'Yes'
	}
}
if(prodDeploy){
	stage("Request Prod Deploy Job"){
		echo "start Prod job"
		//build job: "${childJobLocation}/Prod/prodDeploy", propagate: true, wait: true,parameters:[
		build job: "${childJobLocation}/ChildJobs/deploy", propagate: true, wait: true,parameters:[
				string(name: 'parentPath', value: "${jobPath}"),
				string(name: 'branch', value: "${env.Branch_Name}"),
				string(name: 'environment', value: "PROD")]
	}
}

// This function stops any currently running builds for the current branch.
def abortPreviousBuilds() {
    Run previousBuild = currentBuild.rawBuild.getPreviousBuildInProgress()

    while (previousBuild != null) {
        if (previousBuild.isInProgress()) {
            def executor = previousBuild.getExecutor()
            if (executor != null) {
                echo ">> Aborting older build #${previousBuild.number}"
                executor.interrupt(Result.ABORTED, new UserInterruption(
                    "Aborted by newer build #${currentBuild.number}"
                ))
            }
        }

        previousBuild = previousBuild.getPreviousBuildInProgress()
    }
}

//Updates the build status in bitbucket
def updateJobStatus(String GIT_COMMIT_HASH, String buildResult) {
  build job: "taps/BitbucketBuildStatusUpdate", propagate: false, wait: false,parameters: [
    string(name: 'commit_hash', value: "${GIT_COMMIT_HASH}"),
    string(name: 'build_result', value: "${buildResult}"),
    string(name: 'key', value: "${env.Branch_Name}"),
    string(name: 'name', value: "Jenkins"),
    string(name: 'description', value: "build"),
    string(name: 'url', value: "${env.BUILD_URL}")
  ]
}
//runs a given command line command and returns the standard output 
def getCMDResults(String cmd){
 	result = sh (script:cmd,returnStdout: true).trim()
 	return result
 }
