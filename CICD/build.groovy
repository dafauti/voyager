node("Linux_Slave") {
    stage("build")
            {
                dir(parentPath) {
                    withMaven(maven: 'Maven_3.3.1', jdk: 'Java 1.8') {
                        sh "mvn clean package"
                    }
                }
                echo "@@Build completed"
            }
}
