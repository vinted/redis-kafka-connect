pipeline {
    agent {
        dockerfile('container-registry.svc.vinted.com/proxy-docker-hub/openjdk:18-jdk-slim')
    }
    options {
        ansiColor('xterm')
        disableConcurrentBuilds(abortPrevious: true)
        timeout(time: 10, unit: 'MINUTES')
        timestamps()
    }
    triggers {
        pollSCM('')
        issueCommentTrigger('.*test this please.*')
    }
    stages {
        stage('Test') {
            steps {
                sh './gradlew test'
            }
        }
        stage('Build') {
            steps {
                sh './gradlew createConfluentArchive'
            }
        }
    }
}
