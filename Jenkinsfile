pipeline {
    agent {
        dockerfile(agent())
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

static def agent() {
    [
        label: 'shared',
        filename: 'Dockerfile',
        args: '--add-host=host.docker.internal:host-gateway -v /var/run/docker.sock:/var/run/docker.sock:z -v $WORKSPACE:/app -v $JENKINS_HOME/workspace/git_repo_reference:$JENKINS_HOME/workspace/git_repo_reference --target base',
        reuseNode: true,
    ]
}
