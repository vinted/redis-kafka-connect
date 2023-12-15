pipeline {
    agent {
        dockerfile(agent())
    }
    environment {
        JRELEASER_GITHUB_TOKEN = credentials('github-jenkins-token')
        CLOUDSMITH_TOKEN = credentials('cloudsmith-platform-api-token')
        CLOUDSMITH_REPO = 'rubygems-hosted-backend'
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
        stage('Release') {
            when {
                branch 'master'
            }
            steps {
                sh './gradlew jreleaserRelease --stacktrace'
            }
        }
    }
}

static def agent() {
    [
        label: 'shared',
        filename: 'Jenkins.Dockerfile',
        args: '--add-host=host.docker.internal:host-gateway -v /var/run/docker.sock:/var/run/docker.sock:z -v $WORKSPACE:/app -v $JENKINS_HOME/workspace/git_repo_reference:$JENKINS_HOME/workspace/git_repo_reference',
        reuseNode: true,
    ]
}
