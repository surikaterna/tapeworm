// Linux agent with host Docker CLI/daemon access; no socket inside the Node runner.
// See ci/README.md for isolation, recovery and main-only release prerequisites.
pipeline {
    agent { label "${params.DOCKER_AGENT_LABEL ?: 'docker'}" }
    parameters {
        string(name: 'DOCKER_AGENT_LABEL', defaultValue: 'docker', description: 'Trusted Linux Docker-capable agent')
    }
    options { timeout(time: 30, unit: 'MINUTES') }
    environment { CI = 'true' }
    stages {
        stage('Run identity') {
            steps {
                script {
                    env.RUN_ID = sh(script: 'read id < /proc/sys/kernel/random/uuid; printf "%s" "$id"', returnStdout: true).trim()
                }
            }
        }
        stage('Qualify source and exact image') {
            steps { sh './ci/qualify.sh' }
        }
        stage('Release preflight without credentials') {
            when { branch 'main' }
            steps { sh './ci/qualify.sh release-preflight' }
        }
        stage('Publish qualified artifacts') {
            when { branch 'main' }
            steps {
                withCredentials([
                    string(credentialsId: 'npm-token', variable: 'NPM_TOKEN'),
                    usernamePassword(credentialsId: 'docker-creds', usernameVariable: 'DOCKER_CREDS_USR', passwordVariable: 'DOCKER_CREDS_PSW'),
                    string(credentialsId: 'docker-registry', variable: 'DOCKER_REGISTRY')
                ]) {
                    sh './ci/qualify.sh publish'
                }
            }
        }
    }
    post {
        always {
            script {
                try {
                    if (env.RUN_ID) { sh './ci/qualify.sh cleanup' }
                } finally {
                    try {
                        archiveArtifacts artifacts: '.ci-artifacts/*/*.log,.ci-artifacts/*/*.json,.ci-artifacts/*/image-id,.ci-artifacts/*/revision', allowEmptyArchive: true
                    } finally { deleteDir() }
                }
            }
        }
    }
}
