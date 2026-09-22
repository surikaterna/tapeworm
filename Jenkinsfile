// Linux agent with host Docker CLI/daemon access; no socket inside the Node runner.
// See ci/README.md for isolation, recovery and master-only publication prerequisites.
pipeline {
    agent { label "${params.DOCKER_AGENT_LABEL ?: 'lynx'}" }
    parameters {
        string(name: 'DOCKER_AGENT_LABEL', defaultValue: 'lynx', description: 'Trusted Linux Docker-capable agent label (default: lynx)')
    }
    options { timeout(time: 30, unit: 'MINUTES') }
    environment {
        CI = 'true'
        DOCKER_BIN = '/usr/bin/docker'
    }
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
            when {
                allOf {
                    branch 'master'
                    not { buildingTag() }
                }
            }
            steps { sh './ci/qualify.sh release-preflight' }
        }
        stage('Publish qualified artifacts') {
            when {
                allOf {
                    branch 'master'
                    not { buildingTag() }
                }
            }
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
