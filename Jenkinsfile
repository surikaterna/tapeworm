// Jenkinsfile — Declarative CI/CD pipeline for tapeworm monorepo
//
// Operator setup required in Jenkins:
//   1. Configure a "secret text" credential with ID 'npm-token'
//      containing your npm registry auth token.
//   2. Configure a "username/password" credential with ID 'docker-creds'
//      containing your Docker registry credentials.
//   3. Configure a "secret text" credential with ID 'docker-registry'
//      containing your Docker registry hostname (e.g. ghcr.io/yourorg).

pipeline {
    agent {
        docker { image 'node:26-alpine' }
    }

    environment {
        CI = 'true'
    }

    stages {
        stage('Install') {
            steps {
                sh 'npm ci'
            }
        }

        stage('Build') {
            steps {
                sh 'npm run build'
            }
        }

        stage('Test') {
            steps {
                sh 'npm run test'
            }
        }

        stage('Docker Build') {
            steps {
                sh '''
                    docker build \
                        -f packages/tapeworm_dispatcher_mdb_rmq/Dockerfile \
                        -t tapeworm-dispatcher:${BUILD_NUMBER} \
                        -t tapeworm-dispatcher:latest \
                        .
                '''
            }
        }

        stage('Publish') {
            when { branch 'main' }
            environment {
                NPM_TOKEN    = credentials('npm-token')
                DOCKER_CREDS = credentials('docker-creds')
                DOCKER_REGISTRY = credentials('docker-registry')
            }
            steps {
                sh '''
                    echo "//registry.npmjs.org/:_authToken=${NPM_TOKEN}" > .npmrc
                    npm run changeset:version
                    npm run changeset:publish
                '''

                sh '''
                    echo "${DOCKER_CREDS_PSW}" | docker login -u "${DOCKER_CREDS_USR}" --password-stdin "${DOCKER_REGISTRY}"
                    docker tag tapeworm-dispatcher:${BUILD_NUMBER} ${DOCKER_REGISTRY}/tapeworm-dispatcher:${BUILD_NUMBER}
                    docker tag tapeworm-dispatcher:latest ${DOCKER_REGISTRY}/tapeworm-dispatcher:latest
                    docker push ${DOCKER_REGISTRY}/tapeworm-dispatcher:${BUILD_NUMBER}
                    docker push ${DOCKER_REGISTRY}/tapeworm-dispatcher:latest
                '''
            }
        }
    }

    post {
        always {
            cleanWs()
        }
    }
}
