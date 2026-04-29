// Jenkinsfile — Declarative CI/CD pipeline for tapeworm monorepo
//
// Operator setup required in Jenkins:
//   1. Configure a "secret text" credential with ID 'npm-token'
//      containing your npm registry auth token.
//   2. Configure a "username/password" credential with ID 'docker-registry-login'
//      containing your Docker registry credentials.
//   3. Configure a "secret text" credential with ID 'docker-registry'
//      containing your Docker registry hostname (e.g. ghcr.io/yourorg).

pipeline {
    agent none

    environment {
        CI = 'true'
    }

    stages {
        stage('Install') {
            agent {
                docker {
                    image 'node:22'
                    label 'lynx'
                }
            }
            steps {
                sh 'npm ci'
            }
        }

        stage('Build') {
            agent {
                docker {
                    image 'node:22'
                    label 'lynx'
                }
            }
            steps {
                sh 'npm run build'
            }
        }

        stage('Test') {
            agent {
                docker {
                    image 'node:22'
                    label 'lynx'
                }
            }
            steps {
                sh 'npm run test'
            }
        }

        stage('Docker Build') {
            agent {
                label 'lynx'
            }

            environment {
                RELEASE_BRANCH = 'release'
                DEVELOP_BRANCH = 'develop'
            }

            steps {
                script {
                    def releaseTag
                    if (env.BRANCH_NAME.startsWith(env.RELEASE_BRANCH)) {
                        releaseTag = escapedTagName('RC-')
                    } else {
                        releaseTag = "${env.DEVELOP_BRANCH}${env.BUILD_NUMBER}"
                    }

                    docker.withRegistry('', 'docker-registry-login') {
                        sh """
                            docker build \
                                -f packages/tapeworm_dispatcher_mdb_rmq/Dockerfile \
                                -t tapeworm-dispatcher:${releaseTag} \
                                -t tapeworm-dispatcher:latest \
                                .
                        """
                    }
                }
            }
        }

        stage('Publish Docker') {
            agent {
                label 'lynx'
            }
            environment {
                DOCKER_REGISTRY = credentials('docker-registry')
                RELEASE_BRANCH = 'release'
                DEVELOP_BRANCH = 'develop'
            }
            steps {
                script {
                    def releaseTag
                    if (env.BRANCH_NAME.startsWith(env.RELEASE_BRANCH)) {
                        releaseTag = escapedTagName('RC-')
                    } else {
                        releaseTag = "${env.DEVELOP_BRANCH}${env.BUILD_NUMBER}"
                    }

                    withEnv(["RELEASE_TAG=${releaseTag}"]) {
                        docker.withRegistry('', 'docker-registry-login') {
                            sh '''
                                docker tag tapeworm-dispatcher:$RELEASE_TAG $DOCKER_REGISTRY/tapeworm-dispatcher:$RELEASE_TAG
                                docker tag tapeworm-dispatcher:latest $DOCKER_REGISTRY/tapeworm-dispatcher:latest
                                docker push $DOCKER_REGISTRY/tapeworm-dispatcher:$RELEASE_TAG
                                docker push $DOCKER_REGISTRY/tapeworm-dispatcher:latest
                            '''
                        }
                    }
                }
            }
        }

        stage('Publish Packages') {
            agent {
                label 'lynx'
            }
            environment {
                NPM_TOKEN    = credentials('npm-token')
            }
            steps {
                sh '''
                    echo "//registry.npmjs.org/:_authToken=${NPM_TOKEN}" > .npmrc
                    npm run changeset:version
                    npm run changeset:publish -- --no-git-tag
                '''
            }
        }
    }

    post {
        always {
            node('lynx') {
                cleanWs()
            }
        }
    }
}


String escapedTagName(String prefix) {
    if (prefix == null) {
        prefix = ''
    }
    def tagName = env.BRANCH_NAME.replace("/", '-') + prefix + env.BUILD_NUMBER;
    return tagName
}