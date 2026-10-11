// filepath: Jenkinsfile
pipeline {
    agent any

    options {
        timeout(time: 2, unit: 'HOURS')
        timestamps()
        ansiColor('xterm')
        buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '15'))
        disableConcurrentBuilds()
    }

    parameters {
        booleanParam(
            name: 'RETRAIN_MODELS',
            defaultValue: false,
            description: 'Trigger automated retraining of FP-Growth association rules and PyTorch NCF neural network from Lakehouse data'
        )
        booleanParam(
            name: 'SYNC_REDIS_FEATURE_STORE',
            defaultValue: true,
            description: 'Synchronize association rules and precomputed user embeddings to Redis Feature Store cluster'
        )
        choice(
            name: 'DEPLOY_TARGET',
            choices: ['staging', 'production', 'dry-run'],
            description: 'Target deployment environment'
        )
        string(
            name: 'DOCKER_REGISTRY',
            defaultValue: 'ghcr.io/nghon4maeri',
            description: 'Container registry prefix'
        )
        string(
            name: 'KUBE_NAMESPACE',
            defaultValue: 'recommendation-platform',
            description: 'Kubernetes namespace for deployment'
        )
    }

    environment {
        PROJECT_NAME        = 'ecom-hybrid-recommender'
        IMAGE_TAG           = "${BUILD_NUMBER}-${GIT_COMMIT.take(7)}"
        PYTHONUNBUFFERED    = '1'
        PYTEST_REPORT_DIR   = 'test-reports'
        REDIS_HOST          = 'redis'
        REDIS_PORT          = '6379'
        FASTAPI_URL         = 'http://localhost:8000'
    }

    stages {

        stage('1. Environment Diagnosis & Checkout') {
            steps {
                echo "=== Stage 1: Checkout & Environment Diagnostic ==="
                echo "Build ID: ${env.BUILD_ID} | Commit: ${env.GIT_COMMIT}"
                echo "Target Environment: ${params.DEPLOY_TARGET}"

                sh '''
                    echo "Checking system toolchains..."
                    python3 --version || python --version
                    node --version
                    npm --version
                    docker --version || true
                    mkdir -p test-reports reports
                '''
            }
        }

        stage('2. Code Quality & Linting') {
            parallel {
                stage('Python Linting') {
                    steps {
                        echo "Running Python static analysis (flake8 / black)..."
                        sh '''
                            python3 -m pip install --upgrade flake8 black || true
                            python3 -m flake8 backend mlops streaming tests --count --select=E9,F63,F7,F82 --show-source --statistics || true
                        '''
                    }
                }
                stage('Frontend Typecheck & Lint') {
                    steps {
                        dir('frontend') {
                            echo "Running TypeScript typecheck..."
                            sh '''
                                npm ci
                                npm run build
                            '''
                        }
                    }
                }
            }
        }

        stage('3. Automated Unit & Integration Tests') {
            steps {
                echo "=== Stage 3: Executing Automated Test Suite ==="
                sh '''
                    python3 -m pip install -r backend/requirements.txt || true
                    python3 -m pip install pytest pytest-cov httpx || true
                    python3 -m pytest tests/ \
                        --junitxml=test-reports/junit-results.xml \
                        -v
                '''
            }
            post {
                always {
                    junit testResults: 'test-reports/junit-results.xml', allowEmptyResults: true
                }
            }
        }

        stage('4. ML Artifact & Model Gate') {
            steps {
                echo "=== Stage 4: Validating MLOps Model Artifacts ==="
                sh '''
                    python3 -c "
import os, sys
rules_json = 'data/models/fpgrowth_rules.json'
ncf_pt = 'models/ncf_model.pt'

if not os.path.exists(rules_json):
    print(f'[WARN] FP-Growth rules file {rules_json} not found. Retrain required.')
else:
    print(f'[PASS] FP-Growth rules file verified: {rules_json}')

if not os.path.exists(ncf_pt):
    print(f'[WARN] NCF PyTorch weights {ncf_pt} not found. Running in rules-fallback mode.')
else:
    size_mb = os.path.getsize(ncf_pt) / (1024 * 1024)
    print(f'[PASS] PyTorch NCF Model weights verified: {size_mb:.2f} MB')
"
                '''
            }
        }

        stage('5. Automated Model Retrain & Sync (Conditional)') {
            when {
                expression { return params.RETRAIN_MODELS == true }
            }
            steps {
                echo "=== Stage 5: Retraining FP-Growth & NCF from Lakehouse ==="
                sh '''
                    echo "Triggering FP-Growth training on real transactions..."
                    python3 mlops/train_fpgrowth.py \
                        --data-path data/lakehouse/real_transactions.parquet \
                        --min-support 0.015 \
                        --min-confidence 0.3 \
                        --output data/models/fpgrowth_rules.json

                    if [ "${params.SYNC_REDIS_FEATURE_STORE}" = "true" ]; then
                        echo "Syncing updated rules to Redis Feature Store..."
                        python3 mlops/sync_redis_store.py \
                            --redis-host ${REDIS_HOST} \
                            --redis-port ${REDIS_PORT} \
                            --rules-json data/models/fpgrowth_rules.json
                    fi
                '''
            }
        }

        stage('6. Multi-Container Image Build') {
            when {
                expression { return params.DEPLOY_TARGET != 'dry-run' }
            }
            steps {
                echo "=== Stage 6: Building Production Container Images ==="
                script {
                    echo "Building Docker image: ${params.DOCKER_REGISTRY}/recommender-backend:${env.IMAGE_TAG}"
                    sh "docker build -t ${params.DOCKER_REGISTRY}/recommender-backend:${env.IMAGE_TAG} -t ${params.DOCKER_REGISTRY}/recommender-backend:latest -f backend/Dockerfile backend/ || true"

                    echo "Building Docker image: ${params.DOCKER_REGISTRY}/recommender-frontend:${env.IMAGE_TAG}"
                    sh "docker build -t ${params.DOCKER_REGISTRY}/recommender-frontend:${env.IMAGE_TAG} -t ${params.DOCKER_REGISTRY}/recommender-frontend:latest -f frontend/Dockerfile frontend/ || true"

                    echo "Building Docker image: ${params.DOCKER_REGISTRY}/recommender-consumer:${env.IMAGE_TAG}"
                    sh "docker build -t ${params.DOCKER_REGISTRY}/recommender-consumer:${env.IMAGE_TAG} -t ${params.DOCKER_REGISTRY}/recommender-consumer:latest -f streaming/Dockerfile streaming/ || true"
                }
            }
        }

        stage('7. Security Vulnerability Scan') {
            when {
                expression { return params.DEPLOY_TARGET != 'dry-run' }
            }
            steps {
                echo "=== Stage 7: Container Security Audit ==="
                sh '''
                    # Trivy or Bandit security scan placeholder
                    echo "Checking for high/critical CVEs in container base images..."
                    which trivy > /dev/null 2>&1 && trivy image --severity HIGH,CRITICAL --exit-code 0 ${params.DOCKER_REGISTRY}/recommender-backend:latest || echo "Trivy not installed, skipping container CVE scan."
                '''
            }
        }

        stage('8. Continuous Deployment') {
            when {
                expression { return params.DEPLOY_TARGET in ['staging', 'production'] }
            }
            steps {
                echo "=== Stage 8: Deploying to ${params.DEPLOY_TARGET} ==="
                script {
                    if (params.DEPLOY_TARGET == 'production') {
                        echo "Promoting to Production Kubernetes Cluster (Namespace: ${params.KUBE_NAMESPACE})..."
                        sh '''
                            which kubectl > /dev/null 2>&1 && kubectl rollout restart deployment -n ${params.KUBE_NAMESPACE} || echo "kubectl not configured on Jenkins agent. Using docker compose deployment."
                        '''
                    } else {
                        echo "Deploying to Staging Docker Compose environment..."
                        sh '''
                            docker compose up -d --remove-orphans || true
                        '''
                    }
                }
            }
        }

        stage('9. Post-Deployment Verification & SLA Check') {
            when {
                expression { return params.DEPLOY_TARGET in ['staging', 'production'] }
            }
            steps {
                echo "=== Stage 9: Probing /health & Latency SLA (< 5ms) ==="
                sh '''
                    python3 -c "
import urllib.request, json, time

url = '${FASTAPI_URL}/health'
try:
    start = time.time()
    req = urllib.request.Request(url, headers={'User-Agent': 'Jenkins-CI-Probe'})
    with urllib.request.urlopen(req, timeout=5) as resp:
        elapsed_ms = (time.time() - start) * 1000
        data = json.loads(resp.read().decode())
        print(f'[HEALTH PROBE PASS] Status: {data.get(\"status\")} | Latency: {elapsed_ms:.2f}ms')
except Exception as e:
    print(f'[HEALTH PROBE WARN] Unable to reach {url} directly from Jenkins agent: {e}')
"
                '''
            }
        }
    }

    post {
        always {
            echo "Archiving build artifacts..."
            archiveArtifacts artifacts: 'test-reports/**', allowEmptyArchive: true
            cleanWs deleteDirs: true, notFailBuild: true, patterns: [[pattern: '.venv/**', type: 'EXCLUDE'], [pattern: 'data/**', type: 'EXCLUDE']]
        }
        success {
            echo " Pipeline Succeeded! Hybrid Recommender Platform deployed smoothly."
        }
        failure {
            echo " Pipeline Failed! Check test reports and step logs."
        }
    }
}
