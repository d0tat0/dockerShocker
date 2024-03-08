# Orchestrator

### Setup Repo
- git clone mchs repo
- git config --global url."https://<github_acess_token>@github.com".insteadOf "https://github.com"

### For deployments, configure bundle alias in deploy_config.json
- git pull
- git checkout spark_dev
- python3 deploy_orchestrator.py --config-alias V5_200

## Login to airflow, to verify and run dags.
