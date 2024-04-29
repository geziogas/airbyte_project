from airflow.decorators import dag, task
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig
from include.dbt.fraud.cosmos_config import DBT_CONFIG, DBT_PROJECT_CONFIG
from airflow.models.baseoperator import chain

from datetime import datetime

AIRBYTE_ID_LOAD_CUSTOMER_TRANSACTIONS_RAW = '1f214a28-61f6-4e87-afdc-ccc2560e7d53'
AIRBYTE_ID_LOAD_LABELED_TRANSACTIONS_RAW = '7a589fa0-1cfd-4640-b45e-fc74353cbaed'
AIRBYTE_JOB_ID_RAW_TO_STAGING = '18c3fe6d-b9e0-430a-84c7-43d9be128939'

@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['airbyte','risk'],
)
def customer_metrics_test_dag():    
    
    load_customer_transactions_raw = AirbyteTriggerSyncOperator(
        task_id='load_customer_transactions_raw',
        airbyte_conn_id='airbyte',
        connection_id=AIRBYTE_ID_LOAD_CUSTOMER_TRANSACTIONS_RAW
    )

    load_labeled_transactions_raw = AirbyteTriggerSyncOperator(
        task_id='load_labeled_transactions_raw',
        airbyte_conn_id='airbyte',
        connection_id=AIRBYTE_ID_LOAD_LABELED_TRANSACTIONS_RAW
    )
    
    write_to_staging = AirbyteTriggerSyncOperator(
        task_id='write_to_staging',
        airbyte_conn_id='airbyte',
        connection_id=AIRBYTE_JOB_ID_RAW_TO_STAGING
    )

    @task
    def airbyte_jobs_done():
        return True
    
    @task.external_python(python='/opt/airflow/soda_venv/bin/python')
    def audit_customer_transactions(scan_name='customer_transactions',
                                    checks_subpath='tables',
                                    data_source='staging'):
        from include.soda.helpers import check
        check(scan_name, checks_subpath, data_source)

    @task.external_python(python='/opt/airflow/soda_venv/bin/python')
    def audit_labeled_transactions(scan_name='labeled_transactions',
                                    checks_subpath='tables',
                                    data_source='staging'):
        from include.soda.helpers import check
        check(scan_name, checks_subpath, data_source)
    
    @task
    def quality_checks_done():
        return True
    
    chain(
        [load_customer_transactions_raw, load_labeled_transactions_raw], 
        write_to_staging,
        airbyte_jobs_done(),
        [audit_customer_transactions(), audit_labeled_transactions()],
        quality_checks_done()
    )

customer_metrics_test_dag()