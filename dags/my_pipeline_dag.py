from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

NEWS_PYTHON = os.path.join(os.path.dirname(__file__), "..", "news", ".venv", "bin", "python")
NEWS_DIR = os.path.join(os.path.dirname(__file__), "..", "news")
DBT_DIR = os.path.join(os.path.dirname(__file__), "..", "news", "news_sentiment")
DBT_BIN = os.path.join(os.path.dirname(__file__), "..", "news", ".venv", "bin", "dbt")
DBT_CMD = f"{NEWS_PYTHON} {DBT_BIN}"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="my_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",        # or None to trigger manually
    catchup=False,
    default_args=default_args,
) as dag:

    extract_guardian = BashOperator(
        task_id="extract_guardian",
        bash_command=f"cd {NEWS_DIR} && {NEWS_PYTHON} -c 'import news_extract; news_extract.extract_guardian()'",
    )

    extract_nytimes = BashOperator(
        task_id="extract_nytimes",
        bash_command=f"cd {NEWS_DIR} && {NEWS_PYTHON} -c 'import news_extract; news_extract.extract_nytimes()'",
    )

    extract_bbc = BashOperator(
        task_id="extract_bbc",
        bash_command=f"cd {NEWS_DIR} && {NEWS_PYTHON} -c 'import news_extract; news_extract.extract_bbc()'",
    )

    extract_foxnews = BashOperator(
        task_id="extract_foxnews",
        bash_command=f"cd {NEWS_DIR} && {NEWS_PYTHON} -c 'import news_extract; news_extract.extract_foxnews()'",
    )

    extract_cnn = BashOperator(
        task_id="extract_cnn",
        bash_command=f"cd {NEWS_DIR} && {NEWS_PYTHON} -c 'import news_extract; news_extract.extract_cnn()'",
    )

    extract_npr = BashOperator(
        task_id="extract_npr",
        bash_command=f"cd {NEWS_DIR} && {NEWS_PYTHON} -c 'import news_extract; news_extract.extract_npr()'",
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_DIR} && {DBT_CMD} source freshness",
    )

    dbt_run_bronze_silver = BashOperator(
        task_id="dbt_run_bronze_silver",
        bash_command=f"cd {DBT_DIR} && {DBT_CMD} run --select bronze silver",
    )

    dbt_test_bronze_silver = BashOperator(
        task_id="dbt_test_bronze_silver",
        bash_command=f"cd {DBT_DIR} && {DBT_CMD} test --select bronze silver",
    )

    score_articles = BashOperator(
        task_id="score_articles",
        bash_command=f"cd {NEWS_DIR} && {NEWS_PYTHON} -c 'import news_score; news_score.score()'",
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"cd {DBT_DIR} && {DBT_CMD} run --select gold",
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=f"cd {DBT_DIR} && {DBT_CMD} test --select gold",
    )

    [extract_guardian, extract_nytimes, extract_bbc, extract_foxnews, extract_cnn, extract_npr] >> dbt_source_freshness >> dbt_run_bronze_silver >> dbt_test_bronze_silver >> score_articles >> dbt_run_gold >> dbt_test_gold
