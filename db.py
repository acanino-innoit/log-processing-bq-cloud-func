
import json
from config_bq import BQConfig
from google.cloud import bigquery
import pandas as pd
import time
from functools import wraps
from typing import List

def timing(label="⏱️ Execution time"):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            print(f"{label} - started")
            start = time.time()
            result = func(*args, **kwargs)
            end = time.time()
            print(f"{label} - finished in {end - start:.2f} sec")
            return result
        return wrapper
    return decorator


def async_timing(label="⏱️ Execution time"):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            print(f"{label} - started")
            start = time.time()
            result = await func(*args, **kwargs)
            end = time.time()
            print(f"{label} - finished in {end - start:.2f} sec")
            return result
        return wrapper
    return decorator



@timing(f"⏱️ Fetch timing for mark_thread_as_processed_bq ")
def mark_threads_as_processed_bq(client: bigquery.Client, config: BQConfig, thread_ids: List[str]) -> None:
    if not thread_ids:
        return

    try:
        query = f"""
        UPDATE `{config.full_table_id(config.thread_table)}`
        SET state = 'processed'
        WHERE thread_id IN UNNEST(@thread_ids)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("thread_ids", "STRING", thread_ids)
            ]
        )
        client.query(query, job_config=job_config).result()
        print(f"✅ Marked {len(thread_ids)} threads as processed")
    except Exception as e:
        print(f"❌ Failed to bulk mark threads as processed: {e}")

@timing(f"⏱️ Fetch timing for mark_threads_as_error_bq ")
def mark_threads_as_error_bq(client: bigquery.Client, config: BQConfig, thread_ids: List[str]) -> None:
    if not thread_ids:
        return

    try:
        query = f"""
        UPDATE `{config.full_table_id(config.thread_table)}`
        SET state = 'error'
        WHERE thread_id IN UNNEST(@thread_ids)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("thread_ids", "STRING", thread_ids)
            ]
        )
        client.query(query, job_config=job_config).result()
        print(f"⚠️ Marked {len(thread_ids)} threads as error")
    except Exception as e:
        print(f"❌ Failed to bulk mark threads as error: {e}")


def rollback_thread_data_bq(client: bigquery.Client, bq_config: BQConfig, thread_id: str, inserted: dict):
    """
    Rolls back inserted data based on a flag dict like {"processed": True, "tasks": False}
    """
    try:
        if inserted.get("processed"):
            table_id = bq_config.full_table_id(bq_config.processed_table)
            client.query(
                f"DELETE FROM `{table_id}` WHERE thread_id = @thread_id",
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("thread_id", "STRING", thread_id)]
                )
            ).result()
            print(f"🧹 Rolled back from {bq_config.processed_table}")

        if inserted.get("tasks"):
            table_id = bq_config.full_table_id(bq_config.task_table)
            client.query(
                f"DELETE FROM `{table_id}` WHERE thread_id = @thread_id",
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ScalarQueryParameter("thread_id", "STRING", thread_id)]
                )
            ).result()
            print(f"🧹 Rolled back from {bq_config.task_table}")

    except Exception as e:
        print(f"⚠️ Rollback failed for thread_id {thread_id}: {e}")



@timing(f"⏱️ Fetch summary for initial fetch_thread_ids_to_process function")
def fetch_thread_ids_to_process(client: bigquery.Client, config: BQConfig) -> list:
    try:
        table_id = config.full_table_id(config.thread_table)
        query = f"""
        SELECT thread_id
        FROM `{table_id}`
        WHERE state IN ('to_be_processed', 'error')
        """
        df = client.query(query).to_dataframe()

        if df.empty:
            print("⚠️ No thread IDs found with state 'to_be_processed' or 'error'.")

        return df["thread_id"].tolist()

    except Exception as e:
        print(f"❌ Failed to fetch thread IDs from BigQuery: {e}")
        return []

def create_table_if_not_exists(client: bigquery.Client, config: BQConfig, table_name: str, schema: list) -> None:
    table_id = config.full_table_id(table_name)
    try:
        client.get_table(table_id)
        print(f"✅ Table {table_id} already exists.")
    except Exception:
        print(f"🔧 Creating table {table_id}...")
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        print(f"✅ Table {table_id} created.")


    

@timing(f"⏱️ Fetch timing for initial optimize_get_summary_and_created_at")
def optimize_get_summary_and_created_at(client, full_table_id, thread_ids):
    try:
        query = f"""
        SELECT thread_id, summary_json, created_at
        FROM `{full_table_id}`
        WHERE thread_id IN UNNEST(@thread_ids)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("thread_ids", "STRING", thread_ids)
            ]
        )
        df = client.query(query, job_config=job_config).to_dataframe()
        return {
            row["thread_id"]: {
                "summary_json": json.loads(row["summary_json"]),
                "created_at": row["created_at"]
            }
            for _, row in df.iterrows()
        }
    except Exception as e:
        print(f"❌ Failed to batch fetch summaries: {e}")
        return {}

@timing(f"⏱️ Fetch timing for initial optimize_bulk_inserts")
def optimize_bulk_inserts(
    client, processed_table_id, task_table_id,
    processed_rows: list[dict],
    task_rows: list[dict]
):
    try:
        if processed_rows:
            df_proc = pd.DataFrame(processed_rows)
            client.load_table_from_dataframe(df_proc, processed_table_id).result()
            print(f"✅ Inserted {len(df_proc)} rows into processed_conversation")

        if task_rows:
            df_task = pd.DataFrame(task_rows)
            client.load_table_from_dataframe(df_task, task_table_id).result()
            print(f"✅ Inserted {len(df_task)} rows into task_details")
    except Exception as e:
        print(f"❌ Failed bulk insert: {e}")

# init variables:
bq_config = BQConfig()

bq_client = bigquery.Client(project=bq_config.project_id)

# init schemas:
processed_schema = [
    bigquery.SchemaField("thread_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("created_at", "INTEGER"),
    bigquery.SchemaField("total_tasks", "INTEGER"),
    bigquery.SchemaField("in_scope_tasks", "INTEGER"),
    bigquery.SchemaField("out_scope_tasks", "INTEGER"),
    bigquery.SchemaField("solved", "INTEGER"),
    bigquery.SchemaField("partially_solved", "INTEGER"),
    bigquery.SchemaField("not_solved", "INTEGER"),
    bigquery.SchemaField("out_scope_not_solved", "INTEGER"),
    bigquery.SchemaField("evaluation_json", "STRING"),
]

task_details_schema = [
    bigquery.SchemaField("thread_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("created_at", "INTEGER"),
    bigquery.SchemaField("in_scope", "BOOLEAN"),
    bigquery.SchemaField("label", "STRING"),
    bigquery.SchemaField("value", "STRING"),
]