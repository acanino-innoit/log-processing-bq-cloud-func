
import json
from config_bq import BQConfig
from google.cloud import bigquery
import pandas as pd


# def insert_thread_summary(conn: sqlite3.Connection, thread_id: str, created_at: int, summary: list, state: str = "to_be_processed", user_id: str = None, user_mail: str = None) -> None:
#     cur = conn.cursor()
#     cur.execute(
#         """
#         INSERT OR REPLACE INTO thread_summary (thread_id, created_at, summary_json, state, user_id, user_mail)
#         VALUES (?, ?, ?, ?, ?, ?);
#         """,
#         (thread_id, created_at, json.dumps(summary, ensure_ascii=False), state, user_id, user_mail),
#     )
#     conn.commit()
def insert_task_details_bq(
    client: bigquery.Client,
    table_id: str,
    thread_id: str,
    tasks: list[dict],
    created_at: int
) -> None:
    if not tasks:
        print(f"⚠️ No task details to insert for thread {thread_id}.")
        return

    try:
        # Prepare DataFrame for insertion
        rows = [
            {
                "thread_id": thread_id,
                "created_at": created_at,
                "in_scope": task.get("in_scope"),
                "label": task.get("label"),
                "value": task.get("value")
            }
            for task in tasks
        ]
        df = pd.DataFrame(rows)

        # Load into BigQuery using WRITE_APPEND
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"✅ Inserted {len(df)} task details into {table_id} for thread {thread_id}")

    except Exception as e:
        print(f"❌ Failed to insert task details for thread {thread_id}: {e}")

# def update_user_info_if_missing(conn: sqlite3.Connection, thread_id: str, user_id: str, user_mail: str) -> None:
#     cur = conn.cursor()
#     cur.execute("""
#         UPDATE thread_summary
#         SET user_id = COALESCE(user_id, ?),
#             user_mail = COALESCE(user_mail, ?)
#         WHERE thread_id = ?;
#     """, (user_id, user_mail, thread_id))
#     conn.commit()

def row_exists(client: bigquery.Client, table_id: str, thread_id: str) -> bool:
    query = f"""
        SELECT 1 FROM `{table_id}` WHERE thread_id = @thread_id LIMIT 1
    """
    job = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("thread_id", "STRING", thread_id)
        ]
    ))
    return job.result().total_rows > 0


def insert_processed_conversation_bq(
    client: bigquery.Client,
    table_id: str,
    thread_id: str,
    metrics: dict,
    eval_data: list,
    created_at: int
) -> None:
    try:
        row = {
            "thread_id": thread_id,
            "created_at": created_at,
            "total_tasks": metrics["total_tasks"],
            "in_scope_tasks": metrics["in_scope_tasks"],
            "out_scope_tasks": metrics["out_scope_tasks"],
            "solved": metrics["solved_tasks"],
            "partially_solved": metrics["partially_solved_tasks"],
            "not_solved": metrics["not_solved_tasks"],
            "out_scope_not_solved": metrics["out_scope_not_solved_tasks"],
            "evaluation_json": json.dumps(eval_data, ensure_ascii=False)
        }

        df = pd.DataFrame([row])

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE" if row_exists(client, table_id, thread_id) else "WRITE_APPEND"
        )

        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"✅ Row for thread_id {thread_id} inserted/updated in processed_conversation")

    except Exception as e:
        print(f"❌ Error inserting processed_conversation for {thread_id}: {e}")
        raise


def mark_thread_as_processed_bq(client: bigquery.Client, config: BQConfig, thread_id: str) -> None:
    try:
        query = f"""
        UPDATE `{config.full_table_id(config.thread_table)}`
        SET state = 'processed'
        WHERE thread_id = @thread_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("thread_id", "STRING", thread_id)
            ]
        )
        client.query(query, job_config=job_config).result()
        print(f"✅ Marked thread {thread_id} as processed")
    except Exception as e:
        print(f"❌ Failed to mark thread {thread_id} as processed: {e}")

def mark_thread_as_error_bq(client: bigquery.Client, config: BQConfig, thread_id: str) -> None:
    try:
        query = f"""
        UPDATE `{config.full_table_id(config.thread_table)}`
        SET state = 'error'
        WHERE thread_id = @thread_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("thread_id", "STRING", thread_id)
            ]
        )
        client.query(query, job_config=job_config).result()
        print(f"⚠️ Marked thread {thread_id} as error")
    except Exception as e:
        print(f"❌ Failed to mark thread {thread_id} as error: {e}")


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


def get_summary_json_by_thread_id_bq(client: bigquery.Client, config: BQConfig, thread_id: str) -> list:
    try:
        table_id = config.full_table_id(config.thread_table)
        query = f"""
            SELECT summary_json
            FROM `{table_id}`
            WHERE thread_id = @thread_id
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("thread_id", "STRING", thread_id)
            ]
        )
        result = client.query(query, job_config=job_config).to_dataframe()

        if not result.empty:
            return json.loads(result.iloc[0]["summary_json"])
        else:
            return []
    except Exception as e:
        print(f"❌ Failed to get summary for thread {thread_id}: {e}")
        return []



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


def get_thread_created_at_bq(client: bigquery.Client, config: BQConfig, thread_id: str) -> int | None:
    """
    Retrieve the created_at timestamp for a given thread_id from BigQuery.
    Returns None if the thread is not found or on error.
    """
    try:
        table_id = config.full_table_id(config.thread_table)
        query = f"""
        SELECT created_at
        FROM `{table_id}`
        WHERE thread_id = @thread_id
        LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("thread_id", "STRING", thread_id)
            ]
        )
        df = client.query(query, job_config=job_config).to_dataframe()

        if df.empty:
            print(f"⚠️ No created_at found for thread {thread_id}")
            return None

        return int(df.loc[0, "created_at"])

    except Exception as e:
        print(f"❌ Failed to fetch created_at for thread {thread_id}: {e}")
        return None


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