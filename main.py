
import json
import asyncio
import db
import pandas as pd
from google.cloud import bigquery
from  config_bq import BQConfig
import llm_functions as llm
from db import bq_client as bq_client
import warnings
from functions_framework import http
from db import timing

# client overwritten to test locally # remove when we are in cloud env


PROJECT_ID = db.bq_config.project_id
#DATASET = os.getenv("DATASET", "logs_timbal")
#LOCATION = os.getenv("LOCATION", "europe-west1")
#TABLE_THREAD = os.getenv("BQ_TABLE_THREAD", "thread_summary")
#TABLE_MESSAGE = os.getenv("BQ_TABLE_MESSAGE", "message_details")


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

    



@timing(f"⏱️ Fetch summary for initial extract_task_metrics function")
def extract_task_metrics(evaluation: list) -> dict:
    total_tasks = len(evaluation)
    in_scope = sum(1 for e in evaluation if e.get("in_scope"))
    out_scope = total_tasks - in_scope

    solved = sum(1 for e in evaluation if e.get("value") == "solved")
    partially_solved = sum(1 for e in evaluation if e.get("value") == "partially_solved")
    not_solved = sum(1 for e in evaluation if e.get("value") == "not_solved")

    out_scope_not_solved = sum(1 for e in evaluation if not e.get("in_scope") and e.get("value") == "not_solved")

    return {
        "total_tasks": total_tasks,
        "in_scope_tasks": in_scope,
        "out_scope_tasks": out_scope,
        "solved_tasks": solved,
        "partially_solved_tasks": partially_solved,
        "not_solved_tasks": not_solved,
        "out_scope_not_solved_tasks": out_scope_not_solved
    }



async def evaluate_all_threads(bq_client = bq_client, bq_config = db.bq_config):
    
    #checking what rows have to be processed: 
    thread_id_list = db.fetch_thread_ids_to_process(bq_client, bq_config)
    if not thread_id_list:
        print("✅ No conversations to be processed. Exiting.")
        return {"status": "done", "message": "No conversations to be processed."}
    print(f"✅ Identified {len(thread_id_list)} Conversations to be processed.")

    db.create_table_if_not_exists(bq_client, bq_config, bq_config.processed_table, db.processed_schema)
    db.create_table_if_not_exists(bq_client, bq_config, bq_config.task_table, db.task_details_schema)

    # reading the intents csv and parsing them into a json file.
    try:
        print("📄 Reading intents_summary.csv...")
        intents_csv = pd.read_csv("intents_summary.csv")
        records = intents_csv.to_dict(orient="records")
        intents_json = json.dumps(records, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"❌ Error loading intents_summary.csv: {e}")
        intents_json = "[]"
    
    
    # process threads individually
    for thread_id in thread_id_list:
        summary = db.get_summary_json_by_thread_id_bq(bq_client, bq_config, thread_id)
        if not summary:
            print(f"⚠️ No summary found for thread {thread_id}")
            continue

        try:
            # flags used to implement rollback logic
            inserted_flags = {"processed": False, "tasks": False}
            result = llm.call_openai_summary_evaluation(intents_json, summary)
            print(f"\n🧠 Evaluation for thread {thread_id} succesfully compleated")

            cleaned_result = result.strip().removeprefix("```json").removesuffix("```").strip()

            # Debugging output for malformed LLM responses
            if not cleaned_result.startswith("["):
                print(f"⚠️ Unexpected format from LLM for thread {thread_id}:\n{result}")

            parsed_result = json.loads(cleaned_result)
            metrics = extract_task_metrics(parsed_result)
            created_at = db.get_thread_created_at_bq(bq_client, bq_config, thread_id)
            # inserting data in tables:

            db.insert_processed_conversation_bq(
                bq_client,
                bq_config.full_table_id(bq_config.processed_table),
                thread_id,
                metrics,
                parsed_result,
                created_at
            )
            inserted_flags["processed"] = True
            db.insert_task_details_bq(
                    bq_client,
                    bq_config.full_table_id(bq_config.task_table),
                    thread_id,
                    parsed_result,
                    created_at
                )
            inserted_flags["tasks"] = True

            db.mark_thread_as_processed_bq(bq_client, bq_config, thread_id)
            

        except Exception as e:
            print(f"❌ Failed to evaluate thread {thread_id}: {e}")

            db.rollback_thread_data_bq(bq_client, bq_config, thread_id, inserted_flags)
            db.mark_thread_as_error_bq(bq_client, bq_config, thread_id)

@http
def process_logs_bq(request):
    """Cloud Function HTTP entrypoint."""
    warnings.filterwarnings("ignore")

    if request.method != "POST":
        return {"error": "Only POST allowed"}, 405

    print(" Received HTTP POST to trigger logs import")

    try:
        result = asyncio.run(evaluate_all_threads())
        print("✅ Preprocessing completed")
        return {"message": "Logs processing finished", "result": result}, 200
    except Exception as e:
        print(f"❌ Error during log processing: {e}")
        return {"error": str(e)}, 500
    
