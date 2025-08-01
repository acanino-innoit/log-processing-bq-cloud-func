
import json
import asyncio
import db
import pandas as pd
import llm_functions as llm
from db import bq_client as bq_client, async_timing
import warnings
from functions_framework import http

# client overwritten to test locally # remove when we are in cloud env


PROJECT_ID = db.bq_config.project_id

    


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



@async_timing(f"⏱️ Fetch summary for OVERALL function")
async def evaluate_all_threads(bq_client=bq_client, bq_config=db.bq_config):
    # Step 1: Fetch thread IDs to process
    thread_id_list = db.fetch_thread_ids_to_process(bq_client, bq_config)
    if not thread_id_list:
        print("✅ No conversations to be processed. Exiting.")
        return {"status": "done", "message": "No conversations to be processed."}
    print(f"✅ Identified {len(thread_id_list)} Conversations to be processed.")

    db.create_table_if_not_exists(bq_client, bq_config, bq_config.processed_table, db.processed_schema)
    db.create_table_if_not_exists(bq_client, bq_config, bq_config.task_table, db.task_details_schema)

    # Step 2: Read intents JSON
    try:
        print("📄 Reading intents_summary.csv...")
        intents_csv = pd.read_csv("intents_summary.csv")
        records = intents_csv.to_dict(orient="records")
        intents_json = json.dumps(records, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"❌ Error loading intents_summary.csv: {e}")
        intents_json = "[]"

    # Step 3: Preload all summaries and created_at
    thread_data_map = db.optimize_get_summary_and_created_at(
        bq_client, bq_config.full_table_id(bq_config.thread_table), thread_id_list
    )

    processed_rows = []
    task_rows = []
    successful_threads = []
    failed_threads = []
    rollback_threads = []

    # Step 4: Process each thread
    for thread_id in thread_id_list:
        data = thread_data_map.get(thread_id)
        if not data or not data.get("summary_json"):
            print(f"⚠️ No summary found for thread {thread_id}")
            failed_threads.append(thread_id)
            continue

        summary = data["summary_json"]
        created_at = data["created_at"]

        try:
            result = llm.call_openai_summary_evaluation(intents_json, summary)
            print(f"🧠 Evaluation for thread {thread_id} completed.")

            cleaned_result = result.strip().removeprefix("```json").removesuffix("```").strip()
            if not cleaned_result.startswith("["):
                print(f"⚠️ Unexpected format from LLM for thread {thread_id}:\n{result}")

            parsed_result = json.loads(cleaned_result)
            metrics = extract_task_metrics(parsed_result)

            processed_rows.append({
                "thread_id": thread_id,
                "created_at": created_at,
                "total_tasks": metrics["total_tasks"],
                "in_scope_tasks": metrics["in_scope_tasks"],
                "out_scope_tasks": metrics["out_scope_tasks"],
                "solved": metrics["solved_tasks"],
                "partially_solved": metrics["partially_solved_tasks"],
                "not_solved": metrics["not_solved_tasks"],
                "out_scope_not_solved": metrics["out_scope_not_solved_tasks"],
                "evaluation_json": json.dumps(parsed_result, ensure_ascii=False),
            })

            for task in parsed_result:
                task_rows.append({
                    "thread_id": thread_id,
                    "created_at": created_at,
                    "in_scope": task.get("in_scope"),
                    "label": task.get("label"),
                    "value": task.get("value"),
                })

            successful_threads.append(thread_id)

        except Exception as e:
            print(f"❌ Failed to evaluate thread {thread_id}: {e}")
            failed_threads.append(thread_id)
            rollback_threads.append(thread_id)

    # Step 5: Bulk insert
    try:
        db.optimize_bulk_inserts(
            bq_client,
            bq_config.full_table_id(bq_config.processed_table),
            bq_config.full_table_id(bq_config.task_table),
            processed_rows,
            task_rows
        )
    except Exception as e:
        print(f"❌ Bulk insert failed: {e}")
        rollback_threads += successful_threads
        successful_threads = []

    # Step 6: Rollback if necessary
    if rollback_threads:
        db.rollback_thread_data_bq(bq_client, bq_config, rollback_threads)

    # Step 7: Bulk mark threads
    if successful_threads:
        db.mark_threads_as_processed_bq(bq_client, bq_config, successful_threads)
    if failed_threads:
        db.mark_threads_as_error_bq(bq_client, bq_config, failed_threads)

    return {
        "status": "done",
        "message": f"✅ {len(successful_threads)} threads processed, ❌ {len(failed_threads)} failed."
    }

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
    
