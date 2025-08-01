from openai import OpenAI
import os
from dotenv import load_dotenv
load_dotenv()
import json
from db import timing

openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

PROMPT_TEMPLATE = """
You are analyzing a conversation between a user and a travel assistant. Your goal is to identify the distinct tasks mentioned by the user and classify them using a label from the predefined taxonomy.

Each task should be returned as a JSON object with the following fields:
- in_scope: true if the task is a question about a specific tourist activity, false otherwise.
- label: a generic label that best fits the task, based on the taxonomy provided below. If no match is found, use "Otras".
- value: one of "solved", "partially_solved", or "not_solved", depending on whether the assistant resolved the task.

Here is the taxonomy:

{intents_json}

Here is the conversation payload:

{conversation_json}

Return ONLY the resulting tasks as a JSON array. Do not include any explanation, headings, or Markdown formatting.
"""



@timing(f"⏱️ Fetch summary for llm call")
def call_openai_summary_evaluation( intents_json: list ,conversation_json: list) -> str:
    formatted_prompt = PROMPT_TEMPLATE.format(
        conversation_json=json.dumps(conversation_json, ensure_ascii=False, indent=2),
        intents_json=json.dumps(intents_json, ensure_ascii=False, indent=2)
    )
    response = openai_client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
             {"role": "system", "content": formatted_prompt}
        ],
        temperature=0.3
    )

    return response.choices[0].message.content


