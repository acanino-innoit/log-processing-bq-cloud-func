from dataclasses import dataclass

@dataclass
class BQConfig:
    project_id: str = "timbal-civi"
    dataset: str = "logs_timbal"
    thread_table: str = "thread_summary"
    processed_table: str = "processed_conversation"
    task_table: str = "task_details"

    def full_table_id(self, table_name: str) -> str:
            return f"{self.project_id}.{self.dataset}.{table_name}"