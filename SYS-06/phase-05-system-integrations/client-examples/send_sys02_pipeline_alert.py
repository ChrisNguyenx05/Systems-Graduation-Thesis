"""Example: SYS-02 (Airflow) emitting a DAG failure callback alert."""

import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

from src.alert_dispatcher.dispatcher import AlertDispatcherEngine
from src.alert_dispatcher.models import AlertPayload


def airflow_on_failure_callback(context_info: dict):
    """Function structure usable directly inside Airflow DAG `on_failure_callback`."""
    print("[SYS-02 Airflow] Triggering DAG Failure Alert...")
    payload = AlertPayload(
        alert_type="PIPELINE_FAILURE",
        level="CRITICAL",
        source_system="SYS-02",
        event="DAG_RUN_FAILED",
        deduplicate_key=f"dag_failure:{context_info['dag_id']}:{context_info['task_id']}",
        message=f"DAG '{context_info['dag_id']}' gặp lỗi nghiêm trọng tại task '{context_info['task_id']}'.",
        receiver_roles=["data_engineer", "devops_sre"],
        channels=["telegram", "slack"],
        action_url=f"http://localhost:8080/dags/{context_info['dag_id']}/grid",
        suggested_action="Xem Airflow task logs và retry lại bước transform dữ liệu.",
        metadata=context_info
    )

    import asyncio
    engine = AlertDispatcherEngine()
    result = asyncio.run(engine.dispatch(payload))
    print(f"Airflow Callback Alert Result: {result.model_dump()}")


if __name__ == "__main__":
    sample_context = {
        "dag_id": "crypto_stream_bronze_to_silver",
        "task_id": "validate_schema_and_dedup",
        "execution_date": "2026-08-23T12:00:00Z",
        "try_number": 3,
        "exception": "JSONDecodeError: Expecting property name enclosed in double quotes"
    }
    airflow_on_failure_callback(sample_context)
