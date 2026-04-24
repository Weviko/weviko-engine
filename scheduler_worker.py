import logging
import os
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

from streamlit_services import (
    fetch_due_scheduled_crawls,
    execute_scheduled_crawl_job,
)

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

load_dotenv()


def _env_int(name: str, default: int) -> int:
    raw_value = str(os.getenv(name, "") or "").strip()
    if not raw_value:
        return default
    try:
        return int(raw_value)
    except ValueError:
        return default


def run_scheduler_worker() -> None:
    """
    Main loop for the scheduler worker.
    It periodically fetches due scheduled crawls and executes them.
    """
    interval_seconds = _env_int("SCHEDULER_WORKER_INTERVAL_SECONDS", 60)
    logger.info(
        f"Starting Weviko Scheduler Worker. Polling interval: {interval_seconds} seconds."
    )

    while True:
        try:
            logger.info(
                f"[{datetime.now(timezone.utc).isoformat()}] Checking for due scheduled crawls..."
            )
            due_crawls = fetch_due_scheduled_crawls(limit=5)  # Process a few at a time

            if not due_crawls:
                logger.info("No scheduled crawls are due.")
            else:
                logger.info(f"Found {len(due_crawls)} scheduled crawls to execute.")
                for crawl_job in due_crawls:
                    schedule_id = crawl_job["id"]
                    logger.info(
                        f"Executing scheduled crawl job: {schedule_id} (URL: {crawl_job['start_url']})"
                    )
                    execute_scheduled_crawl_job(schedule_id)
                    logger.info(
                        f"Finished executing scheduled crawl job: {schedule_id}"
                    )

        except Exception as exc:
            logger.critical(
                f"FATAL: Unhandled exception in scheduler worker loop: {exc}",
                exc_info=True,
            )

        logger.info(f"Sleeping for {interval_seconds} seconds...")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    run_scheduler_worker()
