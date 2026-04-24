from __future__ import annotations

import os
import logging
import time
from typing import Any

from dotenv import load_dotenv

from streamlit_services import (
    fetch_dead_letters,
    get_config_prompt,
    parse_dead_letter_metadata,
    process_scraped_text_and_save,
    resolve_dead_letter,
)
from weviko_engine import run_crawler_sync

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


def process_dead_letter_queue() -> None:
    """Fetches and processes a batch of dead letters."""
    logger.info("Checking for dead letters to retry...")
    items = fetch_dead_letters(limit=_env_int("RETRY_WORKER_BATCH_SIZE", 5))
    if not items:
        logger.info("No unresolved dead letters found.")
        return

    logger.info(f"Found {len(items)} dead letters to process.")
    proxy_url, _ = get_config_prompt("proxy_url", "")
    user_agent, _ = get_config_prompt("custom_user_agent", "")

    for item in items:
        item_id = item.get("id")
        url = item.get("url")
        error_reason = item.get("error_reason", "")
        if not item_id or not url:
            continue

        logger.info(f"Retrying URL: {url}")
        metadata = parse_dead_letter_metadata(error_reason)
        schema_key = metadata.get("schema_key", "path_detail")
        source_path_hint = metadata.get("path_hint", "")
        source_type = metadata.get("source_type", "dead_letter_retry")

        try:
            scraped_text = run_crawler_sync(
                url,
                proxy=proxy_url.strip() or None,
                user_agent=user_agent.strip() or None,
            )

            if not scraped_text:
                logger.warning(
                    f"  -> Retry failed: still unable to crawl content from {url}"
                )
                continue

            logger.info(
                f"  -> Crawled {len(scraped_text):,} chars. Processing with Gemini..."
            )
            _, save_result = process_scraped_text_and_save(
                scraped_text=scraped_text,
                doc_type_key=schema_key,
                market="GLOBAL",
                destination="pending",  # Retries always go to pending for review
                source_path_hint=source_path_hint,
                document_type=f"Retry of {schema_key}",
                source_url=url,
                source_type_override=source_type,
            )

            if save_result.get("saved"):
                logger.info(
                    "  -> Successfully processed and saved. Marking as resolved."
                )
                resolve_result = resolve_dead_letter(item_id)
                if not resolve_result["saved"]:
                    logger.error(
                        f"  -> WARNING: Failed to mark as resolved: {resolve_result['message']}"
                    )
            else:
                logger.warning(
                    f"  -> Failed to save processed data: {save_result.get('message')}"
                )

        except Exception as exc:
            logger.error(
                f"  -> An unexpected error occurred during retry: {exc}", exc_info=True
            )


def run_worker() -> None:
    """Main worker loop."""
    interval_seconds = _env_int("RETRY_WORKER_INTERVAL_SECONDS", 300)
    logger.info(
        f"Starting dead letter retry worker. Interval: {interval_seconds} seconds."
    )
    while True:
        try:
            process_dead_letter_queue()
        except Exception as exc:
            logger.critical(
                f"FATAL: Unhandled exception in worker loop: {exc}", exc_info=True
            )

        logger.info(f"Sleeping for {interval_seconds} seconds...")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    run_worker()
