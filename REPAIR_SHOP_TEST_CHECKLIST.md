# Repair Shop Test Checklist

Use this checklist when validating the crawler and automotive search pipeline in a real workshop environment.

Related docs:

- `WORKSHOP_REHEARSAL_RUNBOOK.md`
- `WORKSHOP_TEST_RESULTS_TEMPLATE.md`
- `LIVE_CAPTURE_SETUP.md`
- `CRAWLING_OPTIMIZATION_GUIDE.md`

## Preflight

- Set `ADMIN_PASSWORD` in `.env` or deployment secrets.
- Confirm `NEXT_PUBLIC_SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` are set.
- Confirm `GOOGLE_API_KEY` or `GEMINI_API_KEY` is set.
- Open the Streamlit console and verify Gemini and Supabase both show as connected.
- Choose the correct schema before each test run.

## Acceptance Rules

- `Direct` means the record is automatically promoted into `parts`.
- `Pending` means the record is held for human review in `pending_data`.
- `dead_letters` should receive blocked, broken, empty, or failed URLs.
- `path_detail` content must not auto-publish if the part number is missing or placeholder.
- Document-style content such as manuals, wiring, VIN, and DTC should usually remain review-first unless your business rules explicitly change that behavior.

## Core Scenarios

| ID | Scenario | Input | Schema | Expected Result | Pass Criteria |
| --- | --- | --- | --- | --- | --- |
| 1 | Part detail with valid part number | Public product/detail URL with clear OEM part number and fitment data | `path_detail` | `Direct` or high-confidence `Pending` depending on threshold | `summary`, `extracted_facts`, and non-placeholder `part_number` are present |
| 2 | Part detail missing part number | Public detail/spec page without a trustworthy part number | `path_detail` | `Pending` only | No auto-publish into `parts`; `quality_reasons` should reflect identifier weakness |
| 3 | Service manual page | Public repair manual URL with torque and procedure | `path_manual` | `Pending` | Procedure summary and torque-related fields are preserved |
| 4 | Body manual page | Panel removal/install page | `path_body_manual` | `Pending` | Steps, cautions, and body-related notes are visible in payload |
| 5 | Wiring or connector page | Wiring diagram or connector reference page | `path_wiring` or `path_connector` | `Pending` | Connector or signal facts are captured without forcing a fake part number |
| 6 | Vehicle ID / registration reference | VIN, paint code, engine code, or label location page | `path_vehicle_id` | `Pending` | Identification facts are stored and `part_number` stays non-promoted |
| 7 | DTC page | Public DTC explanation page | `path_dtc` | `Pending` | Code, cause, and inspection notes are captured |
| 8 | Broken or blocked URL | 403, login wall, timeout, bad route, or blocked content | Any | `dead_letters` | URL and failure reason are recorded for retry or investigation |
| 9 | Batch crawl category run | Category or index URL with multiple children | Match target schema | Mix of `Direct`, `Pending`, and `Skipped` | Batch summary shows realistic counts and route statuses |
| 10 | Repeat crawl of same stable page | Re-run a previously crawled URL | Same as first run | Cache skip or stable duplicate behavior | No noisy duplicate processing when content has not changed |

## Operator Review Checks

- Verify `quality_status`, `confidence_score`, and `quality_reasons` on sampled records.
- Confirm `summary` is short, factual, and useful to a technician or parts advisor.
- Confirm `vehicle`, `compatibility`, and `specifications` are present when the source supports them.
- Confirm `source_url`, `schema_key`, and `source_path_hint` are correct.
- Confirm false positives are not being auto-promoted into `parts`.

## Failure Triage

- If a page fails to crawl, check the `dead_letters` screen first.
- If a page lands in `Pending` unexpectedly, inspect `confidence_score`, `auto_publish_ready`, and `quality_reasons`.
- If the crawler finds content but stores little structure, compare the selected schema against the actual page type.
- If batch runs show many `Skipped` rows, inspect `route_status`, `skip_reason`, and `cache_hit`.

## Sign-off

A workshop-ready build should pass all of the following:

- At least one real `path_detail` page auto-publishes correctly.
- At least one weak `path_detail` page is held in `Pending`.
- Manual, wiring, VIN, and DTC pages do not create unsafe direct parts records.
- Broken URLs are visible in `dead_letters`.
- Operators can explain every `Direct`, `Pending`, and `Skipped` result from the saved metadata.
