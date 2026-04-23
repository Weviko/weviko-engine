# Workshop Test Results Template

Copy this template for each rehearsal day.

## Session Info

- Date:
- Location:
- Operator:
- Reviewer:
- Environment:
- App version / commit:
- Confidence threshold:

## Preflight Check

| Check | Status | Notes |
| --- | --- | --- |
| ADMIN_PASSWORD configured | Pass / Fail | |
| Gemini connected | Pass / Fail | |
| Supabase connected | Pass / Fail | |
| Review queue accessible | Pass / Fail | |
| Dead letters screen accessible | Pass / Fail | |

## Single URL Test Results

| Case ID | URL | Schema | Destination Mode | Result | Confidence | Quality | Part Number | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | | `path_detail` | Direct | | | | | |
| 2 | | `path_detail` | Direct | | | | | |
| 3 | | `path_manual` | Safe | | | | | |
| 4 | | `path_vehicle_id` | Safe | | | | | |
| 5 | | Any | Any | | | | | |

Result guidance:

- `Direct`
- `Pending`
- `Skipped`
- `Dead Letter`
- `Failed`

## Batch Crawl Result

- Batch URL:
- Schema:
- Max URLs:
- Workers:
- Direct count:
- Pending count:
- Skipped count:
- Notable route statuses:
- Overall notes:

## Review Queue Audit

| Item | Action Taken | Expected | Actual | Notes |
| --- | --- | --- | --- | --- |
| 1 | Approve | | | |
| 2 | Edit + Approve | | | |
| 3 | Reject | | | |

## Failure Audit

| URL | Failure Type | Logged in dead_letters | Notes |
| --- | --- | --- | --- |
| 1 | | Yes / No | |
| 2 | | Yes / No | |

## Sign-off Summary

- What worked well:
- What needs adjustment:
- Safe to expand batch size: Yes / No
- Safe for pilot operators: Yes / No
- Blockers before next rehearsal:
