# Workshop Rehearsal Runbook

Use this runbook for the first end-to-end rehearsal inside a repair shop or pilot service center.

## Goal

Validate that the system can:

- crawl real automotive pages,
- classify them into the correct schema,
- auto-promote safe part-detail records,
- hold ambiguous records for review,
- and capture failures in `dead_letters`.

## Before You Start

Prepare these items first:

- 2 real `path_detail` URLs with clear OEM part numbers
- 1 weak `path_detail` URL without a reliable part number
- 1 `path_manual` URL
- 1 `path_vehicle_id` URL
- 1 intentionally broken or blocked URL
- 1 category/index URL for a batch crawl

Recommended operator roles:

- Operator A: runs the console
- Operator B: verifies workshop correctness of the payload
- Operator C: records results in the test template

## Phase 1: Environment Check

1. Start the app.
2. Log in with `ADMIN_PASSWORD`.
3. Confirm Gemini status is connected.
4. Confirm Supabase status is connected.
5. Open the settings page and verify the current `confidence_threshold`.

Go/no-go:

- Stop if Gemini or Supabase is disconnected.
- Stop if `ADMIN_PASSWORD` was not explicitly configured.

## Phase 2: Single URL Part Detail Test

Run this first because it confirms the most business-critical path.

1. Open the factory page.
2. Choose `단일 타겟 엔진`.
3. Select schema `path_detail`.
4. Set destination to direct mode.
5. Paste a real part-detail URL with a visible OEM part number.
6. Run the crawl.

Expected outcome:

- `confidence_score` is shown.
- `quality_status` is `ok` or `high`.
- `part_number` is not placeholder.
- Result is saved as `Direct` into `parts`.

If it lands in `Pending`:

- inspect `quality_reasons`
- verify the selected schema matches the page
- verify the page really contains a trustworthy part number

## Phase 3: Weak Part Detail Safety Test

This confirms the system does not auto-publish unsafe part records.

1. Keep schema as `path_detail`.
2. Use a page that contains automotive description/specs but no trustworthy part number.
3. Run again in direct mode.

Expected outcome:

- `confidence_score` may still be moderate or high
- `auto_publish_ready` should be `False`
- result should go to `Pending`, not `Direct`

This is a required safety pass.

## Phase 4: Manual / Procedure Test

1. Switch schema to `path_manual`.
2. Use a service or repair procedure URL.
3. Run in safe mode first.

Expected outcome:

- `summary` describes the repair content
- torque or service facts are preserved if present
- result is stored for review, not as a direct part record

## Phase 5: Vehicle Identification / Registration Test

1. Switch schema to `path_vehicle_id`.
2. Use a VIN, paint code, engine code, or label-location page.
3. Run in safe mode.

Expected outcome:

- identification facts are preserved
- fake part numbers are not invented
- result remains review-first

## Phase 6: Failure Capture Test

1. Use a blocked, expired, login-gated, or intentionally broken URL.
2. Run the crawl.
3. Open the `dead_letters` screen.

Expected outcome:

- the URL appears in `dead_letters`
- failure reason is visible
- operators can use that record for retry or investigation

## Phase 7: Batch Category Rehearsal

1. Switch to batch crawl mode.
2. Use a category/index URL.
3. Choose a realistic schema.
4. Run with a small limit first, such as `10` to `30` URLs.

Expected outcome:

- mixed `Direct`, `Pending`, and `Skipped` counts look realistic
- route summary is populated
- obviously broken or blocked pages do not silently disappear

Do not start with a large batch in the first workshop rehearsal.

## Phase 8: Human Review Flow

1. Open the review queue.
2. Approve one correct item.
3. Edit and approve one item that needs cleanup.
4. Reject one bad item.

Expected outcome:

- approved content moves cleanly into master storage
- edited approvals retain the corrected payload
- rejected items stop polluting the pipeline

## Exit Criteria

The rehearsal is successful when all of the following are true:

- at least one real part-detail URL auto-publishes correctly
- at least one weak part-detail URL is held in `Pending`
- at least one manual or vehicle-ID page is captured without unsafe direct promotion
- at least one failed URL is visible in `dead_letters`
- operators can explain why each sample landed in `Direct`, `Pending`, or `Skipped`

## Recommended First-Day Limits

- Single-target tests: 5 to 8 URLs total
- Batch category test: 10 to 30 URLs
- Review queue audit: at least 5 records

After this first rehearsal passes, increase batch size gradually instead of jumping straight to production volume.
