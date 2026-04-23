# Crawling Optimization Guide

## Best collection path by source

- Public part catalog pages with stable URLs
  Use the URL factory.
- Logged-in pages, filtered search results, or pages that depend on human navigation
  Use live browser capture.
- Screenshots, PDFs, wiring sheets, torque charts
  Use Vision capture.

## Fastest safe rollout for a workshop

1. `Vision` for scanned manuals and screenshots.
2. `Live browser capture` for logged-in supplier or OEM pages the operator is already viewing.
3. `URL factory` only for stable public routes that repeat the same structure.

This order usually gives the best speed with the least rework.

For live browser capture:

- Chrome extension is the preferred operator experience.
- Bookmarklet is the fastest fallback when installation is inconvenient.

## Recommended factory presets

For early workshop testing:

- `worker_count=3`
- `max_depth=2`
- `max_urls=50`
- destination: `pending`

For trusted public category pages after validation:

- `worker_count=4` to `5`
- `max_depth=2`
- `max_urls=100`
- destination: `parts` only for `path_detail`

Avoid high worker counts unless the source is proven stable and rate-limit tolerant.

## Quality tips that improve speed

- Always choose the closest schema before crawling.
- Keep `path_hint` narrow so the spider ignores unrelated pages.
- For live capture, highlight the exact part block or compatibility table before clicking the bookmarklet.
- For part detail pages, make sure the visible area includes the real part number.
- For manuals, wiring, DTC, and VIN pages, prefer `pending` because document pages should stay reviewable.

## Where the current system is strongest

- `path_detail`
  Good candidate for `Direct` once the page consistently exposes a real part number.
- `path_manual`, `path_wiring`, `path_connector`, `path_dtc`, `path_vehicle_id`
  Best handled as document-type records with review.
- `dead_letters`
  Now useful for failed routes and weak captures. Review it daily during rollout.

## Simple operating rules

- Use `Direct` only for structured part-detail pages.
- Use `Pending` for everything else.
- Keep host allowlists small.
- Keep one operator workflow per source type.
- Review the first 20 to 30 captures before widening automation.

## Good next improvements after the first workshop test

- Domain-specific prompts by supplier or OEM site.
- A lightweight Chrome extension using `activeTab` for more stable current-tab capture.
- A duplicate detector keyed by `source_url + part_number + schema_key`.
- A workshop preset screen with one-click schema and destination templates.
