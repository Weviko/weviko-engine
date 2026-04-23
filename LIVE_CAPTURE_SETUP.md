# Live Browser Capture Setup

## What this adds

`live_capture_server.py` lets an operator send the currently opened browser tab into the existing Weviko pipeline.

- Manual only: the operator clicks a bookmarklet from the current page.
- Safer by default: only allowed hosts can send data.
- Pending-first rollout: direct live capture is disabled unless you explicitly enable it.
- Copyright and storage risk reduction: the system stores structured facts, not full HTML pages.

## Files

- `live_capture.py`
- `live_capture_server.py`
- `chrome_extension/weviko-live-capture`
- `streamlit_app.py`
- `streamlit_services.py`

## 1. Environment setup

Set these values in `.env`.

```env
WEVIKO_LIVE_CAPTURE_SCHEME=http
WEVIKO_LIVE_CAPTURE_HOST=127.0.0.1
WEVIKO_LIVE_CAPTURE_PORT=8765
WEVIKO_ALLOWED_CAPTURE_HOSTS=www.weviko.com,weviko.com,localhost,127.0.0.1
WEVIKO_LIVE_CAPTURE_DIRECT_ENABLED=false
```

Recommended first rollout:

- Keep `WEVIKO_LIVE_CAPTURE_DIRECT_ENABLED=false`
- Keep `WEVIKO_ALLOWED_CAPTURE_HOSTS` narrow
- Expand hosts only after contract and authorization are confirmed

## 2. Start the local server

```bash
python live_capture_server.py
```

Then open:

```text
http://127.0.0.1:8765
```

You will see a setup page with the bookmarklet code and health status.

## 3. Install the bookmarklet

1. Open the setup page.
2. Drag `Weviko Capture` to the bookmarks bar.
3. If drag-and-drop is awkward, create a new bookmark manually.
4. Paste the bookmarklet code from the setup page into the bookmark URL field.

## 3A. Install the Chrome extension

If you want a more stable current-tab workflow, load the unpacked extension from:

```text
chrome_extension/weviko-live-capture
```

Detailed steps are in `CHROME_EXTENSION_SETUP.md`.

## 4. Operator workflow

1. Open an allowed site and navigate to the exact page you want.
2. Apply login, filters, or search conditions manually first.
3. Select the most relevant text on the page if possible.
4. Click the bookmarklet.
5. Confirm `schema_key` and destination.
6. Review the result in `Pending`, `Parts`, or `dead_letters`.

## 5. Destination policy

- `pending`
  Use this for workshop rollout, mixed-quality sources, manuals, VIN pages, and unknown sites.
- `parts`
  Use this only after the source is validated and `WEVIKO_LIVE_CAPTURE_DIRECT_ENABLED=true`.

If direct mode is requested while live direct is disabled, the server automatically downgrades the capture to `pending`.

## 6. Legal and operational guardrails

This is not legal advice. Use the feature only inside your permission scope.

- Capture only sites you own, operate, license, or are contractually authorized to access.
- Do not use it to bypass login, paywalls, technical blocks, or terms you are not permitted to bypass.
- Do not bulk-copy manuals or store whole pages when a structured fact summary is enough.
- Keep host allowlists explicit and review them periodically.
- Minimize personal data. Avoid collecting customer names, phone numbers, addresses, and unrelated account data.
- For VIN, license plate, and workshop customer records, keep only the fields required for the job.

## 7. Troubleshooting

- `host_not_allowed`
  Add the exact domain to `WEVIKO_ALLOWED_CAPTURE_HOSTS` only if you are authorized to use it.
- `Capture payload exceeds ... bytes`
  Reduce page complexity or lower the live capture limits in `.env`.
- `The current tab did not yield enough visible text`
  Scroll the page fully, open the detail view, or manually select a useful text block before sending.
- Saved to `Pending` instead of `Direct`
  This is expected if direct live capture is disabled or the confidence gate was not met.

## 8. Recommended workshop rollout

1. Start with one approved domain.
2. Run all live captures into `Pending`.
3. Verify schema quality, part number quality, and dead-letter rate.
4. Enable direct live capture only for proven `path_detail` pages.
5. Keep manuals, wiring, connector references, and VIN-related pages on `Pending`.
