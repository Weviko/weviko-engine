# Chrome Extension Setup

## Extension path

Load this folder as an unpacked Chrome extension:

```text
chrome_extension/weviko-live-capture
```

## What it does

The extension captures the active tab only after the operator clicks `Capture Tab`.

- Uses `activeTab`
- Reads the current tab with `chrome.scripting.executeScript`
- Sends the page snapshot to the local Weviko live capture server
- Reuses the existing `Pending`, `Parts`, and `dead_letters` pipeline

## Install steps

1. Open Chrome.
2. Go to `chrome://extensions`.
3. Turn on `Developer mode`.
4. Click `Load unpacked`.
5. Select `chrome_extension/weviko-live-capture`.

## Before first use

1. Start the local server:

```bash
python live_capture_server.py
```

2. Keep `.env` host allowlists narrow:

```env
WEVIKO_ALLOWED_CAPTURE_HOSTS=www.weviko.com,weviko.com
WEVIKO_LIVE_CAPTURE_DIRECT_ENABLED=false
```

3. In the extension popup, click `Check Server`.

## Operator flow

1. Open the exact tab you want to capture.
2. Make sure the page is within the allowed host list.
3. Open the extension popup.
4. Choose schema or leave it on `Auto detect`.
5. Keep destination on `Pending` for early rollout.
6. Add hints only when they help.
7. Click `Capture Tab`.

## Notes

- The extension does not crawl in the background.
- It does not monitor every site automatically.
- It captures only the active tab after a user action.
- If the server blocks `parts`, the result is downgraded to `pending`.

## Recommended use

- Use the extension for logged-in pages, filtered searches, supplier portals, and pages that depend on human navigation.
- Use the bookmarklet when you want a no-install fallback.
- Use the URL factory only for stable public routes that are safe to spider.
