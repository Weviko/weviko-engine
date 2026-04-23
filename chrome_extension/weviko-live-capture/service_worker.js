const EXTENSION_VERSION = "1.0.0";
const DEFAULT_SERVER_URL = "http://127.0.0.1:8765";

function sanitizeServerUrl(value) {
  const raw = String(value || DEFAULT_SERVER_URL).trim();
  return raw.replace(/\/+$/, "") || DEFAULT_SERVER_URL;
}

function inferSchemaFromUrl(url) {
  const safeUrl = String(url || "").toLowerCase();
  if (safeUrl.includes("/item/detail/")) return "path_detail";
  if (safeUrl.includes("/shop/manual/")) return "path_manual";
  if (safeUrl.includes("/body/manual/")) return "path_body_manual";
  if (safeUrl.includes("/contents/etc/")) return "path_wiring";
  if (safeUrl.includes("/wiring/connector/")) return "path_connector";
  if (safeUrl.includes("/dtc/")) return "path_dtc";
  if (safeUrl.includes("/community/")) return "path_community";
  if (safeUrl.includes("/vehicle-id/") || safeUrl.includes("/vin/")) return "path_vehicle_id";
  return "path_manual";
}

function deriveSourcePathHint(url) {
  try {
    return new URL(url).pathname || "/";
  } catch {
    return "/";
  }
}

function collectPageSnapshot() {
  const selectionText = globalThis.getSelection
    ? String(globalThis.getSelection().toString() || "")
    : "";
  const visibleText = document.body
    ? String(document.body.innerText || document.body.textContent || "")
    : "";
  return {
    url: location.href,
    title: document.title || "",
    html: document.documentElement ? document.documentElement.outerHTML.slice(0, 350000) : "",
    text: visibleText.slice(0, 120000),
    selection_text: selectionText.slice(0, 12000),
    sent_at: new Date().toISOString(),
  };
}

async function getActiveTab() {
  const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
  const [tab] = tabs;
  if (!tab || !tab.id) {
    throw new Error("No active tab is available.");
  }
  return tab;
}

async function getActiveTabInfo() {
  const tab = await getActiveTab();
  const url = tab.url || "";
  if (!/^https?:/i.test(url)) {
    return {
      ok: false,
      message: "Only http/https tabs can be captured.",
    };
  }
  return {
    ok: true,
    url,
    title: tab.title || "",
    inferredSchema: inferSchemaFromUrl(url),
    sourcePathHint: deriveSourcePathHint(url),
  };
}

async function checkHealth(serverUrl) {
  const normalizedServerUrl = sanitizeServerUrl(serverUrl);
  const response = await fetch(`${normalizedServerUrl}/health`, {
    method: "GET",
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.message || `Health check failed (${response.status}).`);
  }
  return {
    ok: true,
    ...data,
  };
}

async function captureActiveTab(settings) {
  const tab = await getActiveTab();
  const url = tab.url || "";
  if (!/^https?:/i.test(url)) {
    throw new Error("Only http/https tabs can be captured.");
  }

  const execution = await chrome.scripting.executeScript({
    target: { tabId: tab.id },
    func: collectPageSnapshot,
  });
  const page = execution?.[0]?.result;
  if (!page) {
    throw new Error("The current tab could not be read.");
  }

  const normalizedServerUrl = sanitizeServerUrl(settings.serverUrl);
  const schemaKey = settings.schemaKey === "auto" ? inferSchemaFromUrl(page.url || url) : settings.schemaKey;
  const payload = {
    ...page,
    schema_key: schemaKey,
    source_path_hint: deriveSourcePathHint(page.url || url),
    document_type: "",
    market: String(settings.market || "GLOBAL").trim().toUpperCase() || "GLOBAL",
    destination: String(settings.destination || "pending").trim().toLowerCase() === "parts" ? "parts" : "pending",
    oem_brand: String(settings.oemBrand || "").trim(),
    part_number_hint: String(settings.partNumberHint || "").trim(),
    vehicle_hint: String(settings.vehicleHint || "").trim(),
    system_hint: String(settings.systemHint || "").trim(),
    operator_identifier: String(settings.operatorIdentifier || "").trim(),
    operator_note: String(settings.operatorNote || "").trim(),
    capture_channel: "chrome_extension",
    capture_client_version: `weviko-extension-${EXTENSION_VERSION}`,
  };

  const response = await fetch(`${normalizedServerUrl}/capture`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.message || `Capture failed (${response.status}).`);
  }
  return {
    ok: true,
    ...data,
  };
}

chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  (async () => {
    try {
      if (message?.type === "get-active-tab-info") {
        sendResponse(await getActiveTabInfo());
        return;
      }
      if (message?.type === "check-health") {
        sendResponse(await checkHealth(message.serverUrl));
        return;
      }
      if (message?.type === "capture-active-tab") {
        sendResponse(await captureActiveTab(message.settings || {}));
        return;
      }
      sendResponse({ ok: false, message: "Unknown message type." });
    } catch (error) {
      sendResponse({
        ok: false,
        message: error?.message || String(error),
      });
    }
  })();

  return true;
});
