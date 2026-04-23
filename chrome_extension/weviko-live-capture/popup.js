const DEFAULTS = {
  serverUrl: "http://127.0.0.1:8765",
  schemaKey: "auto",
  market: "GLOBAL",
  destination: "pending",
  oemBrand: "",
  partNumberHint: "",
  vehicleHint: "",
  systemHint: "",
  operatorIdentifier: "",
  operatorNote: "",
};

const form = document.querySelector("#capture-form");
const statusNode = document.querySelector("#status");
const activeTabNode = document.querySelector("#active-tab");
const healthButton = document.querySelector("#health-check");
const captureButton = document.querySelector("#capture-button");

function setStatus(message, kind = "meta") {
  statusNode.textContent = message;
  statusNode.className = `status ${kind}`;
}

async function loadSettings() {
  const stored = await chrome.storage.local.get(Object.keys(DEFAULTS));
  const settings = { ...DEFAULTS, ...stored };
  for (const [key, value] of Object.entries(settings)) {
    const field = form.elements.namedItem(key);
    if (!field) {
      continue;
    }
    field.value = value;
  }
}

async function saveSettings(settings) {
  await chrome.storage.local.set(settings);
}

function collectSettings() {
  return {
    serverUrl: String(form.elements.serverUrl.value || DEFAULTS.serverUrl).trim(),
    schemaKey: String(form.elements.schemaKey.value || DEFAULTS.schemaKey).trim(),
    market: String(form.elements.market.value || DEFAULTS.market).trim().toUpperCase(),
    destination: String(form.elements.destination.value || DEFAULTS.destination).trim().toLowerCase(),
    oemBrand: String(form.elements.oemBrand.value || "").trim(),
    partNumberHint: String(form.elements.partNumberHint.value || "").trim(),
    vehicleHint: String(form.elements.vehicleHint.value || "").trim(),
    systemHint: String(form.elements.systemHint.value || "").trim(),
    operatorIdentifier: String(form.elements.operatorIdentifier.value || "").trim(),
    operatorNote: String(form.elements.operatorNote.value || "").trim(),
  };
}

async function refreshActiveTabInfo() {
  try {
    const result = await chrome.runtime.sendMessage({ type: "get-active-tab-info" });
    if (!result?.ok) {
      throw new Error(result?.message || "Unable to inspect the active tab.");
    }
    const safeUrl = result.url || "(unknown)";
    activeTabNode.textContent = `Tab: ${safeUrl}`;
    if (form.elements.schemaKey.value === "auto" && result.inferredSchema) {
      setStatus(`Ready. Auto schema guess: ${result.inferredSchema}`, "meta");
    }
  } catch (error) {
    setStatus(error.message || String(error), "error");
  }
}

async function runHealthCheck() {
  const settings = collectSettings();
  await saveSettings(settings);
  setStatus("Checking local server...", "meta");
  try {
    const result = await chrome.runtime.sendMessage({
      type: "check-health",
      serverUrl: settings.serverUrl,
    });
    if (!result?.ok) {
      throw new Error(result?.message || "Health check failed.");
    }
    setStatus(
      `Server OK. Allowed hosts: ${(result.allowed_hosts || []).join(", ") || "-"}`,
      "ok",
    );
  } catch (error) {
    setStatus(error.message || String(error), "error");
  }
}

async function captureActiveTab(event) {
  event.preventDefault();
  const settings = collectSettings();
  await saveSettings(settings);
  setStatus("Capturing current tab and sending it to Weviko...", "meta");
  captureButton.disabled = true;
  healthButton.disabled = true;
  try {
    const result = await chrome.runtime.sendMessage({
      type: "capture-active-tab",
      settings,
    });
    if (!result?.ok) {
      throw new Error(result?.message || "Capture failed.");
    }
    const score = result.confidence_score ?? "-";
    const destination = result.destination || "-";
    const quality = result.quality_status || "-";
    const partNumber = result.part_number ? ` | Part: ${result.part_number}` : "";
    setStatus(
      `Saved to ${destination} | Score ${score} | Quality ${quality}${partNumber}\n${result.message || ""}`,
      "ok",
    );
  } catch (error) {
    setStatus(error.message || String(error), "error");
  } finally {
    captureButton.disabled = false;
    healthButton.disabled = false;
  }
}

form.addEventListener("submit", captureActiveTab);
healthButton.addEventListener("click", runHealthCheck);

await loadSettings();
await refreshActiveTabInfo();
