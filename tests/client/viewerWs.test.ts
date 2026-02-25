import assert from "node:assert/strict";
import { test } from "node:test";
import { buildViewerWsUrl } from "../../client/src/lib/features/viewer/viewerWs.js";

test("buildViewerWsUrl uses ws with active page host for http", () => {
  assert.strictEqual(
    buildViewerWsUrl({ protocol: "http:", host: "192.168.1.41:3000" }),
    "ws://192.168.1.41:3000/ws",
  );
});

test("buildViewerWsUrl uses wss with active page host for https", () => {
  assert.strictEqual(
    buildViewerWsUrl({ protocol: "https:", host: "chart.example.com" }),
    "wss://chart.example.com/ws",
  );
});

test("buildViewerWsUrl falls back to localhost in non-browser contexts", () => {
  assert.strictEqual(buildViewerWsUrl(null), "ws://localhost:3000/ws");
});
