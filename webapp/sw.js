// P4-001: SW キャッシュは shell のみ（DRAFT §4）。
// データ応答（PII）は一切キャッシュしない — SHELL 以外は素通しの network fetch。
const CACHE = "webapp-shell-v1";
const SHELL = ["/app", "/app/app.js", "/app/manifest.json"];
self.addEventListener("install", (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL)));
});
self.addEventListener("activate", (e) => {
  e.waitUntil(caches.keys().then((keys) =>
    Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)))));
});
self.addEventListener("fetch", (e) => {
  const url = new URL(e.request.url);
  if (SHELL.includes(url.pathname)) {
    e.respondWith(caches.match(e.request).then((r) => r || fetch(e.request)));
  }
  // SHELL 以外はハンドリングしない＝ブラウザ既定の network fetch（キャッシュゼロ）
});
