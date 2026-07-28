// P4-001 fix1 H01（[人]裁定・SW キャッシュ全廃）: network-only。
// 設計判断: 認証済み応答が Cache Storage に残ると cookie 失効・署名鍵差し替え後も
// ブラウザ側で表示できてしまうため、SW によるキャッシュ経路を構造的に排除する
// （オフライン非対応=認証境界優先）。fetch handler は登録しない＝全 request が
// ブラウザ既定の network fetch。install/activate は SW 登録の維持のみ。
self.addEventListener("install", () => { self.skipWaiting(); });
self.addEventListener("activate", (e) => { e.waitUntil(self.clients.claim()); });
