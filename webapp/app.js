// P4-001 shell のみ。データ取得は P4-002 以降（PII を扱う fetch はここに書かない）
if ("serviceWorker" in navigator) {
  navigator.serviceWorker.register("/app/sw.js");
}

// MAINT-3 A（R-P4-004-2 L01 の発火条件充足）: PWA の fetch 閉集合ラッパー。
// - 生 fetch の呼出しは本ファイルのこの 1 箇所のみ（静的テストで pin）
// - path は同一 origin の "/app/api/" 配下（文字列・固定 prefix）のみ受理——
//   外部 origin・スキーム付き URL・非 API パスは throw（送信自体が起きない）
// - オプションは {redirect:"follow"} 固定（従来の全呼出しと同値＝挙動変更なし。
//   method 指定不可＝GET のみ・credentials は同一 origin 既定のまま）
const APP_API_PREFIX = "/app/api/";
async function app_fetch(path) {
  if (typeof path !== "string" || !path.startsWith(APP_API_PREFIX)) {
    throw new Error("app_fetch: /app/api/ 配下のパスのみ使用できます");
  }
  return fetch(path, {redirect: "follow"});
}
