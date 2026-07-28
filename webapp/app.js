// P4-001 shell のみ。データ取得は P4-002 以降（PII を扱う fetch はここに書かない）
if ("serviceWorker" in navigator) {
  navigator.serviceWorker.register("/app/sw.js");
}
