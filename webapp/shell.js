// PWA-BATCH-1 A(iii): 共通画面枠（ヘッダ・ナビ・ログアウト）。
// DOM 構築のみ（HTML 文字列補間なし）・業務データを一切扱わない静的枠。
// ログアウトは form POST（/app/logout・cookie 削除→ログイン画面へ）。
(function () {
  const nav = [
    ["/app", "ホーム"],
    ["/app/souzoku", "相続案件"],
    ["/app/cases", "案件一覧"],
    ["/app/approvals", "承認"],
    ["/app/kinship", "関係図"],
  ];
  const bar = document.createElement("header");
  bar.style.display = "flex";
  bar.style.flexWrap = "wrap";
  bar.style.alignItems = "center";
  bar.style.gap = "0.8rem";
  bar.style.padding = "0.55rem 0.9rem";
  bar.style.background = "#1a3c6e";
  bar.style.borderRadius = "8px";
  bar.style.marginBottom = "1rem";
  for (const pair of nav) {
    const a = document.createElement("a");
    a.href = pair[0];
    a.textContent = pair[1];
    a.style.color = "#fff";
    a.style.textDecoration = "none";
    a.style.fontSize = "0.95rem";
    bar.appendChild(a);
  }
  const form = document.createElement("form");
  form.method = "post";
  form.action = "/app/logout";
  form.style.marginLeft = "auto";
  const btn = document.createElement("button");
  btn.type = "submit";
  btn.textContent = "ログアウト";
  btn.style.fontSize = "0.85rem";
  form.appendChild(btn);
  bar.appendChild(form);
  document.body.insertBefore(bar, document.body.firstChild);
})();
