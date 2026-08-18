// PWA-BATCH-1 A(iii): 共通画面枠（ヘッダ・ナビ・ログアウト）。
// DOM 構築のみ（HTML 文字列補間なし）・業務データを一切扱わない静的枠。
// ログアウトは form POST（/app/logout・cookie 削除→ログイン画面へ）。
// UI-POLISH-1: 質問画面と統一の配色・タップしやすい間隔へ（構造は不変）
(function () {
  const nav = [
    ["/app", "ホーム"],
    ["/app/souzoku", "相続案件"],
    ["/app/cases", "時効案件"],
    ["/app/q", "質問"],
    ["/app/approvals", "承認"],
    ["/app/kinship", "関係図"],
  ];
  const bar = document.createElement("header");
  bar.style.display = "flex";
  bar.style.flexWrap = "wrap";
  bar.style.alignItems = "center";
  bar.style.gap = "0.15rem";
  bar.style.padding = "0.4rem 0.5rem";
  bar.style.background = "#1a3c6e";
  bar.style.borderRadius = "12px";
  bar.style.marginBottom = "0.8rem";
  const here = location.pathname;
  for (const pair of nav) {
    const a = document.createElement("a");
    a.href = pair[0];
    a.textContent = pair[1];
    a.style.color = "#fff";
    a.style.textDecoration = "none";
    a.style.fontSize = "0.9rem";
    a.style.padding = "0.45rem 0.55rem";
    a.style.borderRadius = "8px";
    if (here === pair[0]) {
      a.style.background = "rgba(255,255,255,0.18)";
      a.style.fontWeight = "bold";
    }
    bar.appendChild(a);
  }
  const form = document.createElement("form");
  form.method = "post";
  form.action = "/app/logout";
  form.style.marginLeft = "auto";
  const btn = document.createElement("button");
  btn.type = "submit";
  btn.textContent = "ログアウト";
  btn.style.fontSize = "0.8rem";
  btn.style.padding = "0.4rem 0.7rem";
  btn.style.borderRadius = "8px";
  btn.style.border = "1px solid rgba(255,255,255,0.5)";
  btn.style.background = "transparent";
  btn.style.color = "#fff";
  form.appendChild(btn);
  bar.appendChild(form);
  document.body.insertBefore(bar, document.body.firstChild);
})();
