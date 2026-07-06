function onFileAdded() {
  const FOLDER_IDS = {
    '相談カード': '1ENVuANHT_U56i-iSaH2wxCILXWQdJDIr',
    '戸籍謄本': '1heGIYhNoVFnK3kB13liY3mNgiPWrFLXk',
    '通帳': '1ScAr7bXGIF6xY016qtJVa48wtoIP2tAy'
  };

  const RAILWAY_URL = 'https://jikou-line-bot-production.up.railway.app';

  // ── 戸籍読解ライン（2026-07-06追加・既存3フォルダとは独立） ──
  const KOSEKI_FOLDER_ID = '1Wzko2aNErlY5ouh8GI9_caG55LM8C1N1';
  const KOSEKI_TOKEN = '<Railway env KOSEKI_INGEST_TOKENと同値・実物はGASエディタ側>';

  // ── 既存の /scan 行き（一切変更なし） ──
  for (const [folderName, folderId] of Object.entries(FOLDER_IDS)) {
    const folder = DriveApp.getFolderById(folderId);
    const files = folder.getFiles();

    while (files.hasNext()) {
      const file = files.next();

      if (file.getName().startsWith('[済]')) continue;

      // ファイルの中身をbase64に変換
      const blob = file.getBlob();
      const base64Data = Utilities.base64Encode(blob.getBytes());

      // RailwayにPDFデータを直接送信
      UrlFetchApp.fetch(RAILWAY_URL + '/scan', {
        method: 'POST',
        contentType: 'application/json',
        payload: JSON.stringify({
          fileData: base64Data,
          fileName: file.getName(),
          folderName: folderName
        })
      });

      // 処理済みにリネーム
      file.setName('[済]' + file.getName());
    }
  }

  // ── 戸籍読解フォルダ → /koseki/ingest（独立ブロック・失敗しても上の既存処理に影響しない） ──
  const kosekiFolder = DriveApp.getFolderById(KOSEKI_FOLDER_ID);
  const kosekiFiles = kosekiFolder.getFiles();

  while (kosekiFiles.hasNext()) {
    const file = kosekiFiles.next();

    if (file.getName().startsWith('[済]')) continue;

    try {
      const res = UrlFetchApp.fetch(
        RAILWAY_URL + '/koseki/ingest?token=' + encodeURIComponent(KOSEKI_TOKEN), {
          method: 'POST',
          payload: {
            file: file.getBlob(),
            drive_file_id: file.getId()
          },
          muteHttpExceptions: true
        });

      if (res.getResponseCode() === 200) {
        file.setName('[済]' + file.getName());
      }
      // 200以外はリネームせず次回トリガーで自然リトライ
    } catch (e) {
      // このファイルは飛ばして次へ（既存3フォルダを道連れにしない）
    }
  }
}
