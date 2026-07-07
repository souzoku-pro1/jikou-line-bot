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
// ── 書類仕分けライン(2026-07-06追加・既存の処理とは独立) ──
  const MISEIRI_FOLDER_ID = '1yPky1KQ5n8bZL8TEcLUeEv05bRnGuRy-';
  const KOKYAKU_SHORUI_FOLDER_ID = '1iERvSmUJAfB6ri4uYOds7-tD3aXWxKkc';
  const SORTATION_TOKEN = '<Railway env SORTATION_INGEST_TOKENと同値・実物はGASエディタ側>';

  const miseiriFolder = DriveApp.getFolderById(MISEIRI_FOLDER_ID);
  const kokyakuParent = DriveApp.getFolderById(KOKYAKU_SHORUI_FOLDER_ID);
  const miseiriFiles = miseiriFolder.getFiles();

  while (miseiriFiles.hasNext()) {
    const file = miseiriFiles.next();
    const name = file.getName();
    if (name.startsWith('[済]') || name.startsWith('[照会中]')) continue;

    try {
      const res = UrlFetchApp.fetch(
        RAILWAY_URL + '/sortation/ingest?token=' + encodeURIComponent(SORTATION_TOKEN), {
          method: 'POST',
          payload: {
            file: file.getBlob(),
            drive_file_id: file.getId(),
            drive_file_url: file.getUrl()
          },
          muteHttpExceptions: true
        });

      if (res.getResponseCode() !== 200) continue;

      const result = JSON.parse(res.getContentText());

      if (result.action === 'auto' && result.customer && result.customer.folder_name) {
        const folders = kokyakuParent.getFoldersByName(result.customer.folder_name);
        const target = folders.hasNext() ? folders.next()
                                         : kokyakuParent.createFolder(result.customer.folder_name);
        if (result.suggested_filename) file.setName(result.suggested_filename);
        file.moveTo(target);
      } else {
        file.setName('[照会中]' + name);
      }
    } catch (e) {
      // このファイルは飛ばして次へ(既存処理を道連れにしない)
    }
  }
// ── 仕分け実行ライン(2026-07-06追加・第2段③: 状態=確定のログを拾ってDrive移動) ──
  const SORTATION_LOG_APP_ID = '38';
  const SORTATION_LOG_TOKEN = '<kintone App 38 APIトークン・実物はGASエディタ側>';
  const KINTONE_BASE = 'https://edmjisxyx9uc.cybozu.com';

  try {
    const q = encodeURIComponent('状態 in ("確定") limit 20');
    const listRes = UrlFetchApp.fetch(
      KINTONE_BASE + '/k/v1/records.json?app=' + SORTATION_LOG_APP_ID + '&query=' + q, {
        headers: { 'X-Cybozu-API-Token': SORTATION_LOG_TOKEN },
        muteHttpExceptions: true
      });
    if (listRes.getResponseCode() === 200) {
      const records = JSON.parse(listRes.getContentText()).records || [];

      for (const rec of records) {
        try {
          const fileId = rec['Drive_fileId'].value;
          const folderName = rec['仕分け先フォルダ名'].value;
          const custName = rec['仕分け先氏名'].value;
          const docType = rec['書類種類'].value;
          if (!fileId || !folderName) continue;

          const file = DriveApp.getFileById(fileId);

          const folders = kokyakuParent.getFoldersByName(folderName);
          const target = folders.hasNext() ? folders.next()
                                           : kokyakuParent.createFolder(folderName);

          const d = new Date();
          const ymd = d.getFullYear() +
                      ('0' + (d.getMonth() + 1)).slice(-2) +
                      ('0' + d.getDate()).slice(-2);
          file.setName(custName + '_' + docType + '_' + ymd + '.pdf');
          file.moveTo(target);

          UrlFetchApp.fetch(KINTONE_BASE + '/k/v1/record.json', {
            method: 'PUT',
            contentType: 'application/json',
            headers: { 'X-Cybozu-API-Token': SORTATION_LOG_TOKEN },
            payload: JSON.stringify({
              app: SORTATION_LOG_APP_ID,
              id: rec['$id'].value,
              record: {
                '状態': { value: '実行済み' },
                '実行日時': { value: new Date().toISOString() }
              }
            }),
            muteHttpExceptions: true
          });
        } catch (e) {
          // このレコードは飛ばして次へ(ファイル不存在等。状態は確定のまま残り人が確認できる)
        }
      }
    }
  } catch (e) {
    // 仕分け実行ラインの失敗は既存処理を道連れにしない
  }
 processRegistryFolder_();
}
// ── 登記読解ライン(2026-07-07追加・既存の処理とは独立) ──
function processRegistryFolder_() {
  const RAILWAY_URL = 'https://jikou-line-bot-production.up.railway.app';
  const REGISTRY_FOLDER_ID = '16LlliLFys2t1haKENAHTatEcu-5cSXF4';
  const REGISTRY_TOKEN = '<Railway env REGISTRY_INGEST_TOKENと同値・実物はGASエディタ側>';

  const registryFolder = DriveApp.getFolderById(REGISTRY_FOLDER_ID);
  const registryFiles = registryFolder.getFiles();

  while (registryFiles.hasNext()) {
    const file = registryFiles.next();
    const name = file.getName();
    if (name.startsWith('[済]')) continue;
    if (file.getMimeType() !== 'application/pdf') continue;

    try {
      const res = UrlFetchApp.fetch(
        RAILWAY_URL + '/registry/ingest?token=' + encodeURIComponent(REGISTRY_TOKEN), {
          method: 'POST',
          payload: {
            file: file.getBlob(),
            drive_file_id: file.getId()
          },
          muteHttpExceptions: true
        });

      if (res.getResponseCode() === 200) {
        file.setName('[済]' + name);
      }
      // 200以外はリネームせず次回トリガーで自然リトライ
    } catch (e) {
      // このファイルは飛ばして次へ(他の処理を道連れにしない)
    }
  }
}