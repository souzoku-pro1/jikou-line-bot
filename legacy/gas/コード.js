function onFileAdded() {
  const FOLDER_IDS = {
    '相談カード': '1ENVuANHT_U56i-iSaH2wxCILXWQdJDIr',
    '戸籍謄本': '1heGIYhNoVFnK3kB13liY3mNgiPWrFLXk',
    '通帳': '1ScAr7bXGIF6xY016qtJVa48wtoIP2tAy'
  };
  
  const RAILWAY_URL = 'https://jikou-line-bot-production.up.railway.app';
  
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
}