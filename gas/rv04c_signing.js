/**
 * RV-04c: GAS 署名ヘルパ（NM01 v1 HMAC・DRAFT_RV04C_CALLER_MIGRATION.md §1 準拠）
 *
 * 正本の扱い（重要）:
 *   - legacy/gas/ は「正本は GAS 側」の読み取りコピーだが、**この署名ヘルパは S4 で
 *     GAS 本番へ反映するまで repo（このファイル）が正本**（票規約）。S4 反映後は
 *     legacy/gas/README.md と同じ「正本は GAS 側」規約へ移行する。
 *   - 本番反映（clasp push / エディタ手動転記）と Script Properties への secret 投入は
 *     大野の工程（S4）。このコードに secret を **平文で埋め込まない**（§3・M03）。
 *
 * byte 規約（§1.1 R1〜R5）:
 *   R1 multipart 手組み（payload=byte[] 直渡し・自動組立禁止）
 *   R2 文字列→byte は UTF-8（Utilities.newBlob(str).getBytes()）
 *   R3 signed byte を (b+256)%256 で 0..255 化してから hex
 *   R4 固定サイズ chunk append（push.apply 禁止）
 *   R5 content_sha256 は payload に渡す同一 byte 配列を digest
 *
 * Python 参照実装（test_rv04c_gas_builder.py）と byte 等価。golden self-test で固定。
 */

// ── lane 別 field 名 allowlist（§1.1b・サーバ *_ingest.py の Form 定義と 1:1） ──
var LANE_FIELDS = {
  '/koseki/ingest':     ['file', 'case_hint', 'case_app_hint', 'drive_file_id'],
  '/registry/ingest':   ['file', 'case_hint', 'drive_file_id'],
  '/bank/ingest':       ['file', 'case_hint', 'case_app_hint', 'drive_file_id'],
  '/sortation/ingest':  ['file', 'drive_file_id', 'drive_file_url'],
  '/valuation/ingest':  ['file', 'case_hint', 'case_app_hint', 'drive_file_id']
};

// ── lane 別署名切替（false=旧 query token 経路のまま・rollback は 1 箇所） ──
var SIGNED_LANES = {
  '/koseki/ingest': false,
  '/registry/ingest': false,
  '/bank/ingest': false,
  '/sortation/ingest': false,
  '/valuation/ingest': false
};

var RAILWAY_URL = 'https://jikou-line-bot-production.up.railway.app';
var CHUNK = 8192;

// ── R2: 文字列→UTF-8 bytes ──
function utf8Bytes_(str) {
  return Utilities.newBlob(str).getBytes();   // GAS Blob の既定 charset=UTF-8
}

// ── R3: signed byte 配列 → hex（0..255 正規化） ──
function toHex_(bytes) {
  var s = '';
  for (var i = 0; i < bytes.length; i++) {
    var b = (bytes[i] + 256) % 256;
    s += (b < 16 ? '0' : '') + b.toString(16);
  }
  return s;
}

// hex 文字列 → byte 配列（secret 復元用） ──
function hexToBytes_(hex) {
  var out = [];
  for (var i = 0; i < hex.length; i += 2) {
    var v = parseInt(hex.substr(i, 2), 16);
    out.push(v > 127 ? v - 256 : v);   // GAS は signed byte
  }
  return out;
}

// ── R4: 固定サイズ chunk append（push.apply 禁止） ──
function appendBytes_(dst, src) {
  for (var i = 0; i < src.length; i += CHUNK) {
    var end = Math.min(i + CHUNK, src.length);
    for (var j = i; j < end; j++) dst.push(src[j]);
  }
  return dst;
}

// ── §1.1b: filename 規則（CR/LF/NUL/" 拒否＋非 ASCII は ASCII fallback） ──
function sanitizeFilename_(rawName, driveFileId) {
  for (var i = 0; i < rawName.length; i++) {
    var c = rawName.charCodeAt(i);
    if (c === 13 || c === 10 || c === 0 || c === 34) {   // CR LF NUL "
      throw new Error('filename forbidden char');
    }
  }
  var ascii = true;
  for (var k = 0; k < rawName.length; k++) {
    if (rawName.charCodeAt(k) > 127) { ascii = false; break; }
  }
  if (ascii) return rawName;
  var ext = 'bin';
  var dot = rawName.lastIndexOf('.');
  if (dot !== -1) {
    var cand = rawName.substring(dot + 1);
    if (cand.length > 0 && /^[0-9A-Za-z]+$/.test(cand)) ext = cand;
  }
  return 'doc-' + driveFileId + '.' + ext;
}

// ── R1/R4: multipart 手組み builder（parts: {name, filename?, contentType?, value(bytes)}） ──
// filename は sanitize 済みを前提（helper 層で sanitize）。builder は禁止文字を防御 assert。
function buildMultipart_(boundary, parts) {
  var out = [];
  var bnd = utf8Bytes_('--' + boundary);
  var crlf = utf8Bytes_('\r\n');
  for (var i = 0; i < parts.length; i++) {
    var p = parts[i];
    appendBytes_(out, bnd);
    appendBytes_(out, crlf);
    var disp = 'form-data; name="' + p.name + '"';
    if (p.filename !== undefined && p.filename !== null) {
      if (/[\r\n\x00"]/.test(p.filename)) throw new Error('filename forbidden char in builder');
      disp += '; filename="' + p.filename + '"';
    }
    appendBytes_(out, utf8Bytes_('Content-Disposition: ' + disp));
    appendBytes_(out, crlf);
    if (p.contentType) {
      appendBytes_(out, utf8Bytes_('Content-Type: ' + p.contentType));
      appendBytes_(out, crlf);
    }
    appendBytes_(out, crlf);
    appendBytes_(out, p.value);
    appendBytes_(out, crlf);
  }
  appendBytes_(out, utf8Bytes_('--' + boundary + '--'));
  appendBytes_(out, crlf);
  return out;   // byte 配列（payload に直渡し）
}

// ── §2.1: canonical（length-prefix・UTF-8 バイト長） ──
function canonicalV1_(keyId, caller, method, npath, ts, nonce, csha) {
  var order = ['v1', keyId, caller, method.toUpperCase(), npath, String(ts), nonce, csha];
  var out = [];
  for (var i = 0; i < order.length; i++) {
    var u = utf8Bytes_(order[i]);
    appendBytes_(out, utf8Bytes_(String(u.length) + ':'));
    appendBytes_(out, u);
    appendBytes_(out, utf8Bytes_('\n'));
  }
  return out;
}

// ── content_sha256（R5: payload と同一 byte 配列を digest） ──
function sha256Hex_(bytes) {
  return toHex_(Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, bytes));
}

// ── 署名（byte[] 版のみ・R3 で hex 化） ──
function hmacHex_(canonicalBytes, keyBytes) {
  return toHex_(Utilities.computeHmacSha256Signature(canonicalBytes, keyBytes));
}

// ── 公開: 署名付き fetch（watcher の UrlFetchApp.fetch 呼出しをこの 1 行に置換） ──
// path 例 '/koseki/ingest'。parts は {name, value(bytes), filename?, contentType?} の配列。
// filename は呼び出し側で原名を渡す（内部で sanitize する）。
function rv04cSignedFetch_(path, parts) {
  // field 名 allowlist enforce（§1.1b）
  var allowed = LANE_FIELDS[path];
  if (!allowed) throw new Error('unknown lane: ' + path);
  for (var i = 0; i < parts.length; i++) {
    if (allowed.indexOf(parts[i].name) === -1) {
      throw new Error('field not allowed for ' + path + ': ' + parts[i].name);
    }
    if (parts[i].filename) {
      parts[i].filename = sanitizeFilename_(parts[i].filename, _driveIdOf_(parts));
    }
  }
  var props = PropertiesService.getScriptProperties();
  var keyId = props.getProperty('RV04C_KEY_ID');
  var secretHex = props.getProperty('RV04C_SECRET_HEX');
  if (!keyId || !secretHex) throw new Error('RV04C key not configured in Script Properties');

  var boundary = 'RV04C' + Utilities.getUuid().replace(/-/g, '');
  var body = buildMultipart_(boundary, parts);
  var csha = sha256Hex_(body);
  var ts = Math.floor(Date.now() / 1000);
  var nonce = Utilities.getUuid().replace(/-/g, '');
  var canon = canonicalV1_(keyId, 'gas-ingest', 'POST', path, ts, nonce, csha);
  var sig = hmacHex_(canon, hexToBytes_(secretHex));

  return UrlFetchApp.fetch(RAILWAY_URL + path, {
    method: 'post',
    contentType: 'multipart/form-data; boundary=' + boundary,
    payload: body,                      // byte[] 直渡し（無変換送出）
    headers: {
      'X-Sig-Version': 'v1',
      'X-Sig-Key-Id': keyId,
      'X-Sig-Caller': 'gas-ingest',
      'X-Sig-Timestamp': String(ts),
      'X-Sig-Nonce': nonce,
      'X-Sig-Content-SHA256': csha,
      'X-Sig-Signature': sig
    },
    muteHttpExceptions: true
  });
}

function _driveIdOf_(parts) {
  for (var i = 0; i < parts.length; i++) {
    if (parts[i].name === 'drive_file_id') return Utilities.newBlob(parts[i].value).getDataAsString();
  }
  return 'unknown';
}
