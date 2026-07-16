/**
 * RV-04c golden self-test（GAS 実機実行用・DRAFT §2）
 *
 * 実行者=大野（S4 or S2 検証時・K3-test プロジェクト流用可）。実行ログ（Logger.log）を
 * スクショ→PC-A が実出力を work-log(.md) へ保存。**本 self-test は本番 helper と同一の
 * builder（rv04c_signing.js の buildMultipart_ / canonicalV1_ / sha256Hex_ / hmacHex_）を
 * 呼ぶ**（H03: 別実装 builder での PASS は受入と認めない＝検証対象の同一性を構造で担保）。
 *
 * fixture（rv04c_gas_builder_vectors.v1.json / rv04_hmac_golden_vectors.v1.json）は
 * GAS には fetch できないため、S4 実行時に必要な vectors を GAS 定数として転記する
 * （テスト専用 secret のみ・本番 secret 不使用）。ここでは 1 本の代表 vector を例示する。
 * 全 vector 転記版は S4 手順書（司令塔発行）に添付する。
 */

// テスト専用 secret（golden の secret_hex_test_only・本番鍵ではない）
var RV04C_TEST_SECRET_HEX =
  '00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff';

// 代表 vector（ascii_filename_multipart 相当・parts + 期待値）
var RV04C_SAMPLE_VECTOR = {
  name: 'ascii_filename_multipart',
  boundary: 'BND-ASCII',
  parts: [{ name: 'file', filename: 'koseki.pdf', contentType: 'application/pdf',
            valueUtf8: '%PDF-1.4 ascii body\n%%EOF' }],
  // 期待値は fixture から転記（S4 手順書で全 vector 分を差し込む）
  expect_body_b64: null,     // ← S4 手順書で fixture の body_b64 を差し込む
  key_id: 'kid-golden', caller: 'gas-koseki', method: 'POST',
  normalized_path: '/koseki/ingest', timestamp: 1700000000,
  nonce: '0000000000000000000000000000aa00',
  expect_content_sha256: null,   // ← 差し込む
  expect_canonical_b64: null,    // ← 差し込む
  expect_signature: null         // ← 差し込む
};

function rv04c_goldenSelfTest() {
  var v = RV04C_SAMPLE_VECTOR;
  var parts = v.parts.map(function (p) {
    return { name: p.name, filename: p.filename, contentType: p.contentType,
             value: utf8Bytes_(p.valueUtf8) };
  });

  // 第0段（H03）: parts → 本番 builder → body byte 一致
  var body = buildMultipart_(v.boundary, parts);
  var bodyB64 = Utilities.base64Encode(body);
  var stage0 = (v.expect_body_b64 === null) ? 'SKIP(no-expect)' :
               (bodyB64 === v.expect_body_b64 ? 'PASS' : 'FAIL');

  // 第1段: content_sha256（R5: 同一 byte 配列）
  var csha = sha256Hex_(body);
  var stage1 = (v.expect_content_sha256 === null) ? 'SKIP' :
               (csha === v.expect_content_sha256 ? 'PASS' : 'FAIL');

  // 第2段: canonical_b64
  var canon = canonicalV1_(v.key_id, v.caller, v.method, v.normalized_path,
                           v.timestamp, v.nonce, csha);
  var canonB64 = Utilities.base64Encode(canon);
  var stage2 = (v.expect_canonical_b64 === null) ? 'SKIP' :
               (canonB64 === v.expect_canonical_b64 ? 'PASS' : 'FAIL');

  // 第3段: signature
  var sig = hmacHex_(canon, hexToBytes_(RV04C_TEST_SECRET_HEX));
  var stage3 = (v.expect_signature === null) ? 'SKIP' :
               (sig === v.expect_signature ? 'PASS' : 'FAIL');

  Logger.log('vector=' + v.name);
  Logger.log('  stage0(body byte一致)=' + stage0);
  Logger.log('  stage1(content_sha256)=' + stage1 + ' got=' + csha);
  Logger.log('  stage2(canonical_b64)=' + stage2);
  Logger.log('  stage3(signature)=' + stage3 + ' got=' + sig);
  Logger.log('=> 全 vector 版は S4 手順書の転記表で実行する');
  return { stage0: stage0, stage1: stage1, stage2: stage2, stage3: stage3 };
}

// H02: 大サイズ/chunk 境界の実機テスト（GAS 実機で chunked builder が壊れないこと）
function rv04c_builderLargeTest() {
  var big = [];
  for (var i = 0; i < 3000000; i++) big.push(((i * 131 + 7) % 256) - 128);   // ~3MB signed
  var parts = [{ name: 'file', filename: 'big.pdf', contentType: 'application/pdf', value: big },
               { name: 'drive_file_id', value: utf8Bytes_('F-big') }];
  var body = buildMultipart_('RV04Cbig', parts);
  var sha = sha256Hex_(body);
  Logger.log('large body len=' + body.length + ' sha256=' + sha);
  Logger.log('=> この sha256 を Python 側（test_rv04c_gas_builder の同一入力）と突合する');
  return sha;
}
