/**
 * RV-04c golden self-test（GAS 実機実行用・DRAFT §2・S2-fix1 H03/M02・S2-fix2 H03残）
 *
 * 実行者=大野（S4）。**本番 builder/前処理を共用**（別実装 PASS を認めない）。
 * H03: 期待値は全 vector を本ファイルに固定（S4 手転記廃止）・secret は正本 fixture と同一。
 * 期待値欠落=FAIL（SKIP 禁止）。production pipeline は vector.pipeline（match/reject/skip）で
 * 期待挙動を明示し、**builder_na（skip）以外の SKIP 変換を根絶**する。
 */

// 正本 fixture と同一のテスト専用 secret（本番鍵ではない）
var RV04C_TEST_SECRET_HEX =
  '1dfa2f9f6becae8c8eaed48f15d1c79da850996e92c0ae55a871bf066e5f2ce5';

var RV04C_VECTORS = [
  {name:"ascii_filename_multipart", pipeline:"match", boundary:"BND-ASCII", parts:[{name:"file",filename:"koseki.pdf",contentType:"application/pdf",valueB64:"JVBERi0xLjQgYXNjaWkgYm9keQolJUVPRg=="}], body_b64:"LS1CTkQtQVNDSUkNCkNvbnRlbnQtRGlzcG9zaXRpb246IGZvcm0tZGF0YTsgbmFtZT0iZmlsZSI7IGZpbGVuYW1lPSJrb3Nla2kucGRmIg0KQ29udGVudC1UeXBlOiBhcHBsaWNhdGlvbi9wZGYNCg0KJVBERi0xLjQgYXNjaWkgYm9keQolJUVPRg0KLS1CTkQtQVNDSUktLQ0K", content_sha256:"68ba13471fc5adf43c10cfb753ddca9f8e16d9c1410ebd75e22f5ca75b3c5dd9", key_id:"kid-golden", caller:"gas-koseki", method:"POST", normalized_path:"/koseki/ingest", nonce:"0000000000000000000000000000aa01", canonical_b64:"Mjp2MQoxMDpraWQtZ29sZGVuCjEwOmdhcy1rb3Nla2kKNDpQT1NUCjE0Oi9rb3Nla2kvaW5nZXN0CjEwOjE3MDAwMDAwMDAKMzI6MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMGFhMDEKNjQ6NjhiYTEzNDcxZmM1YWRmNDNjMTBjZmI3NTNkZGNhOWY4ZTE2ZDljMTQxMGViZDc1ZTIyZjVjYTc1YjNjNWRkOQo=", signature:"9872efac46b4824383a9cbf83fecb15547af9687b495363a9463c46af1e97a36", timestamp:1700000000},
  {name:"japanese_filename_multipart", pipeline:"reject", boundary:"BND-JP", parts:[{name:"file",filename:"戸籍謄本.pdf",contentType:"application/pdf",valueB64:"JVBERi0xLjQg5oi457GNIGJvZHkKJSVFT0Y="}], body_b64:"LS1CTkQtSlANCkNvbnRlbnQtRGlzcG9zaXRpb246IGZvcm0tZGF0YTsgbmFtZT0iZmlsZSI7IGZpbGVuYW1lPSLmiLjnsY3orITmnKwucGRmIg0KQ29udGVudC1UeXBlOiBhcHBsaWNhdGlvbi9wZGYNCg0KJVBERi0xLjQg5oi457GNIGJvZHkKJSVFT0YNCi0tQk5ELUpQLS0NCg==", content_sha256:"0585ce11133e1c62a4308780b58e7207d628bd78c58fa46708e8d11861f379db", key_id:"kid-golden", caller:"gas-koseki", method:"POST", normalized_path:"/koseki/ingest", nonce:"0000000000000000000000000000aa02", canonical_b64:"Mjp2MQoxMDpraWQtZ29sZGVuCjEwOmdhcy1rb3Nla2kKNDpQT1NUCjE0Oi9rb3Nla2kvaW5nZXN0CjEwOjE3MDAwMDAwMDAKMzI6MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMGFhMDIKNjQ6MDU4NWNlMTExMzNlMWM2MmE0MzA4NzgwYjU4ZTcyMDdkNjI4YmQ3OGM1OGZhNDY3MDhlOGQxMTg2MWYzNzlkYgo=", signature:"faee21361acc5ad1c94e24be6159b0c94693dd7550b07608352dd74bfce4ea9b", timestamp:1700000000},
  {name:"empty_body", builder_na:true, pipeline:"skip", parts:[], body_b64:"", content_sha256:"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", key_id:"kid-golden", caller:"gas-koseki", method:"POST", normalized_path:"/koseki/ingest", nonce:"0000000000000000000000000000aa03", canonical_b64:"Mjp2MQoxMDpraWQtZ29sZGVuCjEwOmdhcy1rb3Nla2kKNDpQT1NUCjE0Oi9rb3Nla2kvaW5nZXN0CjEwOjE3MDAwMDAwMDAKMzI6MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMGFhMDMKNjQ6ZTNiMGM0NDI5OGZjMWMxNDlhZmJmNGM4OTk2ZmI5MjQyN2FlNDFlNDY0OWI5MzRjYTQ5NTk5MWI3ODUyYjg1NQo=", signature:"ad167bdceb3a9c61a19e65b333137850bbeb657d73725d10c2ef35ac5465e531", timestamp:1700000000},
  {name:"long_boundary", pipeline:"match", boundary:"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", parts:[{name:"file",filename:"x.pdf",contentType:"application/pdf",valueB64:"YmluYXJ5AAH/IGRhdGE="}], body_b64:"LS1BQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUENCkNvbnRlbnQtRGlzcG9zaXRpb246IGZvcm0tZGF0YTsgbmFtZT0iZmlsZSI7IGZpbGVuYW1lPSJ4LnBkZiINCkNvbnRlbnQtVHlwZTogYXBwbGljYXRpb24vcGRmDQoNCmJpbmFyeQAB/yBkYXRhDQotLUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQS0tDQo=", content_sha256:"42f2135195a13a1c1a71148d740ac7fa6754ff7e237e848bf6327ca7e290571d", key_id:"kid-golden", caller:"gas-koseki", method:"POST", normalized_path:"/koseki/ingest", nonce:"0000000000000000000000000000aa04", canonical_b64:"Mjp2MQoxMDpraWQtZ29sZGVuCjEwOmdhcy1rb3Nla2kKNDpQT1NUCjE0Oi9rb3Nla2kvaW5nZXN0CjEwOjE3MDAwMDAwMDAKMzI6MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMGFhMDQKNjQ6NDJmMjEzNTE5NWExM2ExYzFhNzExNDhkNzQwYWM3ZmE2NzU0ZmY3ZTIzN2U4NDhiZjYzMjdjYTdlMjkwNTcxZAo=", signature:"fff9132dc51e1fc1c506834df17f2757f27d8b437eced728ee2f5a71f1dd1a71", timestamp:1700000000},
  {name:"multi_field", pipeline:"reject", boundary:"BND-MULTI", parts:[{name:"meta",filename:null,contentType:"application/json",valueB64:"eyJraW5kIjoia29zZWtpIn0="},{name:"file",filename:"謄本.pdf",contentType:"application/pdf",valueB64:"JVBERiBtdWx0aSBib2R5CiUlRU9G"}], body_b64:"LS1CTkQtTVVMVEkNCkNvbnRlbnQtRGlzcG9zaXRpb246IGZvcm0tZGF0YTsgbmFtZT0ibWV0YSINCkNvbnRlbnQtVHlwZTogYXBwbGljYXRpb24vanNvbg0KDQp7ImtpbmQiOiJrb3Nla2kifQ0KLS1CTkQtTVVMVEkNCkNvbnRlbnQtRGlzcG9zaXRpb246IGZvcm0tZGF0YTsgbmFtZT0iZmlsZSI7IGZpbGVuYW1lPSLorITmnKwucGRmIg0KQ29udGVudC1UeXBlOiBhcHBsaWNhdGlvbi9wZGYNCg0KJVBERiBtdWx0aSBib2R5CiUlRU9GDQotLUJORC1NVUxUSS0tDQo=", content_sha256:"db7fbb9a0344396f88c3064c069f68d143d277e61814d3f81741365fb6a57da1", key_id:"kid-golden", caller:"gas-koseki", method:"POST", normalized_path:"/koseki/ingest", nonce:"0000000000000000000000000000aa05", canonical_b64:"Mjp2MQoxMDpraWQtZ29sZGVuCjEwOmdhcy1rb3Nla2kKNDpQT1NUCjE0Oi9rb3Nla2kvaW5nZXN0CjEwOjE3MDAwMDAwMDAKMzI6MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMGFhMDUKNjQ6ZGI3ZmJiOWEwMzQ0Mzk2Zjg4YzMwNjRjMDY5ZjY4ZDE0M2QyNzdlNjE4MTRkM2Y4MTc0MTM2NWZiNmE1N2RhMQo=", signature:"d0a4d626716c7cc6ce44d97de2aafde2a91e27c857bdfd22c27ae3d328129de9", timestamp:1700000000},
  {name:"delimiter_lookalike_in_content", pipeline:"match", boundary:"RV04C0011deadbeefcafe", parts:[{name:"file",filename:"x.pdf",contentType:"application/pdf",valueB64:"JVBERg0KLS1SVjA0QzAwMTEgbm90LXRoZS1kZWxpbWl0ZXINCnRhaWw="},{name:"drive_file_id",filename:null,contentType:null,valueB64:"Ri1kZWw="}], body_b64:"LS1SVjA0QzAwMTFkZWFkYmVlZmNhZmUNCkNvbnRlbnQtRGlzcG9zaXRpb246IGZvcm0tZGF0YTsgbmFtZT0iZmlsZSI7IGZpbGVuYW1lPSJ4LnBkZiINCkNvbnRlbnQtVHlwZTogYXBwbGljYXRpb24vcGRmDQoNCiVQREYNCi0tUlYwNEMwMDExIG5vdC10aGUtZGVsaW1pdGVyDQp0YWlsDQotLVJWMDRDMDAxMWRlYWRiZWVmY2FmZQ0KQ29udGVudC1EaXNwb3NpdGlvbjogZm9ybS1kYXRhOyBuYW1lPSJkcml2ZV9maWxlX2lkIg0KDQpGLWRlbA0KLS1SVjA0QzAwMTFkZWFkYmVlZmNhZmUtLQ0K", content_sha256:"f49d80990b167197f87307073ea7f7b6cd965a83d7e1ed5ed7a370225678630a", key_id:"kid-golden", caller:"gas-koseki", method:"POST", normalized_path:"/koseki/ingest", nonce:"0000000000000000000000000000dea1", canonical_b64:"Mjp2MQoxMDpraWQtZ29sZGVuCjEwOmdhcy1rb3Nla2kKNDpQT1NUCjE0Oi9rb3Nla2kvaW5nZXN0CjEwOjE3MDAwMDAwMDAKMzI6MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMGRlYTEKNjQ6ZjQ5ZDgwOTkwYjE2NzE5N2Y4NzMwNzA3M2VhN2Y3YjZjZDk2NWE4M2Q3ZTFlZDVlZDdhMzcwMjI1Njc4NjMwYQo=", signature:"b03545d6d726c43f6ed891ff3f94003d9c8c10c7ef4275ca190c11f2d775a5c3", timestamp:1700000000},
  {name:"ascii_fallback_filename", pipeline:"match", boundary:"RV04C0022", parts:[{name:"file",filename:"doc-F-fb.pdf",contentType:"application/pdf",valueB64:"JVBERi0xLjQgZmIKJSVFT0Y="},{name:"drive_file_id",filename:null,contentType:null,valueB64:"Ri1mYg=="}], body_b64:"LS1SVjA0QzAwMjINCkNvbnRlbnQtRGlzcG9zaXRpb246IGZvcm0tZGF0YTsgbmFtZT0iZmlsZSI7IGZpbGVuYW1lPSJkb2MtRi1mYi5wZGYiDQpDb250ZW50LVR5cGU6IGFwcGxpY2F0aW9uL3BkZg0KDQolUERGLTEuNCBmYgolJUVPRg0KLS1SVjA0QzAwMjINCkNvbnRlbnQtRGlzcG9zaXRpb246IGZvcm0tZGF0YTsgbmFtZT0iZHJpdmVfZmlsZV9pZCINCg0KRi1mYg0KLS1SVjA0QzAwMjItLQ0K", content_sha256:"720d423d5015cc880a2c3a68606929a8b4ec5f111dbbb7ba12961b53a49b95c8", key_id:"kid-golden", caller:"gas-koseki", method:"POST", normalized_path:"/koseki/ingest", nonce:"0000000000000000000000000000fb01", canonical_b64:"Mjp2MQoxMDpraWQtZ29sZGVuCjEwOmdhcy1rb3Nla2kKNDpQT1NUCjE0Oi9rb3Nla2kvaW5nZXN0CjEwOjE3MDAwMDAwMDAKMzI6MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMGZiMDEKNjQ6NzIwZDQyM2Q1MDE1Y2M4ODBhMmMzYTY4NjA2OTI5YThiNGVjNWYxMTFkYmJiN2JhMTI5NjFiNTNhNDliOTVjOAo=", signature:"edd9321eaad38f71e4466b495ae738d55af06f2d97c3682338ccf3506153e98f", timestamp:1700000000, fallback_check:{raw_filename:"住民票の写し.pdf",drive_file_id:"F-fb",expected_filename:"doc-F-fb.pdf"}},
  {name:"empty_text_field", pipeline:"match", boundary:"RV04C0033", parts:[{name:"file",filename:"e.pdf",contentType:"application/pdf",valueB64:"JVBERi0xLjQgZQolJUVPRg=="},{name:"case_hint",filename:null,contentType:null,valueB64:""},{name:"drive_file_id",filename:null,contentType:null,valueB64:"Ri1l"}], body_b64:"LS1SVjA0QzAwMzMNCkNvbnRlbnQtRGlzcG9zaXRpb246IGZvcm0tZGF0YTsgbmFtZT0iZmlsZSI7IGZpbGVuYW1lPSJlLnBkZiINCkNvbnRlbnQtVHlwZTogYXBwbGljYXRpb24vcGRmDQoNCiVQREYtMS40IGUKJSVFT0YNCi0tUlYwNEMwMDMzDQpDb250ZW50LURpc3Bvc2l0aW9uOiBmb3JtLWRhdGE7IG5hbWU9ImNhc2VfaGludCINCg0KDQotLVJWMDRDMDAzMw0KQ29udGVudC1EaXNwb3NpdGlvbjogZm9ybS1kYXRhOyBuYW1lPSJkcml2ZV9maWxlX2lkIg0KDQpGLWUNCi0tUlYwNEMwMDMzLS0NCg==", content_sha256:"15cba38affef80e4900d0e22f95404dc091b58c56cbd48329630eece8a78e2b8", key_id:"kid-golden", caller:"gas-koseki", method:"POST", normalized_path:"/koseki/ingest", nonce:"0000000000000000000000000000e001", canonical_b64:"Mjp2MQoxMDpraWQtZ29sZGVuCjEwOmdhcy1rb3Nla2kKNDpQT1NUCjE0Oi9rb3Nla2kvaW5nZXN0CjEwOjE3MDAwMDAwMDAKMzI6MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMGUwMDEKNjQ6MTVjYmEzOGFmZmVmODBlNDkwMGQwZTIyZjk1NDA0ZGMwOTFiNThjNTZjYmQ0ODMyOTYzMGVlY2U4YTc4ZTJiOAo=", signature:"0073773c30304f6fd06f758629170d6a0236bac7c9e928198df172b6273461a1", timestamp:1700000000},
];

function _partsFromVec_(v) {
  var out = [];
  for (var i = 0; i < v.parts.length; i++) {
    var p = v.parts[i];
    out.push({ name: p.name,
               filename: (p.filename === null ? undefined : p.filename),
               contentType: (p.contentType === null ? undefined : p.contentType),
               value: Utilities.base64Decode(p.valueB64) });
  }
  return out;
}

function rv04c_goldenSelfTest() {
  var keyBytes = hexToBytes_(RV04C_TEST_SECRET_HEX);
  var all = 'PASS';
  for (var i = 0; i < RV04C_VECTORS.length; i++) {
    var v = RV04C_VECTORS[i];
    var s0, s1;
    if (v.builder_na) {
      s0 = 'NA(enum:builder_na)';
      s1 = (sha256Hex_(Utilities.base64Decode(v.body_b64)) === v.content_sha256) ? 'PASS' : 'FAIL';
    } else {
      var body = buildMultipart_(v.boundary, _partsFromVec_(v));
      s0 = (Utilities.base64Encode(body) === v.body_b64) ? 'PASS' : 'FAIL';
      s1 = (sha256Hex_(body) === v.content_sha256) ? 'PASS' : 'FAIL';
    }
    var canon = canonicalV1_(v.key_id, v.caller, v.method, v.normalized_path,
                             v.timestamp, v.nonce, v.content_sha256);
    var s2 = (Utilities.base64Encode(canon) === v.canonical_b64) ? 'PASS' : 'FAIL';
    var s3 = (hmacHex_(canon, keyBytes) === v.signature) ? 'PASS' : 'FAIL';
    if (s0 === 'FAIL' || s1 === 'FAIL' || s2 === 'FAIL' || s3 === 'FAIL') all = 'FAIL';
    Logger.log(v.name + ' stage0=' + s0 + ' stage1=' + s1 + ' stage2=' + s2 + ' stage3=' + s3);
  }
  Logger.log('=== golden self-test total = ' + all + ' ===');
  return all;
}

// M02/H03残: 本番前処理（allowlist→sanitize→fallback→builder）を通す self-test。
// vector.pipeline で期待挙動を明示: 'match'=body 一致で PASS・'reject'=例外送出で PASS・
// 'skip'=builder_na のみ。**その他の SKIP 変換は禁止**（例外→FAIL・成功すべきが例外→FAIL 等）。
function rv04c_productionPipelineSelfTest() {
  var all = 'PASS';
  for (var i = 0; i < RV04C_VECTORS.length; i++) {
    var v = RV04C_VECTORS[i];
    if (v.pipeline === 'skip') { Logger.log(v.name + ' pipeline=SKIP(enum:builder_na)'); continue; }
    var parts = _partsFromVec_(v);
    if (v.fallback_check) {
      for (var k = 0; k < parts.length; k++) {
        if (parts[k].name === 'file') parts[k].filename = v.fallback_check.raw_filename;
      }
    }
    var threw = false, body = null, emsg = '';
    try {
      body = rv04cBuildSignedBody_('/koseki/ingest', parts, v.boundary).body;
    } catch (e) { threw = true; emsg = e.message; }
    var res;
    if (v.pipeline === 'reject') {
      res = threw ? 'PASS(reject)' : 'FAIL(expected reject)';
    } else {  // 'match'
      if (threw) res = 'FAIL(unexpected throw: ' + emsg + ')';
      else res = (Utilities.base64Encode(body) === v.body_b64) ? 'PASS' : 'FAIL(body mismatch)';
    }
    if (res.indexOf('FAIL') === 0) all = 'FAIL';
    Logger.log(v.name + ' pipeline=' + res);
  }
  Logger.log('=== production pipeline self-test = ' + all + ' ===');
  return all;
}

function rv04c_builderLargeTest() {
  var big = [];
  for (var i = 0; i < 3000000; i++) big.push(((i * 131 + 7) % 256) - 128);
  var parts = [{ name: 'file', filename: 'big.pdf', contentType: 'application/pdf', value: big },
               { name: 'drive_file_id', value: utf8Bytes_('F-big') }];
  var body = buildMultipart_('RV04Cbig', parts);
  Logger.log('large body len=' + body.length + ' sha256=' + sha256Hex_(body));
  return sha256Hex_(body);
}
