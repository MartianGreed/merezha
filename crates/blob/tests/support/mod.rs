//! Test support: a small S3-compatible server that verifies SigV4 request
//! and presigned-URL signatures, stores objects on disk, and injects
//! faults; plus the stores the scenario suite runs on.
#![allow(dead_code)]

use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures_util::StreamExt;
use merezha_blob::{DriverConfig, LocalConfig, ObjectBlobStore, S3Config};
use merezha_platform::Secret;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::io::AsyncWriteExt;

pub const BUCKET: &str = "media";
pub const REGION: &str = "us-east-1";
pub const ACCESS_KEY: &str = "AKIDMEREZHATEST";
pub const SECRET_KEY: &str = "merezha-test-secret-key";

/// One stored object.
#[derive(Clone, Debug)]
pub struct Object {
    pub path: PathBuf,
    pub size: u64,
    pub etag: String,
    pub content_type: Option<String>,
    pub content_disposition: Option<String>,
    pub cache_control: Option<String>,
}

#[derive(Default)]
struct Upload {
    key: String,
    headers: BTreeMap<String, String>,
    parts: BTreeMap<u32, PathBuf>,
}

#[derive(Default)]
struct Store {
    objects: BTreeMap<String, Object>,
    uploads: BTreeMap<String, Upload>,
    next: u64,
}

/// A request as the server saw it.
#[derive(Clone, Debug)]
pub struct Seen {
    pub method: Method,
    pub path: String,
    pub query: String,
}

pub struct MockS3 {
    pub addr: SocketAddr,
    dir: tempfile::TempDir,
    store: Mutex<Store>,
    /// Statuses answered (with an S3 error body) before normal handling.
    faults: Mutex<VecDeque<u16>>,
    /// Accept requests and never answer.
    pub hang: AtomicBool,
    pub requests: AtomicUsize,
    seen: Mutex<Vec<Seen>>,
}

impl MockS3 {
    pub async fn start() -> Arc<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock = Arc::new(Self {
            addr: listener.local_addr().unwrap(),
            dir: tempfile::tempdir().unwrap(),
            store: Mutex::default(),
            faults: Mutex::default(),
            hang: AtomicBool::new(false),
            requests: AtomicUsize::new(0),
            seen: Mutex::default(),
        });
        let app = Router::new().fallback(handle).with_state(mock.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        mock
    }
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }
    pub fn config(&self) -> S3Config {
        let mut config = S3Config::new(
            BUCKET,
            REGION,
            Secret::new(ACCESS_KEY),
            Secret::new(SECRET_KEY),
        );
        config.endpoint = Some(self.endpoint());
        config.allow_http = true;
        config.timeout = Duration::from_secs(5);
        config
    }
    pub fn open(&self) -> ObjectBlobStore {
        ObjectBlobStore::open(&DriverConfig::S3(self.config())).unwrap()
    }
    /// Requests seen with this method and no query.
    pub fn plain_requests(&self, method: Method) -> usize {
        self.seen()
            .iter()
            .filter(|r| r.method == method && r.query.is_empty())
            .count()
    }
    pub fn fail_next(&self, statuses: &[u16]) {
        self.faults.lock().unwrap().extend(statuses);
    }
    pub fn object(&self, key: &str) -> Option<Object> {
        self.store.lock().unwrap().objects.get(key).cloned()
    }
    pub fn object_count(&self) -> usize {
        self.store.lock().unwrap().objects.len()
    }
    pub fn open_uploads(&self) -> usize {
        self.store.lock().unwrap().uploads.len()
    }
    pub fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
    pub fn request_count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
    fn next_id(&self) -> u64 {
        let mut store = self.store.lock().unwrap();
        store.next += 1;
        store.next
    }
}

fn s3_error(status: StatusCode, code: &str) -> Response {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code><Message>{code}</Message></Error>"
    );
    (status, [(header::CONTENT_TYPE, "application/xml")], body).into_response()
}

async fn handle(State(mock): State<Arc<MockS3>>, request: Request) -> Response {
    mock.requests.fetch_add(1, Ordering::SeqCst);
    let (parts, body) = request.into_parts();
    let path = parts.uri.path().to_owned();
    let raw_query = parts.uri.query().unwrap_or_default().to_owned();
    mock.seen.lock().unwrap().push(Seen {
        method: parts.method.clone(),
        path: path.clone(),
        query: raw_query.clone(),
    });
    if mock.hang.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
    let fault = mock.faults.lock().unwrap().pop_front();
    if let Some(status) = fault {
        let status = StatusCode::from_u16(status).unwrap();
        let mut response = s3_error(
            status,
            if status.is_server_error() {
                "InternalError"
            } else {
                "Refused"
            },
        );
        // A 300 with `Location`, which the HTTP client does not follow,
        // reaches object_store as a rendered status.
        if status == StatusCode::MULTIPLE_CHOICES {
            response
                .headers_mut()
                .insert(header::LOCATION, "http://127.0.0.1:9/".parse().unwrap());
        }
        return response;
    }
    let query = parse_query(&raw_query);
    let presigned = query.iter().any(|(k, _)| k == "X-Amz-Signature");
    let auth = if presigned {
        verify_presigned(&parts.method, &path, &query, &parts.headers)
    } else {
        verify_header_signature(&parts.method, &path, &query, &parts.headers)
    };
    if let Err(code) = auth {
        return s3_error(StatusCode::FORBIDDEN, code);
    }
    if parts.method == Method::GET
        && (path == format!("/{BUCKET}") || path == format!("/{BUCKET}/"))
        && query.iter().any(|(k, v)| k == "list-type" && v == "2")
    {
        return list(&mock, &query);
    }
    let Some(rest) = path.strip_prefix(&format!("/{BUCKET}/")) else {
        return s3_error(StatusCode::NOT_FOUND, "NoSuchBucket");
    };
    let key = rest.to_owned();
    let param = |name: &str| {
        query
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    };
    match (parts.method.clone(), param("uploadId"), param("partNumber")) {
        (Method::POST, None, _) if param("uploads").is_some() => {
            let id = format!("upload{}", mock.next_id());
            let headers = attribute_headers(&parts.headers);
            mock.store.lock().unwrap().uploads.insert(
                id.clone(),
                Upload {
                    key: key.clone(),
                    headers,
                    parts: BTreeMap::new(),
                },
            );
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><InitiateMultipartUploadResult><Bucket>{BUCKET}</Bucket><Key>{key}</Key><UploadId>{id}</UploadId></InitiateMultipartUploadResult>"
            );
            (StatusCode::OK, xml).into_response()
        }
        (Method::PUT, Some(id), Some(number)) => {
            let Ok(number) = number.parse::<u32>() else {
                return s3_error(StatusCode::BAD_REQUEST, "InvalidArgument");
            };
            if !mock.store.lock().unwrap().uploads.contains_key(&id) {
                return s3_error(StatusCode::NOT_FOUND, "NoSuchUpload");
            }
            let file = mock.dir.path().join(format!("part-{}", mock.next_id()));
            let (_, etag) = match write_body(body, &file, &parts.headers).await {
                Ok(written) => written,
                Err(response) => return response,
            };
            let mut store = mock.store.lock().unwrap();
            let Some(upload) = store.uploads.get_mut(&id) else {
                return s3_error(StatusCode::NOT_FOUND, "NoSuchUpload");
            };
            upload.parts.insert(number, file);
            (StatusCode::OK, [(header::ETAG, etag)]).into_response()
        }
        (Method::POST, Some(id), _) => {
            let Ok(xml) = axum::body::to_bytes(body, 1 << 20).await else {
                return s3_error(StatusCode::BAD_REQUEST, "MalformedXML");
            };
            let xml = String::from_utf8_lossy(&xml).into_owned();
            let numbers: Vec<u32> = xml
                .split("<PartNumber>")
                .skip(1)
                .filter_map(|s| s.split('<').next()?.parse().ok())
                .collect();
            let Some(upload) = mock.store.lock().unwrap().uploads.remove(&id) else {
                return s3_error(StatusCode::NOT_FOUND, "NoSuchUpload");
            };
            let target = mock.dir.path().join(format!("object-{}", mock.next_id()));
            let mut out = tokio::fs::File::create(&target).await.unwrap();
            let mut hasher = Sha256::new();
            let mut size = 0u64;
            for number in numbers {
                let Some(part) = upload.parts.get(&number) else {
                    return s3_error(StatusCode::BAD_REQUEST, "InvalidPart");
                };
                let mut file = tokio::fs::File::open(part).await.unwrap();
                let mut buffer = vec![0u8; 64 * 1024];
                loop {
                    let read = tokio::io::AsyncReadExt::read(&mut file, &mut buffer)
                        .await
                        .unwrap();
                    if read == 0 {
                        break;
                    }
                    hasher.update(&buffer[..read]);
                    out.write_all(&buffer[..read]).await.unwrap();
                    size += read as u64;
                }
                let _ = tokio::fs::remove_file(part).await;
            }
            out.flush().await.unwrap();
            let etag = format!("\"{}-mp\"", &hex(&hasher.finalize())[..32]);
            store_object(
                &mock,
                &upload.key,
                target,
                size,
                etag.clone(),
                &upload.headers,
            );
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUploadResult><Bucket>{BUCKET}</Bucket><Key>{}</Key><ETag>{}</ETag></CompleteMultipartUploadResult>",
                upload.key,
                etag.replace('"', "&quot;")
            );
            (StatusCode::OK, xml).into_response()
        }
        (Method::DELETE, Some(id), _) => {
            if let Some(upload) = mock.store.lock().unwrap().uploads.remove(&id) {
                for part in upload.parts.values() {
                    let _ = std::fs::remove_file(part);
                }
            }
            StatusCode::NO_CONTENT.into_response()
        }
        (Method::PUT, None, _) => {
            let file = mock.dir.path().join(format!("object-{}", mock.next_id()));
            let (size, etag) = match write_body(body, &file, &parts.headers).await {
                Ok(written) => written,
                Err(response) => return response,
            };
            let headers = attribute_headers(&parts.headers);
            store_object(&mock, &key, file, size, etag.clone(), &headers);
            (StatusCode::OK, [(header::ETAG, etag)]).into_response()
        }
        (Method::GET | Method::HEAD, None, _) => {
            let Some(object) = mock.object(&key) else {
                return s3_error(StatusCode::NOT_FOUND, "NoSuchKey");
            };
            read_object(&parts.method, &parts.headers, &object).await
        }
        (Method::DELETE, None, _) => {
            if let Some(object) = mock.store.lock().unwrap().objects.remove(&key) {
                let _ = std::fs::remove_file(object.path);
            }
            StatusCode::NO_CONTENT.into_response()
        }
        _ => s3_error(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed"),
    }
}

/// ListObjectsV2: keys under `prefix`, at most `max-keys`.
fn list(mock: &MockS3, query: &[(String, String)]) -> Response {
    let param = |name: &str| {
        query
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    };
    let prefix = param("prefix").unwrap_or_default();
    let max: usize = param("max-keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let store = mock.store.lock().unwrap();
    let matching: Vec<_> = store
        .objects
        .iter()
        .filter(|(key, _)| key.starts_with(&prefix))
        .collect();
    let contents: String = matching
        .iter()
        .take(max)
        .map(|(key, object)| {
            format!(
                "<Contents><Key>{key}</Key><LastModified>2026-10-24T12:00:00.000Z</LastModified><ETag>{}</ETag><Size>{}</Size></Contents>",
                object.etag.replace('"', "&quot;"),
                object.size
            )
        })
        .collect();
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>{BUCKET}</Name><Prefix>{prefix}</Prefix><KeyCount>{}</KeyCount><MaxKeys>{max}</MaxKeys><IsTruncated>{}</IsTruncated>{contents}</ListBucketResult>",
        matching.len().min(max),
        matching.len() > max
    );
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/xml")],
        xml,
    )
        .into_response()
}

fn attribute_headers(headers: &HeaderMap) -> BTreeMap<String, String> {
    ["content-type", "content-disposition", "cache-control"]
        .iter()
        .filter_map(|name| {
            let value = headers.get(*name)?.to_str().ok()?;
            Some(((*name).to_owned(), value.to_owned()))
        })
        .collect()
}

fn store_object(
    mock: &MockS3,
    key: &str,
    path: PathBuf,
    size: u64,
    etag: String,
    headers: &BTreeMap<String, String>,
) {
    let object = Object {
        path,
        size,
        etag,
        content_type: headers.get("content-type").cloned(),
        content_disposition: headers.get("content-disposition").cloned(),
        cache_control: headers.get("cache-control").cloned(),
    };
    if let Some(old) = mock
        .store
        .lock()
        .unwrap()
        .objects
        .insert(key.to_owned(), object)
    {
        let _ = std::fs::remove_file(old.path);
    }
}

/// Stream the body to `file`, checking `x-amz-content-sha256`.
#[allow(clippy::result_large_err)]
async fn write_body(
    body: Body,
    file: &std::path::Path,
    headers: &HeaderMap,
) -> Result<(u64, String), Response> {
    let mut out = tokio::fs::File::create(file).await.unwrap();
    let mut hasher = Sha256::new();
    let mut size = 0u64;
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else {
            return Err(s3_error(StatusCode::BAD_REQUEST, "IncompleteBody"));
        };
        hasher.update(&chunk);
        size += chunk.len() as u64;
        out.write_all(&chunk).await.unwrap();
    }
    out.flush().await.unwrap();
    let digest = hex(&hasher.finalize());
    let declared = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if declared != "UNSIGNED-PAYLOAD" && declared != digest {
        return Err(s3_error(
            StatusCode::BAD_REQUEST,
            "XAmzContentSHA256Mismatch",
        ));
    }
    Ok((size, format!("\"{}\"", &digest[..32])))
}

async fn read_object(method: &Method, headers: &HeaderMap, object: &Object) -> Response {
    let mut range = 0..object.size;
    let mut partial = false;
    if let Some(value) = headers.get(header::RANGE).and_then(|v| v.to_str().ok()) {
        let Some((start, end)) = value.strip_prefix("bytes=").and_then(|r| r.split_once('-'))
        else {
            return s3_error(StatusCode::BAD_REQUEST, "InvalidRange");
        };
        let start: u64 = start.parse().unwrap_or(u64::MAX);
        if start >= object.size {
            return s3_error(StatusCode::RANGE_NOT_SATISFIABLE, "InvalidRange");
        }
        let end = end
            .parse::<u64>()
            .map_or(object.size, |e| (e + 1).min(object.size));
        range = start..end;
        partial = true;
    }
    let mut response = Response::builder()
        .status(if partial {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        })
        .header(header::CONTENT_LENGTH, range.end - range.start)
        .header(header::ETAG, &object.etag)
        .header(header::LAST_MODIFIED, "Sat, 24 Oct 2026 12:00:00 GMT")
        .header(header::ACCEPT_RANGES, "bytes");
    if partial {
        response = response.header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", range.start, range.end - 1, object.size),
        );
    }
    for (name, value) in [
        (header::CONTENT_TYPE, &object.content_type),
        (header::CONTENT_DISPOSITION, &object.content_disposition),
        (header::CACHE_CONTROL, &object.cache_control),
    ] {
        if let Some(value) = value {
            response = response.header(name, value);
        }
    }
    if method == Method::HEAD {
        return response.body(Body::empty()).unwrap();
    }
    let bytes = std::fs::read(&object.path).unwrap();
    let slice = bytes[range.start as usize..range.end as usize].to_vec();
    response.body(Body::from(slice)).unwrap()
}

// --- SigV4, independently of object_store ---------------------------------

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

fn hmac(key: &[u8], message: &[u8]) -> Vec<u8> {
    let mut block = [0u8; 64];
    if key.len() > 64 {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let pad = |byte: u8| block.iter().map(|b| b ^ byte).collect::<Vec<_>>();
    let mut inner = Sha256::new();
    inner.update(pad(0x36));
    inner.update(message);
    let mut outer = Sha256::new();
    outer.update(pad(0x5c));
    outer.update(inner.finalize());
    outer.finalize().to_vec()
}

fn signing_key(date: &str) -> Vec<u8> {
    let k = hmac(format!("AWS4{SECRET_KEY}").as_bytes(), date.as_bytes());
    let k = hmac(&k, REGION.as_bytes());
    let k = hmac(&k, b"s3");
    hmac(&k, b"aws4_request")
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Ok(byte) = u8::from_str_radix(&text[i + 1..i + 3], 16)
        {
            out.push(byte);
            i += 3;
        } else {
            out.push(if bytes[i] == b'+' { b' ' } else { bytes[i] });
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn strict_encode(text: &str) -> String {
    text.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

fn parse_query(raw: &str) -> Vec<(String, String)> {
    raw.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((k, v)) => (percent_decode(k), percent_decode(v)),
            None => (percent_decode(pair), String::new()),
        })
        .collect()
}

fn canonical_query(query: &[(String, String)], skip: &str) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .filter(|(k, _)| k != skip)
        .map(|(k, v)| (strict_encode(k), strict_encode(v)))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn canonical_headers(headers: &HeaderMap, signed: &str) -> Option<String> {
    let mut out = String::new();
    for name in signed.split(';') {
        let values: Vec<String> = headers
            .get_all(name)
            .iter()
            .map(|v| {
                v.to_str()
                    .unwrap_or_default()
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect();
        if values.is_empty() {
            return None;
        }
        out.push_str(&format!("{name}:{}\n", values.join(",")));
    }
    Some(out)
}

fn expected_signature(
    method: &Method,
    path: &str,
    canonical_query: &str,
    canonical_headers: &str,
    signed: &str,
    payload: &str,
    amz_date: &str,
) -> String {
    let request = format!(
        "{}\n{path}\n{canonical_query}\n{canonical_headers}\n{signed}\n{payload}",
        method.as_str()
    );
    let date = &amz_date[..8.min(amz_date.len())];
    let scope = format!("{date}/{REGION}/s3/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(request.as_bytes())
    );
    hex(&hmac(&signing_key(date), to_sign.as_bytes()))
}

fn verify_header_signature(
    method: &Method,
    path: &str,
    query: &[(String, String)],
    headers: &HeaderMap,
) -> Result<(), &'static str> {
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or("AccessDenied")?;
    let fields: BTreeMap<&str, &str> = auth
        .strip_prefix("AWS4-HMAC-SHA256 ")
        .ok_or("AccessDenied")?
        .split(", ")
        .filter_map(|f| f.split_once('='))
        .collect();
    let credential = fields.get("Credential").ok_or("AccessDenied")?;
    if !credential.starts_with(&format!("{ACCESS_KEY}/")) {
        return Err("InvalidAccessKeyId");
    }
    let signed = fields.get("SignedHeaders").ok_or("AccessDenied")?;
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    };
    let canonical = canonical_headers(headers, signed).ok_or("AccessDenied")?;
    let expected = expected_signature(
        method,
        path,
        &canonical_query(query, ""),
        &canonical,
        signed,
        &header("x-amz-content-sha256"),
        &header("x-amz-date"),
    );
    if fields.get("Signature") == Some(&expected.as_str()) {
        Ok(())
    } else {
        Err("SignatureDoesNotMatch")
    }
}

fn verify_presigned(
    method: &Method,
    path: &str,
    query: &[(String, String)],
    headers: &HeaderMap,
) -> Result<(), &'static str> {
    let param = |name: &str| {
        query
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
            .unwrap_or_default()
    };
    if !param("X-Amz-Credential").starts_with(&format!("{ACCESS_KEY}/")) {
        return Err("InvalidAccessKeyId");
    }
    let amz_date = param("X-Amz-Date");
    let signed = param("X-Amz-SignedHeaders");
    let canonical = canonical_headers(headers, signed).ok_or("AccessDenied")?;
    let expected = expected_signature(
        method,
        path,
        &canonical_query(query, "X-Amz-Signature"),
        &canonical,
        signed,
        "UNSIGNED-PAYLOAD",
        amz_date,
    );
    if param("X-Amz-Signature") != expected {
        return Err("SignatureDoesNotMatch");
    }
    let issued = jiff::civil::DateTime::strptime("%Y%m%dT%H%M%SZ", amz_date)
        .ok()
        .and_then(|dt| dt.to_zoned(jiff::tz::TimeZone::UTC).ok())
        .ok_or("AccessDenied")?
        .timestamp();
    let expires: i64 = param("X-Amz-Expires").parse().map_err(|_| "AccessDenied")?;
    let age = jiff::Timestamp::now().as_second() - issued.as_second();
    if age > expires {
        return Err("AccessDenied");
    }
    Ok(())
}

// --- Stores for the scenario suite ----------------------------------------

/// A local store in a fresh temp directory.
pub fn local_store() -> (ObjectBlobStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = ObjectBlobStore::open(&DriverConfig::Local(LocalConfig::new(
        dir.path().join("root"),
    )))
    .unwrap();
    (store, dir)
}

/// The real S3-compatible service named by `MEREZHA_TEST_S3_*`, if set.
pub fn external_s3() -> Option<ObjectBlobStore> {
    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    let Some(endpoint) = var("MEREZHA_TEST_S3_ENDPOINT") else {
        eprintln!(
            "skipping the external S3 run: set MEREZHA_TEST_S3_ENDPOINT, MEREZHA_TEST_S3_BUCKET, \
             MEREZHA_TEST_S3_ACCESS_KEY_ID and MEREZHA_TEST_S3_SECRET_ACCESS_KEY \
             (and optionally MEREZHA_TEST_S3_REGION) to run against MinIO or another S3 service"
        );
        return None;
    };
    let mut config = S3Config::new(
        var("MEREZHA_TEST_S3_BUCKET").expect("MEREZHA_TEST_S3_BUCKET"),
        var("MEREZHA_TEST_S3_REGION").unwrap_or_else(|| "us-east-1".into()),
        Secret::new(var("MEREZHA_TEST_S3_ACCESS_KEY_ID").expect("MEREZHA_TEST_S3_ACCESS_KEY_ID")),
        Secret::new(
            var("MEREZHA_TEST_S3_SECRET_ACCESS_KEY").expect("MEREZHA_TEST_S3_SECRET_ACCESS_KEY"),
        ),
    );
    config.allow_http = endpoint.starts_with("http://");
    config.endpoint = Some(endpoint);
    // A per-run prefix keeps runs apart in a shared bucket.
    config.key_prefix = format!(
        "merezha-test/{}-{}",
        std::process::id(),
        jiff::Timestamp::now().as_millisecond()
    );
    Some(ObjectBlobStore::open(&DriverConfig::S3(config)).unwrap())
}
