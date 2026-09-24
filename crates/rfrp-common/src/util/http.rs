//! 极简 HTTP 响应工具：Dashboard（rfrps）与客户端状态端点（rfrpc）共用。
//!
//! 只实现"读取请求头 + 写一个 `Connection: close` 响应"这两件事，
//! 足以支撑状态页/JSON/指标三类只读端点，不引入完整 HTTP 栈。

use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// 请求头上限（超过后按已读内容处理，解析失败按 `/` 处理）。
pub const MAX_REQUEST_HEAD: usize = 8192;

/// 解析后的请求行与常用头部（Dashboard 与客户端状态端点共用）。
///
/// 两个只读 HTTP 端点此前各自写了一套 `httparse` 样板；集中到这里后行为一致
/// （最多 32 个头部、方法/路径默认值、`Accept`/`Cookie`/`Authorization` 提取）。
#[derive(Default)]
pub struct RequestHead {
    pub method: String,
    pub path: String,
    /// `Content-Length`（缺失或非法时为 0）。
    pub content_length: usize,
    /// `Accept` 是否包含 `text/html`（用于区分浏览器导航与脚本客户端）。
    pub accept_html: bool,
    pub authorization: Option<String>,
    pub cookie: Option<String>,
}

impl RequestHead {
    /// 解析请求头；解析失败时退化为 `GET /`，由调用方决定返回 404 还是登录页。
    pub fn parse(head: &[u8]) -> Self {
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut req = httparse::Request::new(&mut headers);
        let mut out = Self {
            method: "GET".into(),
            path: "/".into(),
            ..Default::default()
        };
        if let Ok(httparse::Status::Complete(_)) = req.parse(head) {
            if let Some(m) = req.method {
                out.method = m.to_string();
            }
            if let Some(p) = req.path {
                out.path = p.to_string();
            }
            for h in req.headers.iter() {
                let value = std::str::from_utf8(h.value).unwrap_or("").trim();
                if h.name.eq_ignore_ascii_case("content-length") {
                    out.content_length = value.parse().unwrap_or(0);
                } else if h.name.eq_ignore_ascii_case("accept") {
                    out.accept_html = value.to_ascii_lowercase().contains("text/html");
                } else if h.name.eq_ignore_ascii_case("authorization") {
                    out.authorization = Some(value.to_string());
                } else if h.name.eq_ignore_ascii_case("cookie") {
                    out.cookie = Some(value.to_string());
                }
            }
        }
        out
    }
}

/// HTML 转义（`&`、`<`、`>`、`"`、`'`）。
///
/// Dashboard / 状态页会把运行期数据（代理名、run_id、指标标签等）拼进 HTML。
/// 代理名由认证客户端控制，若直接拼接，恶意名字会在管理员浏览器中执行脚本
/// （存储型 XSS）。所有插入 HTML 的动态文本都必须经过本函数。
pub fn html_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// 读取 HTTP 请求头（读到 `\r\n\r\n` 或超过 [`MAX_REQUEST_HEAD`]）。
///
/// `timeout` 为**整体截止时间**：慢速请求（slowloris）超过后返回 `Ok(None)`。
/// 返回 `Ok(None)` 也表示对端在发送完整请求头前关闭。
pub async fn read_request_head<R>(
    stream: &mut R,
    timeout: Duration,
) -> std::io::Result<Option<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let mut buf = Vec::with_capacity(1024);
    let mut tmp = [0u8; 1024];
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Ok(None);
        }
        let n = match tokio::time::timeout(remaining, stream.read(&mut tmp)).await {
            Ok(r) => r?,
            Err(_) => return Ok(None),
        };
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            return Ok(Some(buf));
        }
        if buf.len() > MAX_REQUEST_HEAD {
            return Ok(Some(buf));
        }
    }
}

/// 写一个简单响应并关闭连接（`Connection: close`）。
pub async fn write_response<W>(
    stream: &mut W,
    status: u16,
    content_type: &str,
    body: &str,
    extra_header: Option<&str>,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let reason = match status {
        200 => "OK",
        302 => "Found",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        429 => "Too Many Requests",
        _ => "OK",
    };
    let mut resp = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n",
        body.len()
    );
    if let Some(h) = extra_header {
        resp.push_str(h);
        resp.push_str("\r\n");
    }
    resp.push_str("Connection: close\r\n\r\n");
    resp.push_str(body);
    stream.write_all(resp.as_bytes()).await?;
    stream.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn reads_head_until_terminator() {
        let (mut client, mut server) = duplex(4096);
        client
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\nBODY")
            .await
            .unwrap();
        let head = read_request_head(&mut server, Duration::from_secs(2))
            .await
            .unwrap()
            .unwrap();
        assert!(head.starts_with(b"GET /metrics"));
        // 读到请求头终止符即返回（允许把同批到达的后续字节一并读入，调用方按需忽略）。
        assert!(head.windows(4).any(|w| w == b"\r\n\r\n"));
    }

    #[tokio::test]
    async fn eof_before_head_returns_none() {
        let (client, mut server) = duplex(64);
        drop(client);
        assert!(read_request_head(&mut server, Duration::from_secs(2))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn oversized_head_returned_as_is() {
        let (mut client, mut server) = duplex(64 * 1024);
        let payload = vec![b'a'; MAX_REQUEST_HEAD + 100];
        client.write_all(&payload).await.unwrap();
        let head = read_request_head(&mut server, Duration::from_secs(2))
            .await
            .unwrap()
            .unwrap();
        assert!(head.len() > MAX_REQUEST_HEAD);
    }

    #[tokio::test]
    async fn slow_request_head_times_out() {
        // 只发一半请求头后停住：整体超时后应返回 None（防 slowloris）。
        let (mut client, mut server) = duplex(4096);
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: x\r\n")
            .await
            .unwrap();
        let started = tokio::time::Instant::now();
        let head = read_request_head(&mut server, Duration::from_millis(150))
            .await
            .unwrap();
        assert!(head.is_none(), "incomplete head must time out");
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn writes_response_with_status_and_body() {
        let (mut client, mut server) = duplex(4096);
        write_response(&mut server, 200, "text/plain", "hello", None)
            .await
            .unwrap();
        drop(server); // 关闭写端，read_to_end 才能结束
        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(text.contains("Content-Length: 5\r\n"));
        assert!(text.contains("Connection: close\r\n"));
        assert!(text.ends_with("hello"));
    }

    #[tokio::test]
    async fn writes_response_with_extra_header() {
        let (mut client, mut server) = duplex(4096);
        write_response(
            &mut server,
            401,
            "text/plain",
            "no",
            Some("WWW-Authenticate: Basic realm=\"x\""),
        )
        .await
        .unwrap();
        drop(server); // 关闭写端，read_to_end 才能结束
        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.starts_with("HTTP/1.1 401 Unauthorized\r\n"));
        assert!(text.contains("WWW-Authenticate: Basic realm=\"x\"\r\n"));
    }

    #[test]
    fn html_escape_covers_all_special_chars() {
        assert_eq!(
            html_escape("<script>alert(\"x&y\")</script>'"),
            "&lt;script&gt;alert(&quot;x&amp;y&quot;)&lt;/script&gt;&#39;"
        );
        assert_eq!(html_escape("plain-proxy_1"), "plain-proxy_1");
        assert_eq!(html_escape(""), "");
    }

    #[test]
    fn request_head_parses_method_path_and_headers() {
        let head = b"POST /login HTTP/1.1\r\nHost: x\r\nContent-Length: 12\r\n\
                     Accept: text/html,application/xhtml+xml\r\n\
                     Authorization: Basic abc\r\nCookie: a=1; b=2\r\n\r\n";
        let req = RequestHead::parse(head);
        assert_eq!(req.method, "POST");
        assert_eq!(req.path, "/login");
        assert_eq!(req.content_length, 12);
        assert!(req.accept_html);
        assert_eq!(req.authorization.as_deref(), Some("Basic abc"));
        assert_eq!(req.cookie.as_deref(), Some("a=1; b=2"));

        // 解析失败时退化为 GET /（由调用方决定返回 404 还是登录页）。
        let req = RequestHead::parse(b"garbage");
        assert_eq!(req.method, "GET");
        assert_eq!(req.path, "/");
        assert_eq!(req.content_length, 0);
        assert!(!req.accept_html);
        assert!(req.authorization.is_none());
    }
}
