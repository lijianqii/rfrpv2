//! 极简 HTTP 响应工具：Dashboard（rfrps）与客户端状态端点（rfrpc）共用。
//!
//! 只实现"读取请求头 + 写一个 `Connection: close` 响应"这两件事，
//! 足以支撑状态页/JSON/指标三类只读端点，不引入完整 HTTP 栈。

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// 请求头上限（超过后按已读内容处理，解析失败按 `/` 处理）。
pub const MAX_REQUEST_HEAD: usize = 8192;

/// 读取 HTTP 请求头（读到 `\r\n\r\n` 或超过 [`MAX_REQUEST_HEAD`]）。
///
/// 返回 `Ok(None)` 表示对端在发送完整请求头前关闭。
pub async fn read_request_head<R>(stream: &mut R) -> std::io::Result<Option<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let mut buf = Vec::with_capacity(1024);
    let mut tmp = [0u8; 1024];
    loop {
        let n = stream.read(&mut tmp).await?;
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
        let head = read_request_head(&mut server).await.unwrap().unwrap();
        assert!(head.starts_with(b"GET /metrics"));
        // 读到请求头终止符即返回（允许把同批到达的后续字节一并读入，调用方按需忽略）。
        assert!(head.windows(4).any(|w| w == b"\r\n\r\n"));
    }

    #[tokio::test]
    async fn eof_before_head_returns_none() {
        let (client, mut server) = duplex(64);
        drop(client);
        assert!(read_request_head(&mut server).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn oversized_head_returned_as_is() {
        let (mut client, mut server) = duplex(64 * 1024);
        let payload = vec![b'a'; MAX_REQUEST_HEAD + 100];
        client.write_all(&payload).await.unwrap();
        let head = read_request_head(&mut server).await.unwrap().unwrap();
        assert!(head.len() > MAX_REQUEST_HEAD);
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
}
