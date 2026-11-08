//! End-to-end test for `Page::get_response_details` against a real Chrome.
//!
//! A fixture page loads an `.mp3` through an `<audio>` element and `fetch()`;
//! the page's response details must report each with its MIME type, status
//! and resource type, so callers can find the media a page loaded. Uses the
//! default (header-modifying) configuration, so it also guards against
//! emulated headers breaking subresource requests.
//!
//! Requires: Chrome running with --remote-debugging-port=9222
//! Run: cargo test -p spider --test chrome_response_details_e2e --features "chrome"

#[cfg(feature = "chrome")]
mod e2e {
    use spider::configuration::{ChromeEventTracker, WaitForIdleNetwork};
    use spider::tokio;
    use spider::tokio::io::{AsyncReadExt, AsyncWriteExt};
    use spider::website::Website;
    use std::time::Duration;

    const PAGE: &str = r#"<html><head><title>media</title></head><body>
<audio src="/clip.mp3" preload="auto" controls></audio>
<script>fetch("/clip.mp3?via=fetch")</script>
</body></html>"#;

    async fn chrome_url() -> Option<String> {
        if std::net::TcpStream::connect("127.0.0.1:9222").is_err() {
            return None;
        }
        let resp = spider::reqwest::get("http://127.0.0.1:9222/json/version")
            .await
            .ok()?;
        let json: serde_json::Value = resp.json().await.ok()?;
        json["webSocketDebuggerUrl"].as_str().map(String::from)
    }

    /// Serve the fixture page at `/` and a fake MP3 at `/clip.mp3*`.
    async fn media_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    let request = String::from_utf8_lossy(&buf[..n]);
                    let path = request.split_whitespace().nth(1).unwrap_or("/");
                    let (content_type, body): (&str, Vec<u8>) = if path.starts_with("/clip.mp3") {
                        // An MPEG frame header followed by silence is enough
                        // for Chrome to request and classify the response.
                        let mut mp3 = vec![0xFF, 0xFB, 0x90, 0x64];
                        mp3.resize(4096, 0);
                        ("audio/mpeg", mp3)
                    } else {
                        ("text/html", PAGE.as_bytes().to_vec())
                    };
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(head.as_bytes()).await;
                    let _ = socket.write_all(&body).await;
                });
            }
        });
        format!("http://127.0.0.1:{}/", addr.port())
    }

    /// The `Website` struct and crawl future overflow tokio's default 2 MiB
    /// worker stack, so run the body on a runtime with 16 MiB stacks.
    fn block_on_big_stack<F>(body: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        std::thread::Builder::new()
            .stack_size(16 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_stack_size(16 * 1024 * 1024)
                    .enable_all()
                    .build()
                    .expect("build tokio runtime")
                    .block_on(body);
            })
            .expect("spawn test thread")
            .join()
            .expect("test thread panicked");
    }

    #[test]
    fn response_details_report_media_mime_type() {
        block_on_big_stack(response_details_report_media_mime_type_body());
    }

    async fn response_details_report_media_mime_type_body() {
        let Some(ws) = chrome_url().await else {
            eprintln!("SKIP: no Chrome at port 9222");
            return;
        };
        let url = media_server().await;
        let mut website = Website::new(&url)
            .with_chrome_connection(Some(ws))
            .with_limit(1)
            .with_event_tracker(Some(ChromeEventTracker::new(true, true)))
            // `idle_network0` waits for real network idle; `idle_network`
            // only waits for the first LoadingFinished event.
            .with_wait_for_idle_network0(Some(WaitForIdleNetwork::new(Some(Duration::from_secs(
                5,
            )))))
            .with_request_timeout(Some(Duration::from_secs(30)))
            .build()
            .unwrap();
        let mut rx = website.subscribe(16);
        let collector = tokio::spawn(async move {
            let mut pages = Vec::new();
            while let Ok(page) = rx.recv().await {
                pages.push(page);
            }
            pages
        });
        website.crawl().await;
        website.unsubscribe();
        let pages = collector.await.unwrap();

        let page = pages.first().expect("crawled the fixture page");
        let details = page
            .get_response_details()
            .as_ref()
            .expect("response details are tracked");
        let detail = |u: &str| {
            details
                .get(u)
                .unwrap_or_else(|| panic!("no details for {u}, got: {details:?}"))
        };

        let media = detail(&format!("{url}clip.mp3"));
        assert_eq!(media.mime_type, "audio/mpeg");
        assert_eq!(media.status, 200);
        assert_eq!(media.resource_type, "Media");

        let fetched = detail(&format!("{url}clip.mp3?via=fetch"));
        assert_eq!(fetched.mime_type, "audio/mpeg");
        assert_eq!(fetched.resource_type, "Fetch");

        let doc = detail(&url);
        assert_eq!(doc.mime_type, "text/html");
        assert_eq!(doc.resource_type, "Document");
    }
}
