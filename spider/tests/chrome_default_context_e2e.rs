//! End-to-end tests for `with_chrome_default_context` against a real Chrome.
//!
//! A cookie is planted in the remote browser's default context; a crawl that
//! reuses the default context must send it, an isolated crawl must not, and
//! neither may close tabs that were open before the crawl.
//!
//! Requires: Chrome running with --remote-debugging-port=9222
//! Run: cargo test -p spider --test chrome_default_context_e2e --features "chrome"

#[cfg(feature = "chrome")]
mod e2e {
    use spider::chromiumoxide::cdp::browser_protocol::network::{CookieParam, TimeSinceEpoch};
    use spider::chromiumoxide::Browser;
    use spider::tokio;
    use spider::tokio::io::{AsyncReadExt, AsyncWriteExt};
    use spider::tokio_stream::StreamExt;
    use spider::website::Website;
    use std::time::Duration;

    const COOKIE: &str = "spider_default_ctx";

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

    async fn page_target_ids() -> Vec<String> {
        let resp = spider::reqwest::get("http://127.0.0.1:9222/json/list")
            .await
            .expect("list targets");
        let json: serde_json::Value = resp.json().await.expect("targets json");
        json.as_array()
            .into_iter()
            .flatten()
            .filter(|t| t["type"] == "page")
            .filter_map(|t| t["id"].as_str().map(String::from))
            .collect()
    }

    /// Serve every request with a page that echoes the request's Cookie header.
    async fn cookie_echo_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    let request = String::from_utf8_lossy(&buf[..n]);
                    let cookie = request
                        .lines()
                        .find_map(|l| l.strip_prefix("Cookie: ").or(l.strip_prefix("cookie: ")))
                        .unwrap_or("none")
                        .to_string();
                    let body = format!(
                        "<html><head><title>cookie echo</title></head><body><p id=\"cookie\">cookie={cookie}</p></body></html>"
                    );
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        format!("http://127.0.0.1:{}/", addr.port())
    }

    /// Set (or expire) the test cookie in the remote browser's default context.
    async fn set_default_context_cookie(ws: &str, url: &str, expire: bool) {
        let (browser, mut handler) = Browser::connect(ws.to_string()).await.unwrap();
        let task = tokio::spawn(async move { while handler.next().await.is_some() {} });
        let mut cookie = CookieParam::builder()
            .name(COOKIE)
            .value("yes")
            .url(url.to_string());
        if expire {
            cookie = cookie.expires(TimeSinceEpoch::new(1.0));
        }
        browser
            .set_cookies(vec![cookie.build().unwrap()])
            .await
            .unwrap();
        task.abort();
    }

    async fn crawl_html(ws: &str, url: &str, default_context: bool) -> String {
        let mut website = Website::new(url)
            .with_chrome_connection(Some(ws.to_string()))
            .with_chrome_default_context(default_context)
            .with_limit(1)
            .with_request_timeout(Some(Duration::from_secs(30)))
            .build()
            .unwrap();
        let mut rx = website.subscribe(16);
        let collector = tokio::spawn(async move {
            let mut html = String::new();
            while let Ok(page) = rx.recv().await {
                html.push_str(&page.get_html());
            }
            html
        });
        website.crawl().await;
        website.unsubscribe();
        collector.await.unwrap()
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
    fn default_context_shares_cookies_and_keeps_existing_tabs() {
        block_on_big_stack(default_context_shares_cookies_and_keeps_existing_tabs_body());
    }

    async fn default_context_shares_cookies_and_keeps_existing_tabs_body() {
        let Some(ws) = chrome_url().await else {
            eprintln!("SKIP: no Chrome at port 9222");
            return;
        };
        let url = cookie_echo_server().await;
        let tabs_before = page_target_ids().await;
        set_default_context_cookie(&ws, &url, false).await;

        let shared = crawl_html(&ws, &url, true).await;
        let isolated = crawl_html(&ws, &url, false).await;
        set_default_context_cookie(&ws, &url, true).await;

        assert!(
            shared.contains(&format!("{COOKIE}=yes")),
            "default-context crawl should send the browser's cookie, got: {shared}"
        );
        assert!(
            !isolated.contains(COOKIE),
            "isolated crawl must not see default-context cookies, got: {isolated}"
        );
        let tabs_after = page_target_ids().await;
        for id in &tabs_before {
            assert!(tabs_after.contains(id), "pre-existing tab {id} was closed");
        }
    }
}
