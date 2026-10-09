use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use log::{info, warn};
use reqwest::{
    Client, Url,
    header::{ACCEPT, ACCEPT_LANGUAGE, HeaderMap, HeaderValue, ORIGIN, REFERER, USER_AGENT},
};
use tokio::{sync::mpsc, time::sleep};

/// Number of segments behind the live edge to start from.
const START_BEHIND: usize = 2;

fn build_client() -> Client {
    let mut headers = HeaderMap::new();
    headers.insert(
        USER_AGENT,
        HeaderValue::from_static(
            "Mozilla/5.0 (X11; Linux x86_64; rv:156.0) Gecko/20100101 Firefox/156.0",
        ),
    );
    headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
    headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));
    headers.insert(ORIGIN, HeaderValue::from_static("null"));
    headers.insert(REFERER, HeaderValue::from_static("https://tuwel.tuwien.ac.at/"));
    Client::builder()
        .referer(false)
        .default_headers(headers)
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap()
}

async fn get_text(client: &Client, url: &Url) -> Result<String> {
    Ok(client.get(url.clone()).send().await?.error_for_status()?.text().await?)
}

/// Follows the live playlist forever, sending each new MPEG-TS segment to `tx`.
/// Returns when the receiver is dropped.
pub async fn run(master_url: &'static str, tx: mpsc::Sender<Bytes>) {
    let client = build_client();
    loop {
        match follow(&client, master_url, &tx).await {
            Ok(()) => return,
            Err(e) => {
                warn!("hls: {e:#}; retrying in 5s");
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn follow(client: &Client, master_url: &str, tx: &mpsc::Sender<Bytes>) -> Result<()> {
    let master_url = Url::parse(master_url)?;
    let text = get_text(client, &master_url).await?;
    let master = m3u8_rs::parse_master_playlist_res(text.as_bytes())
        .map_err(|e| anyhow!("master playlist: {e:?}"))?;
    let variant = master
        .variants
        .iter()
        .max_by_key(|v| v.bandwidth)
        .context("master playlist has no variants")?;
    let media_url = master_url.join(&variant.uri)?;
    info!("hls: following {media_url}");

    let mut next_seq: Option<u64> = None;
    loop {
        let text = get_text(client, &media_url).await?;
        let media = m3u8_rs::parse_media_playlist_res(text.as_bytes())
            .map_err(|e| anyhow!("media playlist: {e:?}"))?;
        let first = media.media_sequence;

        let mut next = next_seq
            .unwrap_or(first + media.segments.len().saturating_sub(START_BEHIND) as u64);
        if next < first {
            warn!("hls: fell behind the playlist window, skipping {} segments", first - next);
            next = first;
        }

        for (i, segment) in media.segments.iter().enumerate() {
            let seq = first + i as u64;
            if seq < next {
                continue;
            }
            let url = media_url.join(&segment.uri)?;
            let bytes = client.get(url).send().await?.error_for_status()?.bytes().await?;
            if tx.send(bytes).await.is_err() {
                return Ok(());
            }
            next = seq + 1;
        }
        next_seq = Some(next);

        if media.end_list {
            info!("hls: playlist ended");
            return Ok(());
        }
        sleep(Duration::from_secs_f32((media.target_duration as f32 / 2.0).max(1.0))).await;
    }
}
