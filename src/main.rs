mod demux;
mod hls;
mod rtp;
mod rtsp;

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use log::info;
use demux::{CLOCK, Demuxer, Frame, StreamInfo};
use tokio::{
    sync::{broadcast, mpsc, watch},
    time::{Instant, sleep_until},
};

const PLAYLIST_URL: &str =
    "https://live-cdn-1.video.tuwien.ac.at/lecturetube-live/bau178a-gm-1-audi-max/playlist.m3u8";
const RTSP_ADDR: &str = "0.0.0.0:8554";

#[tokio::main]
async fn main() {
    simple_logger::init_with_level(log::Level::Info).unwrap();

    let (seg_tx, seg_rx) = mpsc::channel(4);
    let (info_tx, info_rx) = watch::channel(None);
    let (frame_tx, _) = broadcast::channel(1024);

    tokio::spawn(hls::run(PLAYLIST_URL, seg_tx));
    tokio::spawn(pipeline(seg_rx, info_tx, frame_tx.clone()));

    let shared = Arc::new(rtsp::Shared {
        frames: frame_tx,
        info: info_rx,
    });
    rtsp::serve(RTSP_ADDR, shared).await.unwrap();
}

/// Demuxes segments and releases frames to clients in real time (segments arrive in bursts).
async fn pipeline(
    mut segments: mpsc::Receiver<Bytes>,
    info_tx: watch::Sender<Option<Arc<StreamInfo>>>,
    frames: broadcast::Sender<Arc<Frame>>,
) {
    let mut demuxer = Demuxer::new(info_tx);
    let mut anchor: Option<(Instant, u64)> = None;
    let (mut max_nal, mut max_frame) = (0, 0);
    let mut last_log = Instant::now();

    while let Some(segment) = segments.recv().await {
        for frame in demuxer.push_segment(&segment) {
            let now = Instant::now();
            let (t0, pts0) = *anchor.get_or_insert((now, frame.pts()));
            let offset =
                Duration::from_micros(frame.pts().saturating_sub(pts0) * 1_000_000 / CLOCK);
            let mut target = t0 + offset;
            // Re-anchor after stalls or timestamp jumps.
            if target + Duration::from_secs(1) < now || target > now + Duration::from_secs(10) {
                anchor = Some((now, frame.pts()));
                target = now;
            }
            sleep_until(target).await;
            if let Frame::Video { nals, .. } = &frame {
                max_nal = max_nal.max(nals.iter().map(Bytes::len).max().unwrap_or(0));
                max_frame = max_frame.max(nals.iter().map(Bytes::len).sum::<usize>());
                if last_log.elapsed() >= Duration::from_secs(5) {
                    info!("video: largest NAL {max_nal} B, largest frame {max_frame} B (last 5s)");
                    (max_nal, max_frame, last_log) = (0, 0, Instant::now());
                }
            }
            // No receivers is fine: nobody is watching.
            let _ = frames.send(Arc::new(frame));
        }
    }
}
