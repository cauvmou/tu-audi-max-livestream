//! Small RTSP server: one task per control connection, one RTP sender task per playing session.

use std::{collections::HashMap, io, net::IpAddr, sync::Arc, time::Duration};

use anyhow::Result;
use base64::{Engine, engine::general_purpose::STANDARD};
use log::{debug, info, warn};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream, UdpSocket, tcp::OwnedWriteHalf},
    sync::{Mutex, broadcast, watch},
    task::JoinHandle,
    time::{Instant, interval, timeout},
};

use crate::{
    demux::{CLOCK, Frame, StreamInfo},
    rtp::{AUDIO_PT, RtpWriter, VIDEO_PT, aac_payload, h264_payloads},
};

pub struct Shared {
    pub frames: broadcast::Sender<Arc<Frame>>,
    pub info: watch::Receiver<Option<Arc<StreamInfo>>>,
}

struct Request {
    method: String,
    uri: String,
    headers: HashMap<String, String>,
}

/// Control connection write half, shared so interleaved RTP never splits an RTSP response.
type Writer = Arc<Mutex<OwnedWriteHalf>>;

/// Where one track's RTP/RTCP packets go.
enum Sink {
    Udp { rtp: UdpSocket, rtcp: Option<UdpSocket> },
    Tcp { writer: Writer, channel: u8 },
}

impl Sink {
    /// UDP send errors (e.g. client port closed) are ignored; TCP errors end the stream.
    async fn rtp(&self, pkt: &[u8]) -> io::Result<()> {
        match self {
            Sink::Udp { rtp, .. } => {
                let _ = rtp.send(pkt).await;
                Ok(())
            }
            Sink::Tcp { writer, channel } => interleave(writer, *channel, pkt).await,
        }
    }

    async fn rtcp(&self, pkt: &[u8]) -> io::Result<()> {
        match self {
            Sink::Udp { rtcp, .. } => {
                if let Some(rtcp) = rtcp {
                    let _ = rtcp.send(pkt).await;
                }
                Ok(())
            }
            Sink::Tcp { writer, channel } => interleave(writer, channel + 1, pkt).await,
        }
    }
}

async fn interleave(writer: &Writer, channel: u8, pkt: &[u8]) -> io::Result<()> {
    let mut frame = Vec::with_capacity(4 + pkt.len());
    frame.extend_from_slice(&[b'$', channel]);
    frame.extend_from_slice(&(pkt.len() as u16).to_be_bytes());
    frame.extend_from_slice(pkt);
    writer.lock().await.write_all(&frame).await
}

#[derive(Default)]
struct Session {
    id: String,
    video: Option<Sink>,
    audio: Option<Sink>,
    task: Option<JoinHandle<()>>,
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

pub async fn serve(addr: &str, shared: Arc<Shared>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("rtsp: listening on rtsp://{addr}/stream");
    loop {
        let (sock, peer) = listener.accept().await?;
        let shared = shared.clone();
        tokio::spawn(async move {
            info!("rtsp: {peer} connected");
            if let Err(e) = handle(sock, peer.ip(), shared).await {
                debug!("rtsp: {peer}: {e:#}");
            }
            info!("rtsp: {peer} disconnected");
        });
    }
}

async fn read_request(rd: &mut BufReader<tokio::net::tcp::OwnedReadHalf>) -> Result<Option<Request>> {
    // Clients may send interleaved RTCP ("$ channel len payload"); ignore it.
    loop {
        match rd.fill_buf().await?.first() {
            None => return Ok(None),
            Some(b'$') => {
                let mut header = [0u8; 4];
                rd.read_exact(&mut header).await?;
                let mut skip = vec![0; u16::from_be_bytes([header[2], header[3]]) as usize];
                rd.read_exact(&mut skip).await?;
            }
            Some(_) => break,
        }
    }
    let mut line = String::new();
    loop {
        line.clear();
        if rd.read_line(&mut line).await? == 0 {
            return Ok(None);
        }
        if !line.trim().is_empty() {
            break;
        }
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let uri = parts.next().unwrap_or_default().to_string();

    let mut headers = HashMap::new();
    loop {
        line.clear();
        if rd.read_line(&mut line).await? == 0 {
            return Ok(None);
        }
        if line.trim().is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    if let Some(len) = headers.get("content-length").and_then(|v| v.parse::<usize>().ok()) {
        let mut body = vec![0; len.min(64 * 1024)];
        rd.read_exact(&mut body).await?;
    }
    Ok(Some(Request { method, uri, headers }))
}

fn response(code: u16, reason: &str, cseq: &str, headers: &[(&str, String)], body: &str) -> String {
    let mut r = format!("RTSP/1.0 {code} {reason}\r\nCSeq: {cseq}\r\n");
    for (k, v) in headers {
        r.push_str(&format!("{k}: {v}\r\n"));
    }
    if !body.is_empty() {
        r.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    r.push_str("\r\n");
    r.push_str(body);
    r
}

async fn handle(sock: TcpStream, peer: IpAddr, shared: Arc<Shared>) -> Result<()> {
    let (rd, wr) = sock.into_split();
    let mut rd = BufReader::new(rd);
    let writer: Writer = Arc::new(Mutex::new(wr));
    // Dropping the session (including on disconnect) aborts its sender task.
    let mut session = Session::default();
    session.id = format!("{:08x}", rand::random::<u32>());

    while let Some(req) = read_request(&mut rd).await? {
        let cseq = req.headers.get("cseq").cloned().unwrap_or_default();
        let session_hdr = ("Session", format!("{};timeout=60", session.id));
        let mut done = false;

        let resp = match req.method.as_str() {
            "OPTIONS" => response(
                200,
                "OK",
                &cseq,
                &[("Public", "OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN, GET_PARAMETER".into())],
                "",
            ),
            "DESCRIBE" => {
                let mut info_rx = shared.info.clone();
                let ready = timeout(Duration::from_secs(20), info_rx.wait_for(|i| i.is_some())).await;
                let info = match ready {
                    Ok(Ok(guard)) => guard.clone(),
                    _ => None,
                };
                match info {
                    Some(info) => response(
                        200,
                        "OK",
                        &cseq,
                        &[
                            ("Content-Base", format!("{}/", req.uri.trim_end_matches('/'))),
                            ("Content-Type", "application/sdp".into()),
                        ],
                        &sdp(&info),
                    ),
                    None => response(503, "Service Unavailable", &cseq, &[], ""),
                }
            }
            "SETUP" => match parse_transport(req.headers.get("transport").map(String::as_str)) {
                None => response(461, "Unsupported Transport", &cseq, &[], ""),
                Some(wanted) => {
                    let (sink, transport) = match wanted {
                        Wanted::Udp(port) => {
                            let rtp = UdpSocket::bind("0.0.0.0:0").await?;
                            rtp.connect((peer, port)).await?;
                            let server_port = rtp.local_addr()?.port();
                            let rtcp = UdpSocket::bind(("0.0.0.0", server_port + 1)).await.ok();
                            if let Some(rtcp) = &rtcp {
                                rtcp.connect((peer, port + 1)).await?;
                            }
                            (
                                Sink::Udp { rtp, rtcp },
                                format!(
                                    "RTP/AVP;unicast;client_port={port}-{};server_port={server_port}-{}",
                                    port + 1,
                                    server_port + 1
                                ),
                            )
                        }
                        Wanted::Tcp(channel) => (
                            Sink::Tcp { writer: writer.clone(), channel },
                            format!("RTP/AVP/TCP;unicast;interleaved={channel}-{}", channel + 1),
                        ),
                    };
                    if req.uri.contains("track1") {
                        session.audio = Some(sink);
                    } else {
                        session.video = Some(sink);
                    }
                    response(200, "OK", &cseq, &[("Transport", transport), session_hdr], "")
                }
            },
            "PLAY" => {
                let sample_rate = shared
                    .info
                    .borrow()
                    .as_ref()
                    .map_or(48_000, |i| i.audio.sample_rate);
                if session.task.is_none() && (session.video.is_some() || session.audio.is_some()) {
                    let rx = shared.frames.subscribe();
                    session.task = Some(tokio::spawn(send_loop(
                        rx,
                        session.video.take(),
                        session.audio.take(),
                        sample_rate,
                    )));
                    info!("rtsp: {peer} playing (session {})", session.id);
                }
                response(200, "OK", &cseq, &[("Range", "npt=0.000-".into()), session_hdr], "")
            }
            "TEARDOWN" => {
                done = true;
                response(200, "OK", &cseq, &[session_hdr], "")
            }
            "GET_PARAMETER" | "SET_PARAMETER" => response(200, "OK", &cseq, &[session_hdr], ""),
            _ => response(501, "Not Implemented", &cseq, &[], ""),
        };
        info!(
            "rtsp: {peer} {} {} -> {}",
            req.method,
            req.uri,
            resp.lines().next().unwrap_or_default()
        );
        writer.lock().await.write_all(resp.as_bytes()).await?;
        if done {
            break;
        }
    }
    Ok(())
}

enum Wanted {
    Udp(u16),
    Tcp(u8),
}

/// Picks the first supported option from a Transport header (clients may offer several).
fn parse_transport(transport: Option<&str>) -> Option<Wanted> {
    transport?.split(',').find_map(|option| {
        let field = |name: &str| -> Option<u16> {
            option
                .split(';')
                .find_map(|p| p.trim().strip_prefix(name))
                .and_then(|v| v.split('-').next())
                .and_then(|v| v.parse().ok())
        };
        if option.contains("/TCP") || option.contains("interleaved=") {
            field("interleaved=").and_then(|c| u8::try_from(c).ok()).map(Wanted::Tcp)
        } else {
            field("client_port=").map(Wanted::Udp)
        }
    })
}

fn sdp(info: &StreamInfo) -> String {
    let a = &info.audio;
    format!(
        "v=0\r\n\
         o=- 0 0 IN IP4 127.0.0.1\r\n\
         s=TU Audi Max\r\n\
         c=IN IP4 0.0.0.0\r\n\
         t=0 0\r\n\
         m=video 0 RTP/AVP {VIDEO_PT}\r\n\
         a=rtpmap:{VIDEO_PT} H264/{CLOCK}\r\n\
         a=fmtp:{VIDEO_PT} packetization-mode=1;profile-level-id={:02X}{:02X}{:02X};sprop-parameter-sets={},{}\r\n\
         a=control:track0\r\n\
         m=audio 0 RTP/AVP {AUDIO_PT}\r\n\
         a=rtpmap:{AUDIO_PT} MPEG4-GENERIC/{}/{}\r\n\
         a=fmtp:{AUDIO_PT} streamtype=5;profile-level-id=1;mode=AAC-hbr;sizelength=13;indexlength=3;indexdeltalength=3;config={:02X}{:02X}\r\n\
         a=control:track1\r\n",
        info.sps[1],
        info.sps[2],
        info.sps[3],
        STANDARD.encode(&info.sps),
        STANDARD.encode(&info.pps),
        a.sample_rate,
        a.channels,
        a.config[0],
        a.config[1],
    )
}

async fn send_loop(
    mut rx: broadcast::Receiver<Arc<Frame>>,
    video: Option<Sink>,
    audio: Option<Sink>,
    sample_rate: u32,
) {
    let mut vw = RtpWriter::new(VIDEO_PT, CLOCK as u32);
    let mut aw = RtpWriter::new(AUDIO_PT, sample_rate);
    // Video must start on a keyframe; audio waits for video so both begin together.
    let mut synced = video.is_none();
    let started = Instant::now();
    let mut tick = interval(Duration::from_secs(1));
    let mut ticks = 0u64;

    loop {
        let frame = tokio::select! {
            received = rx.recv() => match received {
                Ok(f) => f,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("rtsp: client lagged, dropped {n} frames");
                    synced = video.is_none();
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => return,
            },
            _ = tick.tick() => {
                ticks += 1;
                if ticks % 2 == 0 {
                    for (sink, writer) in [(&video, &vw), (&audio, &aw)] {
                        if let (Some(sink), Some(report)) = (sink, writer.sender_report()) {
                            if sink.rtcp(&report).await.is_err() {
                                return;
                            }
                        }
                    }
                }
                if ticks % 5 == 0 {
                    info!(
                        "rtsp: sent {} video / {} audio packets{}",
                        vw.packets(),
                        aw.packets(),
                        if synced { "" } else { " (waiting for keyframe)" }
                    );
                }
                continue;
            }
        };
        match &*frame {
            Frame::Video { pts, idr, nals } => {
                let Some(sink) = &video else { continue };
                if !synced {
                    if !idr {
                        continue;
                    }
                    synced = true;
                    info!("rtsp: first keyframe after {} ms", started.elapsed().as_millis());
                }
                for (payload, marker) in h264_payloads(nals) {
                    if sink.rtp(&vw.packet(*pts as u32, marker, &payload)).await.is_err() {
                        return;
                    }
                }
            }
            Frame::Audio { pts, data } => {
                let Some(sink) = &audio else { continue };
                if !synced {
                    continue;
                }
                let ts = (*pts as u128 * sample_rate as u128 / CLOCK as u128) as u32;
                if sink.rtp(&aw.packet(ts, true, &aac_payload(data))).await.is_err() {
                    return;
                }
            }
        }
    }
}
