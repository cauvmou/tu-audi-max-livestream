//! Minimal MPEG-TS demuxer: extracts H.264 access units and AAC (ADTS) frames.

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use tokio::sync::watch;

pub const CLOCK: u64 = 90_000;
const PTS_MASK: u64 = (1 << 33) - 1;
const SAMPLE_RATES: [u32; 13] = [
    96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350,
];

pub enum Frame {
    Video { pts: u64, idr: bool, nals: Vec<Bytes> },
    Audio { pts: u64, data: Bytes },
}

impl Frame {
    pub fn pts(&self) -> u64 {
        match self {
            Frame::Video { pts, .. } | Frame::Audio { pts, .. } => *pts,
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct AudioInfo {
    /// AudioSpecificConfig
    pub config: [u8; 2],
    pub sample_rate: u32,
    pub channels: u8,
}

/// Everything needed to describe the stream in SDP.
#[derive(Clone, PartialEq)]
pub struct StreamInfo {
    pub sps: Bytes,
    pub pps: Bytes,
    pub audio: AudioInfo,
}

#[derive(Clone, Copy)]
enum Kind {
    Video,
    Audio,
}

#[derive(Default)]
struct Pes {
    buf: Vec<u8>,
    last_pts: u64,
}

pub struct Demuxer {
    pmt_pid: Option<u16>,
    pids: HashMap<u16, Kind>,
    pes: HashMap<u16, Pes>,
    sps: Option<Bytes>,
    pps: Option<Bytes>,
    audio: Option<AudioInfo>,
    /// Elementary stream bytes of the video access unit being assembled; large frames span
    /// several PES packets, only the first of which is guaranteed to carry a PTS.
    video_buf: Vec<u8>,
    video_pts: u64,
    info_tx: watch::Sender<Option<Arc<StreamInfo>>>,
}

impl Demuxer {
    pub fn new(info_tx: watch::Sender<Option<Arc<StreamInfo>>>) -> Self {
        Self {
            pmt_pid: None,
            pids: HashMap::new(),
            pes: HashMap::new(),
            sps: None,
            pps: None,
            audio: None,
            video_buf: Vec::new(),
            video_pts: 0,
            info_tx,
        }
    }

    pub fn push_segment(&mut self, data: &[u8]) -> Vec<Frame> {
        let mut out = Vec::new();
        for pkt in data.chunks_exact(188) {
            if pkt[0] == 0x47 {
                self.packet(pkt, &mut out);
            }
        }
        out
    }

    fn packet(&mut self, pkt: &[u8], out: &mut Vec<Frame>) {
        let pusi = pkt[1] & 0x40 != 0;
        let pid = (((pkt[1] & 0x1f) as u16) << 8) | pkt[2] as u16;
        let afc = (pkt[3] >> 4) & 3;
        if afc & 1 == 0 {
            return;
        }
        let mut off = 4;
        if afc & 2 != 0 {
            off += 1 + pkt[4] as usize;
        }
        if off >= 188 {
            return;
        }
        let payload = &pkt[off..];

        if pid == 0 {
            self.pat(payload, pusi);
        } else if Some(pid) == self.pmt_pid {
            self.pmt(payload, pusi);
        } else if let Some(&kind) = self.pids.get(&pid) {
            self.pes_data(pid, kind, payload, pusi, out);
        }
    }

    fn pat(&mut self, payload: &[u8], pusi: bool) {
        let Some(sec) = section(payload, pusi) else { return };
        if sec[0] != 0 {
            return;
        }
        let end = (3 + section_len(sec)).saturating_sub(4).min(sec.len());
        let mut i = 8;
        while i + 4 <= end {
            let program = u16::from_be_bytes([sec[i], sec[i + 1]]);
            let pid = u16::from_be_bytes([sec[i + 2], sec[i + 3]]) & 0x1fff;
            if program != 0 {
                self.pmt_pid = Some(pid);
                return;
            }
            i += 4;
        }
    }

    fn pmt(&mut self, payload: &[u8], pusi: bool) {
        let Some(sec) = section(payload, pusi) else { return };
        if sec[0] != 2 || sec.len() < 12 {
            return;
        }
        let end = (3 + section_len(sec)).saturating_sub(4).min(sec.len());
        let mut i = 12 + ((((sec[10] & 0x0f) as usize) << 8) | sec[11] as usize);
        while i + 5 <= end {
            let pid = u16::from_be_bytes([sec[i + 1], sec[i + 2]]) & 0x1fff;
            let es_info = (((sec[i + 3] & 0x0f) as usize) << 8) | sec[i + 4] as usize;
            match sec[i] {
                0x1b => {
                    self.pids.entry(pid).or_insert(Kind::Video);
                }
                0x0f => {
                    self.pids.entry(pid).or_insert(Kind::Audio);
                }
                _ => {}
            }
            i += 5 + es_info;
        }
    }

    fn pes_data(&mut self, pid: u16, kind: Kind, payload: &[u8], pusi: bool, out: &mut Vec<Frame>) {
        if pusi {
            let prev = self.pes.get_mut(&pid).map(|p| std::mem::take(&mut p.buf));
            if let Some(buf) = prev {
                self.finish(pid, kind, &buf, out);
            }
            self.pes.entry(pid).or_default().buf.extend_from_slice(payload);
        } else if let Some(p) = self.pes.get_mut(&pid) {
            p.buf.extend_from_slice(payload);
        }
    }

    fn finish(&mut self, pid: u16, kind: Kind, buf: &[u8], out: &mut Vec<Frame>) {
        if buf.len() < 9 || buf[..3] != [0, 0, 1] {
            return;
        }
        let start = 9 + buf[8] as usize;
        if buf.len() < start {
            return;
        }
        let pts = (buf[7] & 0x80 != 0 && buf.len() >= 14).then(|| {
            let b = &buf[9..14];
            let raw = (((b[0] as u64 >> 1) & 7) << 30)
                | ((b[1] as u64) << 22)
                | ((b[2] as u64 >> 1) << 15)
                | ((b[3] as u64) << 7)
                | (b[4] as u64 >> 1);
            let pes = self.pes.entry(pid).or_default();
            pes.last_pts = unwrap_pts(pes.last_pts, raw);
            pes.last_pts
        });

        match kind {
            Kind::Video => match pts {
                // A new PTS starts a new access unit; PES without one continue the current.
                Some(pts) if pts != self.video_pts || self.video_buf.is_empty() => {
                    self.flush_video(out);
                    self.video_pts = pts;
                    self.video_buf.extend_from_slice(&buf[start..]);
                }
                Some(_) => self.video_buf.extend_from_slice(&buf[start..]),
                None if !self.video_buf.is_empty() => {
                    self.video_buf.extend_from_slice(&buf[start..])
                }
                None => {}
            },
            Kind::Audio => {
                if let Some(pts) = pts {
                    self.audio(pts, &buf[start..], out);
                }
            }
        }
        self.publish();
    }

    fn flush_video(&mut self, out: &mut Vec<Frame>) {
        let data = std::mem::take(&mut self.video_buf);
        if !data.is_empty() {
            self.video(self.video_pts, &data, out);
        }
    }

    fn video(&mut self, pts: u64, data: &[u8], out: &mut Vec<Frame>) {
        let mut nals = Vec::new();
        let mut idr = false;
        for nal in split_nals(data) {
            match nal[0] & 0x1f {
                9 | 12 => continue,
                5 => idr = true,
                7 => self.sps = Some(Bytes::copy_from_slice(nal)),
                8 => self.pps = Some(Bytes::copy_from_slice(nal)),
                _ => {}
            }
            nals.push(Bytes::copy_from_slice(nal));
        }
        if !nals.is_empty() {
            out.push(Frame::Video { pts, idr, nals });
        }
    }

    fn audio(&mut self, pts: u64, data: &[u8], out: &mut Vec<Frame>) {
        let mut i = 0;
        let mut n = 0u64;
        while i + 7 <= data.len() {
            if data[i] != 0xff || data[i + 1] & 0xf0 != 0xf0 {
                i += 1;
                continue;
            }
            let header = if data[i + 1] & 1 == 0 { 9 } else { 7 };
            let len = (((data[i + 3] & 3) as usize) << 11)
                | ((data[i + 4] as usize) << 3)
                | (data[i + 5] as usize >> 5);
            if len < header || i + len > data.len() {
                break;
            }
            let aot = (data[i + 2] >> 6) + 1;
            let sfi = (data[i + 2] >> 2) & 0xf;
            let channels = ((data[i + 2] & 1) << 2) | (data[i + 3] >> 6);
            let Some(&sample_rate) = SAMPLE_RATES.get(sfi as usize) else { break };

            let config = (((aot as u16) << 11) | ((sfi as u16) << 7) | ((channels as u16) << 3))
                .to_be_bytes();
            let info = AudioInfo { config, sample_rate, channels };
            if self.audio.as_ref() != Some(&info) {
                self.audio = Some(info);
            }
            out.push(Frame::Audio {
                pts: pts + n * 1024 * CLOCK / sample_rate as u64,
                data: Bytes::copy_from_slice(&data[i + header..i + len]),
            });
            n += 1;
            i += len;
        }
    }

    fn publish(&self) {
        let (Some(sps), Some(pps), Some(audio)) = (&self.sps, &self.pps, &self.audio) else {
            return;
        };
        let info = StreamInfo { sps: sps.clone(), pps: pps.clone(), audio: audio.clone() };
        self.info_tx.send_if_modified(|cur| {
            if cur.as_deref() == Some(&info) {
                false
            } else {
                *cur = Some(Arc::new(info));
                true
            }
        });
    }
}

/// Returns the PSI section following the pointer field, if the packet starts one.
fn section(payload: &[u8], pusi: bool) -> Option<&[u8]> {
    if !pusi {
        return None;
    }
    let sec = payload.get(1 + *payload.first()? as usize..)?;
    (sec.len() >= 8).then_some(sec)
}

fn section_len(sec: &[u8]) -> usize {
    (((sec[1] & 0x0f) as usize) << 8) | sec[2] as usize
}

/// Extends a 33-bit PTS to a monotonically increasing 64-bit value.
fn unwrap_pts(last: u64, pts: u64) -> u64 {
    let half = 1u64 << 32;
    let mut c = (last & !PTS_MASK) | pts;
    if c + half < last {
        c += PTS_MASK + 1;
    } else if c > last + half && c > PTS_MASK {
        c -= PTS_MASK + 1;
    }
    c
}

/// Splits an Annex B byte stream into NAL units (without start codes).
fn split_nals(d: &[u8]) -> Vec<&[u8]> {
    let mut starts = Vec::new();
    let mut i = 0;
    while i + 3 <= d.len() {
        if d[i] == 0 && d[i + 1] == 0 && d[i + 2] == 1 {
            starts.push(i + 3);
            i += 3;
        } else {
            i += 1;
        }
    }
    let mut nals = Vec::new();
    for (k, &s) in starts.iter().enumerate() {
        let mut e = starts.get(k + 1).map_or(d.len(), |n| n - 3);
        while e > s && d[e - 1] == 0 {
            e -= 1;
        }
        if e > s {
            nals.push(&d[s..e]);
        }
    }
    nals
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_nals() {
        let d = [0, 0, 0, 1, 0x67, 1, 2, 0, 0, 1, 0x68, 3, 0, 0, 0, 1, 0x65, 4];
        let nals = split_nals(&d);
        assert_eq!(nals, vec![&[0x67, 1, 2][..], &[0x68, 3][..], &[0x65, 4][..]]);
    }

    fn pes(pts: Option<u64>, payload: &[u8]) -> Vec<u8> {
        let mut p = vec![0, 0, 1, 0xe0, 0, 0, 0x80];
        match pts {
            Some(t) => p.extend_from_slice(&[
                0x80,
                5,
                0x21 | (((t >> 30) & 7) << 1) as u8,
                (t >> 22) as u8,
                (((t >> 15) & 0x7f) << 1) as u8 | 1,
                (t >> 7) as u8,
                ((t & 0x7f) << 1) as u8 | 1,
            ]),
            None => p.extend_from_slice(&[0, 0]),
        }
        p.extend_from_slice(payload);
        p
    }

    #[test]
    fn reassembles_frame_split_over_pes_packets() {
        let (tx, _rx) = watch::channel(None);
        let mut d = Demuxer::new(tx);
        let mut out = Vec::new();
        let slice: Vec<u8> = (1..=200u8).collect();

        let first = [&[0, 0, 0, 1, 0x65][..], &slice[..100]].concat();
        d.finish(0x100, Kind::Video, &pes(Some(9000), &first), &mut out);
        d.finish(0x100, Kind::Video, &pes(None, &slice[100..]), &mut out);
        assert!(out.is_empty());
        d.finish(0x100, Kind::Video, &pes(Some(12000), &[0, 0, 0, 1, 0x41, 1]), &mut out);

        let [Frame::Video { pts, idr, nals }] = &out[..] else { panic!("expected one frame") };
        assert_eq!((*pts, *idr, nals.len()), (9000, true, 1));
        assert_eq!(&nals[0][..], &[&[0x65][..], &slice[..]].concat()[..]);
    }

    #[test]
    fn unwraps_pts() {
        let near_wrap = PTS_MASK - 10;
        assert_eq!(unwrap_pts(near_wrap, 5), PTS_MASK + 1 + 5);
        assert_eq!(unwrap_pts(PTS_MASK + 6, PTS_MASK - 2), PTS_MASK - 2);
    }
}
