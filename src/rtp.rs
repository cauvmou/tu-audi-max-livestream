//! RTP packetization: H.264 (RFC 6184, mode 1) and AAC (RFC 3640, AAC-hbr).

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

pub const VIDEO_PT: u8 = 96;
pub const AUDIO_PT: u8 = 97;
const MTU: usize = 1200;

const CNAME: &[u8] = b"tu-audi-max@localhost";

pub struct RtpWriter {
    pt: u8,
    ssrc: u32,
    seq: u16,
    clock: u32,
    packets: u32,
    octets: u32,
    last: Option<(u32, Instant)>,
}

impl RtpWriter {
    pub fn new(pt: u8, clock: u32) -> Self {
        Self {
            pt,
            ssrc: rand::random(),
            seq: rand::random(),
            clock,
            packets: 0,
            octets: 0,
            last: None,
        }
    }

    pub fn packets(&self) -> u32 {
        self.packets
    }

    pub fn packet(&mut self, ts: u32, marker: bool, payload: &[u8]) -> Vec<u8> {
        let mut p = Vec::with_capacity(12 + payload.len());
        p.push(0x80);
        p.push(self.pt | if marker { 0x80 } else { 0 });
        p.extend_from_slice(&self.seq.to_be_bytes());
        p.extend_from_slice(&ts.to_be_bytes());
        p.extend_from_slice(&self.ssrc.to_be_bytes());
        p.extend_from_slice(payload);
        self.seq = self.seq.wrapping_add(1);
        self.packets = self.packets.wrapping_add(1);
        self.octets = self.octets.wrapping_add(payload.len() as u32);
        self.last = Some((ts, Instant::now()));
        p
    }

    /// RTCP sender report + SDES (CNAME) relating wall-clock time to the RTP timestamp.
    /// `None` until the first packet has been sent.
    pub fn sender_report(&self) -> Option<Vec<u8>> {
        let (ts, at) = self.last?;
        let rtp_ts = ts.wrapping_add((at.elapsed().as_secs_f64() * self.clock as f64) as u32);
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
        let secs = (now.as_secs() + 2_208_988_800) as u32;
        let frac = ((now.subsec_nanos() as u64) << 32) / 1_000_000_000;

        let mut p = vec![0x80, 200, 0, 6];
        p.extend_from_slice(&self.ssrc.to_be_bytes());
        p.extend_from_slice(&secs.to_be_bytes());
        p.extend_from_slice(&(frac as u32).to_be_bytes());
        p.extend_from_slice(&rtp_ts.to_be_bytes());
        p.extend_from_slice(&self.packets.to_be_bytes());
        p.extend_from_slice(&self.octets.to_be_bytes());

        let chunk = (4 + 2 + CNAME.len() + 1).next_multiple_of(4);
        p.extend_from_slice(&[0x81, 202]);
        p.extend_from_slice(&((chunk / 4) as u16).to_be_bytes());
        p.extend_from_slice(&self.ssrc.to_be_bytes());
        p.extend_from_slice(&[1, CNAME.len() as u8]);
        p.extend_from_slice(CNAME);
        p.resize(p.len() + chunk - (6 + CNAME.len()), 0);
        Some(p)
    }
}

/// Payloads for one access unit, each with its marker bit (set on the last packet).
pub fn h264_payloads(nals: &[Bytes]) -> Vec<(Vec<u8>, bool)> {
    let mut out = Vec::new();
    for (k, nal) in nals.iter().enumerate() {
        let last_nal = k == nals.len() - 1;
        if nal.len() <= MTU {
            out.push((nal.to_vec(), last_nal));
            continue;
        }
        // FU-A fragmentation
        let indicator = (nal[0] & 0xe0) | 28;
        let nal_type = nal[0] & 0x1f;
        let chunks: Vec<&[u8]> = nal[1..].chunks(MTU - 2).collect();
        for (j, chunk) in chunks.iter().enumerate() {
            let start = j == 0;
            let end = j == chunks.len() - 1;
            let header = ((start as u8) << 7) | ((end as u8) << 6) | nal_type;
            let mut p = Vec::with_capacity(2 + chunk.len());
            p.extend_from_slice(&[indicator, header]);
            p.extend_from_slice(chunk);
            out.push((p, last_nal && end));
        }
    }
    out
}

/// One AAC access unit with its AU-header section.
pub fn aac_payload(au: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(4 + au.len());
    p.extend_from_slice(&16u16.to_be_bytes());
    p.extend_from_slice(&((au.len() as u16) << 3).to_be_bytes());
    p.extend_from_slice(au);
    p
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fu_a_roundtrip() {
        let mut nal = vec![0x65];
        nal.extend((0..3000).map(|i| i as u8));
        let payloads = h264_payloads(&[Bytes::from(nal.clone())]);
        assert!(payloads.len() > 1);
        assert!(payloads.iter().all(|(p, _)| p.len() <= MTU));
        assert_eq!(payloads.iter().filter(|(_, m)| *m).count(), 1);
        assert!(payloads.last().unwrap().1);

        let mut rebuilt = vec![(payloads[0].0[0] & 0xe0) | (payloads[0].0[1] & 0x1f)];
        for (p, _) in &payloads {
            rebuilt.extend_from_slice(&p[2..]);
        }
        assert_eq!(rebuilt, nal);
    }

    #[test]
    fn sender_report_layout() {
        let mut w = RtpWriter::new(VIDEO_PT, 90_000);
        assert!(w.sender_report().is_none());
        w.packet(1000, true, &[0; 10]);
        let sr = w.sender_report().unwrap();
        assert_eq!(sr.len() % 4, 0);
        // SR: 4 header + 24 body bytes; SDES follows
        assert_eq!(u16::from_be_bytes([sr[2], sr[3]]) as usize, 6);
        assert_eq!(sr[28 + 1], 202);
        let sdes_words = u16::from_be_bytes([sr[30], sr[31]]) as usize;
        assert_eq!(sr.len(), 28 + 4 + sdes_words * 4);
    }

    #[test]
    fn aac_header() {
        let p = aac_payload(&[1, 2, 3]);
        assert_eq!(p, vec![0, 16, 0, 3 << 3, 1, 2, 3]);
    }
}
