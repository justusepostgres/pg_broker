//! Bounded Kafka protocol primitives and magic-2 batch validation.
use anyhow::{bail, ensure, Result};
pub const MAX_FRAME: usize = 8 * 1024 * 1024;
pub const MAX_BATCH: usize = 1024 * 1024;
pub const APIS: &[(i16, i16, i16)] = &[
    (0, 3, 3),
    (1, 4, 4),
    (2, 1, 1),
    (3, 0, 4),
    (8, 2, 2),
    (9, 1, 1),
    (10, 0, 0),
    (11, 0, 2),
    (12, 0, 1),
    (13, 0, 1),
    (14, 0, 1),
    (18, 0, 3),
    (19, 0, 0),
    (20, 0, 0),
];
pub struct Reader<'a> {
    data: &'a [u8],
    at: usize,
    items: usize,
}
impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            at: 0,
            items: 0,
        }
    }
    pub fn unsigned_varint(&mut self) -> Result<u32> {
        let mut value = 0u32;
        for shift in (0..35).step_by(7) {
            let byte = self.i8()? as u8;
            ensure!(shift < 28 || byte <= 15, "unsigned varint overflow");
            value |= ((byte & 127) as u32) << shift;
            if byte & 128 == 0 {
                return Ok(value);
            }
        }
        bail!("unsigned varint overflow")
    }
    pub fn compact_string(&mut self) -> Result<String> {
        let n = self.unsigned_varint()?;
        ensure!(n > 0 && n <= 32768, "compact string length");
        Ok(std::str::from_utf8(self.take((n - 1) as usize)?)?.to_owned())
    }
    pub fn tags(&mut self) -> Result<()> {
        let n = self.unsigned_varint()?;
        ensure!(n <= 128, "tag count");
        let mut previous = None;
        for _ in 0..n {
            let tag = self.unsigned_varint()?;
            ensure!(previous.is_none_or(|p| tag > p), "unordered tags");
            previous = Some(tag);
            let size = self.unsigned_varint()? as usize;
            self.take(size)?;
        }
        Ok(())
    }
    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        ensure!(n <= self.data.len() - self.at, "truncated frame");
        let b = &self.data[self.at..self.at + n];
        self.at += n;
        Ok(b)
    }
    pub fn i8(&mut self) -> Result<i8> {
        Ok(self.take(1)?[0] as i8)
    }
    pub fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into()?))
    }
    pub fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into()?))
    }
    pub fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into()?))
    }
    pub fn string(&mut self) -> Result<String> {
        let n = self.i16()?;
        ensure!(n >= 0, "null string");
        Ok(std::str::from_utf8(self.take(n as usize)?)?.to_owned())
    }
    pub fn nullable_string(&mut self) -> Result<Option<String>> {
        let n = self.i16()?;
        if n == -1 {
            return Ok(None);
        }
        ensure!(n >= 0, "bad string length");
        Ok(Some(
            std::str::from_utf8(self.take(n as usize)?)?.to_owned(),
        ))
    }
    pub fn bytes(&mut self) -> Result<Vec<u8>> {
        let n = self.i32()?;
        if n == -1 {
            return Ok(vec![]);
        }
        ensure!(n >= 0, "bad bytes length");
        Ok(self.take(n as usize)?.to_vec())
    }
    pub fn count(&mut self) -> Result<usize> {
        let n = self.i32()?;
        ensure!((0..=4096).contains(&n), "invalid array count");
        self.items += n as usize;
        ensure!(self.items <= 4096, "array item budget");
        Ok(n as usize)
    }
    pub fn nullable_count(&mut self) -> Result<Option<usize>> {
        let n = self.i32()?;
        if n == -1 {
            return Ok(None);
        }
        ensure!((0..=4096).contains(&n), "invalid array count");
        self.items += n as usize;
        ensure!(self.items <= 4096, "array item budget");
        Ok(Some(n as usize))
    }
    pub fn done(&self) -> Result<()> {
        ensure!(self.at == self.data.len(), "trailing bytes");
        Ok(())
    }
    fn varint(&mut self) -> Result<i64> {
        let mut n = 0u64;
        for i in 0..10 {
            let b = self.i8()? as u8;
            ensure!(i < 9 || b <= 1, "varint overflow");
            n |= ((b & 127) as u64) << (i * 7);
            if b & 128 == 0 {
                return Ok(((n >> 1) as i64) ^ -((n & 1) as i64));
            }
        }
        bail!("varint overflow")
    }
    fn varbytes(&mut self, nullable: bool) -> Result<()> {
        let n = self.varint()?;
        if nullable && n == -1 {
            return Ok(());
        }
        ensure!(n >= 0, "invalid field length");
        self.take(n as usize)?;
        Ok(())
    }
}
#[derive(Default)]
pub struct Writer(pub Vec<u8>);
impl Writer {
    pub fn unsigned_varint(&mut self, mut value: u32) {
        while value >= 128 {
            self.0.push((value as u8 & 127) | 128);
            value >>= 7;
        }
        self.0.push(value as u8);
    }
    pub fn i8(&mut self, n: i8) {
        self.0.push(n as u8)
    }
    pub fn i16(&mut self, n: i16) {
        self.0.extend(n.to_be_bytes())
    }
    pub fn i32(&mut self, n: i32) {
        self.0.extend(n.to_be_bytes())
    }
    pub fn i64(&mut self, n: i64) {
        self.0.extend(n.to_be_bytes())
    }
    pub fn string(&mut self, s: &str) {
        self.i16(s.len() as i16);
        self.0.extend(s.as_bytes())
    }
    pub fn null_string(&mut self) {
        self.i16(-1)
    }
    pub fn bytes(&mut self, b: &[u8]) {
        self.i32(b.len() as i32);
        self.0.extend(b)
    }
    pub fn array(&mut self, n: usize) {
        self.i32(n as i32)
    }

    /// Frame a response once so its length prefix and payload can share a write.
    pub fn into_response_frame(self, correlation: i32) -> Result<Vec<u8>> {
        let length = self
            .0
            .len()
            .checked_add(4)
            .ok_or_else(|| anyhow::anyhow!("response size overflow"))?;
        ensure!(length <= MAX_FRAME, "response size limit");
        let mut frame = Vec::with_capacity(length + 4);
        frame.extend_from_slice(&(length as i32).to_be_bytes());
        frame.extend_from_slice(&correlation.to_be_bytes());
        frame.extend_from_slice(&self.0);
        Ok(frame)
    }
}
/// Return validated producer timestamps in offset order. Compression, control records,
/// transactions and producer IDs are deliberately unsupported.
pub fn validate_batch(b: &[u8]) -> Result<Vec<i64>> {
    ensure!((61..=MAX_BATCH).contains(&b.len()), "invalid batch size");
    let mut r = Reader::new(b);
    r.i64()?;
    let len = r.i32()?;
    ensure!(len as usize + 12 == b.len(), "one batch required");
    r.i32()?;
    ensure!(r.i8()? == 2, "magic 2 required");
    let crc = r.i32()? as u32;
    ensure!(crc32c::crc32c(&b[21..]) == crc, "invalid CRC");
    let attrs = r.i16()?;
    ensure!(attrs == 0, "unsupported batch attributes");
    let last = r.i32()?;
    let first_ts = r.i64()?;
    let max_ts = r.i64()?;
    ensure!(r.i64()? == -1, "idempotence unsupported");
    r.i16()?;
    r.i32()?;
    let count = r.i32()?;
    ensure!(
        (1..=10000).contains(&count) && last == count - 1,
        "invalid offset delta"
    );
    let mut observed = first_ts;
    let mut timestamps = Vec::with_capacity(count as usize);
    for offset in 0..count {
        let len = r.varint()?;
        ensure!(len >= 0, "record length");
        let mut rec = Reader::new(r.take(len as usize)?);
        ensure!(rec.i8()? == 0, "record attributes");
        let ts = first_ts
            .checked_add(rec.varint()?)
            .ok_or_else(|| anyhow::anyhow!("timestamp overflow"))?;
        observed = observed.max(ts);
        timestamps.push(ts);
        ensure!(rec.varint()? == offset as i64, "noncontiguous batch");
        rec.varbytes(true)?;
        rec.varbytes(true)?;
        let n = rec.varint()?;
        ensure!((0..=1024).contains(&n), "headers limit");
        for _ in 0..n {
            rec.varbytes(false)?;
            rec.varbytes(true)?
        }
        rec.done()?;
    }
    r.done()?;
    ensure!(observed == max_ts, "invalid maximum timestamp");
    Ok(timestamps)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn malformed_frames() {
        assert!(Reader::new(&[0]).i32().is_err());
        assert!(Reader::new(&(-2i32).to_be_bytes()).count().is_err());
        assert!(validate_batch(&[0; 61]).is_err());
    }

    #[test]
    fn response_frame_preserves_prefix_correlation_and_body() {
        let mut body = Writer::default();
        body.i16(7);
        assert_eq!(
            body.into_response_frame(-1).unwrap(),
            vec![0, 0, 0, 6, 255, 255, 255, 255, 0, 7]
        );
        assert_eq!(
            Writer::default().into_response_frame(9).unwrap(),
            vec![0, 0, 0, 4, 0, 0, 0, 9]
        );
    }

    #[test]
    fn response_frame_enforces_limit_including_correlation() {
        let mut body = Writer::default();
        body.0.resize(MAX_FRAME - 4, 0);
        let frame = body.into_response_frame(1).unwrap();
        assert_eq!(frame.len(), MAX_FRAME + 4);
        assert_eq!(
            i32::from_be_bytes(frame[..4].try_into().unwrap()) as usize,
            MAX_FRAME
        );
        let mut oversized = Writer::default();
        oversized.0.resize(MAX_FRAME - 3, 0);
        assert!(oversized.into_response_frame(1).is_err());
    }
}

/// Validate a complete partition record set before making any durable changes.
pub fn batches(data: &[u8]) -> Result<Vec<(&[u8], Vec<i64>)>> {
    ensure!(
        !data.is_empty() && data.len() <= MAX_BATCH,
        "record set size"
    );
    let mut r = Reader::new(data);
    let mut result = Vec::new();
    while r.at < data.len() {
        let start = r.at;
        r.i64()?;
        let n = r.i32()?;
        ensure!(n >= 49, "batch length");
        r.take(n as usize)?;
        let batch = &data[start..r.at];
        result.push((batch, validate_batch(batch)?));
    }
    Ok(result)
}
