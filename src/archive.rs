use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};

const MAGIC: &[u8; 8] = b"FRANSSON";
const VERSION: u16 = 2;
const ZSTD_LEVEL: i32 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveHeader {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveRecord {
    pub timestamp: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
    pub headers: Vec<ArchiveHeader>,
}

pub struct ArchiveWriter {
    encoder: zstd::stream::write::Encoder<'static, BufWriter<File>>,
    partitions: Vec<i32>,
    next_partition: usize,
    partition_open: bool,
}

impl ArchiveWriter {
    pub fn create(path: &Path, partitions: &[i32]) -> Result<Self> {
        validate_partitions(partitions)?;
        let mut file = BufWriter::new(
            File::create(path)
                .with_context(|| format!("failed to create archive {}", path.display()))?,
        );
        file.write_all(MAGIC)?;
        file.write_all(&VERSION.to_le_bytes())?;

        let mut encoder = zstd::stream::write::Encoder::new(file, ZSTD_LEVEL)
            .context("failed to initialize zstd archive encoder")?;
        encoder.include_checksum(true)?;
        write_u32(
            &mut encoder,
            usize_to_u32(partitions.len(), "partition count")?,
        )?;
        for partition in partitions {
            write_i32(&mut encoder, *partition)?;
        }

        Ok(Self {
            encoder,
            partitions: partitions.to_vec(),
            next_partition: 0,
            partition_open: false,
        })
    }

    pub fn begin_partition(&mut self, partition: i32) -> Result<()> {
        if self.partition_open {
            bail!("cannot begin partition {partition} before ending the current partition");
        }
        let expected = self
            .partitions
            .get(self.next_partition)
            .copied()
            .context("archive contains more partition blocks than declared")?;
        if partition != expected {
            bail!("expected archive partition {expected}, got {partition}");
        }
        self.partition_open = true;
        Ok(())
    }

    pub fn write_record(&mut self, record: &ArchiveRecord) -> Result<()> {
        if !self.partition_open {
            bail!("cannot write an archive record outside a partition");
        }
        write_u8(&mut self.encoder, 1)?;
        write_optional_i64(&mut self.encoder, record.timestamp)?;
        write_optional_bytes(&mut self.encoder, record.key.as_deref())?;
        write_optional_bytes(&mut self.encoder, record.payload.as_deref())?;
        write_u32(
            &mut self.encoder,
            usize_to_u32(record.headers.len(), "header count")?,
        )?;
        for header in &record.headers {
            write_bytes(&mut self.encoder, header.key.as_bytes())?;
            write_optional_bytes(&mut self.encoder, header.value.as_deref())?;
        }
        Ok(())
    }

    pub fn end_partition(&mut self) -> Result<()> {
        if !self.partition_open {
            bail!("cannot end an archive partition before beginning it");
        }
        write_u8(&mut self.encoder, 0)?;
        self.partition_open = false;
        self.next_partition += 1;
        Ok(())
    }

    pub fn finish(self) -> Result<()> {
        if self.partition_open {
            bail!("cannot finish archive with an open partition");
        }
        if self.next_partition != self.partitions.len() {
            bail!(
                "archive is missing {} partition blocks",
                self.partitions.len() - self.next_partition
            );
        }
        let mut output = self
            .encoder
            .finish()
            .context("failed to finish zstd archive stream")?;
        output.flush()?;
        output.get_ref().sync_all()?;
        Ok(())
    }
}

pub enum ArchiveEvent {
    PartitionStart(i32),
    Record(ArchiveRecord),
    PartitionEnd(i32),
}

pub struct ArchiveReader {
    decoder: zstd::stream::read::Decoder<'static, BufReader<File>>,
    partitions: Vec<i32>,
    next_partition: usize,
    partition_open: bool,
    finished: bool,
}

impl ArchiveReader {
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path)
            .with_context(|| format!("failed to open archive {}", path.display()))?;
        let mut magic = [0_u8; MAGIC.len()];
        file.read_exact(&mut magic)
            .with_context(|| format!("failed to read archive header from {}", path.display()))?;
        if &magic != MAGIC {
            bail!("{} is not a fransson archive", path.display());
        }
        let version = read_u16(&mut file)?;
        if version != VERSION {
            bail!("unsupported fransson archive version {version}; expected {VERSION}");
        }

        let mut decoder = zstd::stream::read::Decoder::new(file)
            .context("failed to initialize zstd archive decoder")?;
        let partition_count = u32_to_usize(read_u32(&mut decoder)?)?;
        let mut partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            partitions.push(read_i32(&mut decoder)?);
        }
        validate_partitions(&partitions)?;

        Ok(Self {
            decoder,
            partitions,
            next_partition: 0,
            partition_open: false,
            finished: false,
        })
    }

    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }

    pub fn next_event(&mut self) -> Result<Option<ArchiveEvent>> {
        if self.finished {
            return Ok(None);
        }
        if !self.partition_open {
            if self.next_partition == self.partitions.len() {
                let mut trailing = [0_u8; 1];
                match self.decoder.read(&mut trailing) {
                    Ok(0) => {
                        self.finished = true;
                        return Ok(None);
                    }
                    Ok(_) => bail!("archive has trailing decompressed data"),
                    Err(err) => return Err(err).context("failed to finish reading archive"),
                }
            }
            self.partition_open = true;
            return Ok(Some(ArchiveEvent::PartitionStart(
                self.partitions[self.next_partition],
            )));
        }

        match read_u8(&mut self.decoder).context("failed to read archive record marker")? {
            0 => {
                let partition = self.partitions[self.next_partition];
                self.partition_open = false;
                self.next_partition += 1;
                Ok(Some(ArchiveEvent::PartitionEnd(partition)))
            }
            1 => Ok(Some(ArchiveEvent::Record(read_record(&mut self.decoder)?))),
            marker => bail!("invalid archive record marker {marker}"),
        }
    }
}

pub fn fingerprint(path: &Path) -> Result<String> {
    let mut input = BufReader::new(
        File::open(path).with_context(|| format!("failed to open archive {}", path.display()))?,
    );
    let mut hasher = Sha256::new();
    io::copy(&mut input, &mut hasher).context("failed to fingerprint archive")?;
    let digest = hasher.finalize();
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub fn format_version() -> u16 {
    VERSION
}

fn read_record(reader: &mut impl Read) -> Result<ArchiveRecord> {
    let timestamp = read_optional_i64(reader)?;
    let key = read_optional_bytes(reader)?;
    let payload = read_optional_bytes(reader)?;
    let header_count = u32_to_usize(read_u32(reader)?)?;
    let mut headers = Vec::with_capacity(header_count);
    for _ in 0..header_count {
        let key =
            String::from_utf8(read_bytes(reader)?).context("archive header key is not UTF-8")?;
        let value = read_optional_bytes(reader)?;
        headers.push(ArchiveHeader { key, value });
    }
    Ok(ArchiveRecord {
        timestamp,
        key,
        payload,
        headers,
    })
}

fn validate_partitions(partitions: &[i32]) -> Result<()> {
    for (expected, actual) in partitions.iter().enumerate() {
        let expected = i32::try_from(expected).context("too many archive partitions")?;
        if *actual != expected {
            bail!(
                "archive partitions must be contiguous from 0; expected {expected}, got {actual}"
            );
        }
    }
    Ok(())
}

fn write_optional_i64(writer: &mut impl Write, value: Option<i64>) -> Result<()> {
    match value {
        Some(value) => {
            write_u8(writer, 1)?;
            writer.write_all(&value.to_le_bytes())?;
        }
        None => write_u8(writer, 0)?,
    }
    Ok(())
}

fn read_optional_i64(reader: &mut impl Read) -> Result<Option<i64>> {
    match read_u8(reader)? {
        0 => Ok(None),
        1 => {
            let mut bytes = [0_u8; 8];
            reader.read_exact(&mut bytes)?;
            Ok(Some(i64::from_le_bytes(bytes)))
        }
        marker => bail!("invalid optional timestamp marker {marker}"),
    }
}

fn write_optional_bytes(writer: &mut impl Write, value: Option<&[u8]>) -> Result<()> {
    match value {
        Some(value) => {
            write_u8(writer, 1)?;
            write_bytes(writer, value)?;
        }
        None => write_u8(writer, 0)?,
    }
    Ok(())
}

fn read_optional_bytes(reader: &mut impl Read) -> Result<Option<Vec<u8>>> {
    match read_u8(reader)? {
        0 => Ok(None),
        1 => Ok(Some(read_bytes(reader)?)),
        marker => bail!("invalid optional bytes marker {marker}"),
    }
}

fn write_bytes(writer: &mut impl Write, value: &[u8]) -> Result<()> {
    write_u64(writer, usize_to_u64(value.len())?)?;
    writer.write_all(value)?;
    Ok(())
}

fn read_bytes(reader: &mut impl Read) -> Result<Vec<u8>> {
    let len = u64_to_usize(read_u64(reader)?)?;
    let mut value = vec![0_u8; len];
    reader.read_exact(&mut value)?;
    Ok(value)
}

fn write_u8(writer: &mut impl Write, value: u8) -> Result<()> {
    writer.write_all(&[value])?;
    Ok(())
}

fn read_u8(reader: &mut impl Read) -> Result<u8> {
    let mut bytes = [0_u8; 1];
    reader.read_exact(&mut bytes)?;
    Ok(bytes[0])
}

fn write_u32(writer: &mut impl Write, value: u32) -> Result<()> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn read_u16(reader: &mut impl Read) -> Result<u16> {
    let mut bytes = [0_u8; 2];
    reader.read_exact(&mut bytes)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u32(reader: &mut impl Read) -> Result<u32> {
    let mut bytes = [0_u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn write_i32(writer: &mut impl Write, value: i32) -> Result<()> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn read_i32(reader: &mut impl Read) -> Result<i32> {
    let mut bytes = [0_u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(i32::from_le_bytes(bytes))
}

fn write_u64(writer: &mut impl Write, value: u64) -> Result<()> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn read_u64(reader: &mut impl Read) -> Result<u64> {
    let mut bytes = [0_u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn usize_to_u32(value: usize, what: &str) -> Result<u32> {
    u32::try_from(value).with_context(|| format!("{what} exceeds archive format limit"))
}

fn usize_to_u64(value: usize) -> Result<u64> {
    u64::try_from(value).context("value length exceeds archive format limit")
}

fn u32_to_usize(value: u32) -> Result<usize> {
    usize::try_from(value).context("archive count does not fit this platform")
}

fn u64_to_usize(value: u64) -> Result<usize> {
    usize::try_from(value).context("archive value length does not fit this platform")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn records() -> Vec<Vec<ArchiveRecord>> {
        vec![
            vec![ArchiveRecord {
                timestamp: Some(42),
                key: Some(Vec::new()),
                payload: None,
                headers: vec![
                    ArchiveHeader {
                        key: "x".to_owned(),
                        value: None,
                    },
                    ArchiveHeader {
                        key: "x".to_owned(),
                        value: Some(vec![0, 1, 2]),
                    },
                ],
            }],
            Vec::new(),
        ]
    }

    fn write_fixture(path: &Path) {
        let input = records();
        let mut writer = ArchiveWriter::create(path, &[0, 1]).unwrap();
        for (partition, records) in input.iter().enumerate() {
            writer.begin_partition(partition as i32).unwrap();
            for record in records {
                writer.write_record(record).unwrap();
            }
            writer.end_partition().unwrap();
        }
        writer.finish().unwrap();
    }

    #[test]
    fn encoding_is_deterministic_and_round_trips() {
        let dir = std::env::temp_dir();
        let first = dir.join(format!("fransson-archive-{}-1", std::process::id()));
        let second = dir.join(format!("fransson-archive-{}-2", std::process::id()));
        write_fixture(&first);
        write_fixture(&second);
        assert_eq!(fs::read(&first).unwrap(), fs::read(&second).unwrap());

        let mut reader = ArchiveReader::open(&first).unwrap();
        assert_eq!(reader.partitions(), &[0, 1]);
        let mut decoded = vec![Vec::new(), Vec::new()];
        let mut partition = None;
        while let Some(event) = reader.next_event().unwrap() {
            match event {
                ArchiveEvent::PartitionStart(id) => partition = Some(id),
                ArchiveEvent::Record(record) => decoded[partition.unwrap() as usize].push(record),
                ArchiveEvent::PartitionEnd(id) => {
                    assert_eq!(partition.take(), Some(id));
                }
            }
        }
        assert_eq!(decoded, records());
        assert_eq!(fingerprint(&first).unwrap(), fingerprint(&second).unwrap());
        let _ = fs::remove_file(first);
        let _ = fs::remove_file(second);
    }

    #[test]
    fn reader_accepts_only_the_current_format() {
        let path = std::env::temp_dir().join(format!(
            "fransson-archive-{}-unsupported",
            std::process::id()
        ));
        write_fixture(&path);
        let mut bytes = fs::read(&path).unwrap();
        bytes[MAGIC.len()..MAGIC.len() + 2].copy_from_slice(&(VERSION - 1).to_le_bytes());
        fs::write(&path, bytes).unwrap();
        let error = match ArchiveReader::open(&path) {
            Ok(_) => panic!("unsupported archive unexpectedly opened"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("unsupported fransson archive version"));
        let _ = fs::remove_file(path);
    }
}
