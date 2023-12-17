/*
Module to help split files into digestible chunks for mapreduce.
*/
use std::fs::File;
use std::io::*;


#[derive(Debug)]
pub struct FileChunk {
    source: String,
    path: String,
    index: usize,
}

/// Context object to use within the chunker. It represents the state of chunking for
/// a single chunk of a single file.
/// 
pub struct ChunkerContext {
    source: String,
    path: String,
    chunk_idx: usize,
    line_i: usize,
    byte_i: usize,
    writer: LineWriter<File>,
}

impl ChunkerContext {

    /// Builds a `FileChunk` from the context
    fn build_chunk(&self) -> FileChunk {
        FileChunk {
            source: self.source.clone(),
            path: self.path.clone(),
            index: self.chunk_idx,
        }
    }
}

/// Creates the initial chunker context object.
/// 
/// Chunk files are named `{out}/{chunk_idx}`.
/// 
/// # Example
/// 
/// ```rust
/// let c = build_chunker_context("/home/big-file.txt", "/tmp/big-file.chunks/", 0);
/// ```
fn build_chunker_context(source: &str, out: &str, chunk_idx: usize) -> ChunkerContext {
    let chunk_path = format!(
        "{}/{}",
        match out.strip_suffix("/") {
            None => {
                match out.ends_with("/") {
                    true => panic!("couldn't strip suffix from {}", out),
                    false => out
                }
            },
            Some(v) => v
        },
        chunk_idx,
    );
    let chunk_file = match File::create(&chunk_path) {
        Err(_why) => panic!("couldn't create {}: {}", chunk_path, _why),
        Ok(chunk_file) => chunk_file
    };
    ChunkerContext {
        source: source.to_string(),
        path: chunk_path.clone(),
        chunk_idx: chunk_idx,
        line_i: 0,
        byte_i: 0,
        writer: LineWriter::new(chunk_file)
    }
}

/// A trait for implementing Chunkers. Must be able to chunk a file into smaller files
pub trait Chunker {
    fn chunk(&self, path: &str, out: &str, chunks: &mut Vec<FileChunk>);
}

/// A chunker that divides a file into equitable line counts.
pub struct LineChunker {
    pub max_lines: usize
}

#[allow(private_interfaces)]
impl Chunker for LineChunker {


    /// Chunks a file into smaller files with no more than `max_lines` per chunk.
    ///
    /// # Example
    /// 
    /// ```rust
    /// use chunker;
    /// let src = String::from("/home/file.txt");
    /// let dir = String::from("/tmp/file.txt.chunks/");
    /// let mut chunks: Vec<chunker::FileChunk> = Vec::new();
    /// let chnkr: chunker::LineChunker = LineChunker{max_lines: 100};
    /// chnkr.chunk(&src, &dir, 150_000, &mut chunks);
    /// ```
    fn chunk(&self, path: &str, out: &str, chunks: &mut Vec<FileChunk>) {
        let file = match File::open(&path) {
            Err(_why) => panic!("couldn't open {}: {}", path, _why),
            Ok(file) => file,
        };
        let file = BufReader::new(file);
        let mut lines = file.lines();
        let mut chunk_idx: usize = 0;

        'chunk_loop: loop {
            // Running this routine for each chunk. Iterates over `lines` and saves it to the chunk
            // until the max is hit or the source file is exhausted
            let mut ctx: ChunkerContext = build_chunker_context(path, out, chunk_idx);
            while ctx.line_i < self.max_lines {
                // Get the next line. If None, the buffer is exhausted and loop should be terminated
                let line = match lines.next() {
                    None => {
                        // Add the chunk before exiting loop scope if there's anything in it
                        if ctx.byte_i > 0 {
                            chunks.push(ctx.build_chunk());
                        };
                        break 'chunk_loop
                    },
                    Some(line) => {
                        match line {
                            Err(_why) => panic!("couldn't read line {}: {}", ctx.line_i, _why),
                            Ok(v) => v
                        }
                    },
                };

                // Write the line and increment
                ctx.writer.write_all(line.as_bytes()).expect(format!("Failed to write line {} to {}", ctx.line_i, ctx.path).as_str());
                ctx.writer.write_all(b"\n").expect(format!("Failed to write linebreak at {} to {}", ctx.line_i, ctx.path).as_str());
                ctx.line_i += 1;
                ctx.byte_i += line.len() + 1;
            }
            ctx.writer.flush().expect(format!("Failed to flush {}", ctx.path).as_str());
            // Add the completed chunk
            chunks.push(ctx.build_chunk());
            chunk_idx += 1;
        }
    }

}

/// A chunker that divides a file into near-equitable sizes but never
/// breaks a contiguous word. A chunk can only end with a whitespace
/// character.
pub struct SizeBySpaceChunker {
    pub max_bytes: usize
}

impl Chunker for SizeBySpaceChunker {

    /// Splits a file into chunks of sizes no larger than `max_bytes` and along whitespace.
    /// 
    /// # Example
    /// ```rust
    /// use chunker::{FileChunk, SizeBySpaceChunker};
    /// let src = String::from("/home/file.txt");
    /// let dir = String::from("/tmp/file.txt.chunks/");
    /// let mut chunks: Vec<FileChunk> = Vec::new();
    /// let chnkr: SizeBySpaceChunker = SizeBySpaceChunker{max_bytes: 2 ^ 20};
    /// chnkr.chunk(&src, &dir, 150_000, &mut chunks);
    /// ```
    fn chunk(&self, path: &str, out: &str, chunks: &mut Vec<FileChunk>) {
        let file = File::open(path).expect(format!("Unable to read {}", path).as_str());
        let mut reader = BufReader::new(file);
        let mut writer: BufWriter<File>;

        let mut remainder: Vec<u8> = Vec::with_capacity(self.max_bytes);
        let mut buf = vec![0u8; self.max_bytes];
        let mut byte_i: usize;
        let mut ctx = build_chunker_context(path, out, 0);
        loop {
            // If there's a remainder from the last iter, the buffer should only read in
            // a limited amount (`right`). If there is no remainder, left will be empty.
            let (left, right) = buf.split_at_mut(remainder.len());

            // If there is a remainder from the last iteration, fill the left array with it
            // and clear the remainder vector
            if !remainder.is_empty() {
                left.copy_from_slice(remainder.as_slice());
                remainder.clear();
            }

            // Read values into the remaining allocated array space
            let count = match reader.read(right) {
                Err(_why) => panic!("{} | Failed to read bytes", path),
                Ok(v) => v                
            };

            // When not having read EOF, this should equal the max size
            // The exception is when the source byte count is perfectly divisible by the chunk
            // size.
            let non_empty_len = left.len() + count;
            if non_empty_len == 0 {break}

            byte_i = non_empty_len - 1;
            // Decrement until a non-whitespace character is reached
            while !buf[byte_i].is_ascii_whitespace() {byte_i -= 1;}

            // If the byte index was walked back, add the excluded values to the remainder
            // so they are handled in the next iteration. Exclude the whitespace from the
            // remainder to keep chunk size as close to max as possible
            if byte_i < non_empty_len - 1 {
                remainder.append(&mut buf[byte_i+1..non_empty_len].to_vec());
            }

            // Create the chunk
            writer = BufWriter::new(
                match File::create(&ctx.path) {
                    Err(_why) => panic!("couldn't create {}: {}", ctx.path, _why),
                    Ok(chunk_file) => chunk_file
                }
            );
            writer.write(&buf[..byte_i+1]).expect("Failed to write chunk");
            writer.flush().expect("Failed to write chunk");
            chunks.push(ctx.build_chunk());

            // If the remainder is empty and the previously written bytes didn't add up
            // to the total buffer size, we are at the end of the file and are done
            // chunking
            if non_empty_len < self.max_bytes {break};
            // Reset, creating the next context
            ctx = build_chunker_context(path, out, ctx.chunk_idx + 1);
            buf.fill(0);
        }

    }
}

#[allow(dead_code)]
impl FileChunk {

    pub fn true_line_count(&self) -> usize {
        let lines = BufReader::new(self.open()).lines();
        lines.count()
    }

    /// Opens the file and returns it
    pub fn open(&self) -> File {
        match File::open(&self.path) {
            Err(_why) => panic!("couldn't open {}: {}", self.path, _why),
            Ok(file) => file,
        }
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use rand::{thread_rng, seq::SliceRandom};
    use rstest::{fixture, rstest};
    use tempdir::TempDir;

    #[fixture]
    fn tmp() -> TempDir {
        match TempDir::new("mit-6.824") {
            Err(_why) => panic!("Couldn't create test dir"),
            Ok(v) => v
        }
    }

    fn many_line_file(tmp: &TempDir, line_count: usize) -> String {
        // Creating a randomly named temp file in the tmp dir
        let mut rng = thread_rng();
        let mut digits = ["0","1","2","3","4","5","6","7","8","9"];
        digits.shuffle(&mut rng);

        let path = match tmp.path().join(digits.join("")).to_str() {
            None => panic!("Couldn't create the temp file"),
            Some(v) => v
        }.to_string();
        let f = File::create(&path).expect(format!("Unable to create file '{}'", path.as_str()).as_str());
        let mut writer = BufWriter::new(f);
        for _ in 0..line_count {
            writer.write(b"1\n").expect("Failed to write test line");
        }
        writer.flush().expect("Failed to write line buffer to file");
        path
    }

    /// Tests the line chunker for when the number of lines in the source file is
    /// divisible by the max number of lines per chunk.
    #[rstest]
    fn test_chunk_by_line_count_even_split(tmp: TempDir) {
        const LINES_IN_TEST_FILE: usize = 10_000;
        let path = many_line_file(&tmp, LINES_IN_TEST_FILE);

        let mut chunks: Vec<FileChunk> = Vec::new();
        let chunker = LineChunker {max_lines: 1000};
        chunker.chunk(
            &path,
            tmp.path().to_str().expect("Failed to convert tempdir path to string"),
            &mut chunks
        );

        // Assert 
        // 1. Number of chunks
        // 2. Reported lines per chunk
        // 3. True lines per chunk
        const EXPECTED_LINES_PER_FILE: usize = 1000;
        assert!(chunks.len() == 10);
        for chnk in chunks.iter() {
            assert!(chnk.true_line_count() == EXPECTED_LINES_PER_FILE);
        }
        tmp.close().expect("Failed to close the expected directory.");
    }

    /// Tests the line chunker for when the number of lines in the source file is
    /// not divisible into equitable chunks, leaving the last chunk with fewer than
    /// `max_lines` lines.
    #[rstest]
    fn test_chunk_by_line_count_with_remainder(tmp: TempDir) {
        const LINES_IN_TEST_FILE: usize = 10_500;
        let path = many_line_file(&tmp, LINES_IN_TEST_FILE);

        let mut chunks: Vec<FileChunk> = Vec::new();
        let chunker = LineChunker {max_lines: 1000};
        chunker.chunk(
            &path,
            tmp.path().to_str().expect("Failed to convert tempdir path to string"),
            &mut chunks
        );

        // Assert
        // 1. Number of chunks
        // 2. Reported lines per chunk
        // 3. True lines per chunk
        const EXPECTED_LINES_PER_FILE: usize = 1000;
        const REMAINDER_LINES_PER_FILE: usize = 500;
        let mut lines_per_file;
        assert!(chunks.len() == 11);
        for (i, chnk) in chunks.iter().enumerate() {
            match i < (chunks.len() - 1) {
                true => {lines_per_file = EXPECTED_LINES_PER_FILE},
                false => {lines_per_file = REMAINDER_LINES_PER_FILE}
            }
            assert!(chnk.true_line_count() == lines_per_file);
        }
        tmp.close().expect("Failed to close the expected directory.");
    }

    fn many_byte_file(tmp: &TempDir, bytes: usize) -> String {
        // Creating a randomly named temp file in the tmp dir
        let mut rng = thread_rng();
        let mut digits = ["0","1","2","3","4","5","6","7","8","9"];
        digits.shuffle(&mut rng);

        let path = match tmp.path().join(digits.join("")).to_str() {
            None => panic!("Couldn't create the temp file"),
            Some(v) => v
        }.to_string();
        let f = File::create(&path).expect(format!("Unable to create file '{}'", path.as_str()).as_str());
        let mut writer = BufWriter::new(f);
        for i in 0..(bytes/2) {
            writer.write(format!("{}\n", i % 9).as_bytes()).expect("Failed to write test line");
        }
        writer.flush().expect("Failed to write line buffer to file");
        path
    }

    #[rstest]
    fn test_chunk_by_size_whitespace_count_even_split(tmp: TempDir) {
        const MULT_2: usize = 32;
        const BYTES_IN_TEST_FILE: usize = MULT_2 * 2;
        let path = many_byte_file(&tmp, BYTES_IN_TEST_FILE);
        println!("{}", path);
        let mut chunks: Vec<FileChunk> = Vec::new();
        // Up to 32 mebi
        let chunker = SizeBySpaceChunker {max_bytes: MULT_2};
        chunker.chunk(
            &path,
            tmp.path().to_str().expect("Failed to convert tempdir path to string"),
            &mut chunks
        );
        assert!(chunks.len() == 2);
        let mut total: usize = 0;
        for chnk in chunks.iter() {
            let mut buf = BufReader::new(chnk.open());
            let mut bytes = vec![0u8; BYTES_IN_TEST_FILE];
            let u = buf.read(&mut bytes).expect("Failed to read");
            total += u;
            assert!(u == MULT_2);
        }
        assert!(total == BYTES_IN_TEST_FILE);
        tmp.close().expect("Failed to close the expected directory.");
    }

    #[rstest]
    fn test_chunk_by_size_whitespace_count_with_remainder(tmp: TempDir) {
        const MULT_2: usize = 32;
        const BYTES_IN_TEST_FILE: usize = MULT_2 * 2 + 4;
        let path = many_byte_file(&tmp, BYTES_IN_TEST_FILE);

        let mut chunks: Vec<FileChunk> = Vec::new();
        // Up to 32 mebi
        let chunker = SizeBySpaceChunker {max_bytes: MULT_2};
        chunker.chunk(
            &path,
            tmp.path().to_str().expect("Failed to convert tempdir path to string"),
            &mut chunks
        );
        assert_eq!(chunks.len(), 3);
        let mut total: usize = 0;
        for (i, chnk) in chunks.iter().enumerate() {
            let mut buf = BufReader::new(chnk.open());
            let mut bytes = vec![0u8; BYTES_IN_TEST_FILE];
            let u = buf.read(&mut bytes).expect("Failed to read");
            total += u;
            if i == 2 {
                assert_eq!(u, 4);
            } else {
                assert_eq!(u, MULT_2);
            }
        }
        assert!(total == BYTES_IN_TEST_FILE);
        tmp.close().expect("Failed to close the expected directory.");
    }


}