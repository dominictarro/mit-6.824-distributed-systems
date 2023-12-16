/*
Module to help split files into digestible chunks for mapreduce.
*/
use std::fs::File;
use std::io::*;


#[derive(Debug)]
pub struct FileChunk {
    pub source: String,
    pub line_start: usize,
    pub line_end: usize,
    pub path: String
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
        out.strip_suffix("/").expect("Couldn't strip / suffix"),
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
/// and create a new chunk object from a chunk's context.
pub trait Chunker {
    fn build_chunk(&self, ctx: &ChunkerContext) -> FileChunk;
    fn chunk(&self, path: &str, out: &str, chunks: &mut Vec<FileChunk>);
}

/// A chunker that divides a file into equitable line counts.
pub struct LineChunker {
    pub max_lines: usize
}


#[allow(private_interfaces)]
impl Chunker for LineChunker {

    fn build_chunk(&self, ctx: &ChunkerContext) -> FileChunk {
        FileChunk {
            source: ctx.source.clone(),
            line_start: self.max_lines * ctx.chunk_idx,
            line_end: ctx.line_i + self.max_lines * ctx.chunk_idx,
            path: ctx.path.clone(),
        }
    }

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
                            chunks.push(self.build_chunk(&ctx));
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
            chunks.push(self.build_chunk(&ctx));
            chunk_idx += 1;
        }
    }

}

#[allow(dead_code)]
impl FileChunk {

    /// The number of lines in the file
    pub fn line_count(&self) -> usize {
        self.line_end - self.line_start
    }

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
            assert!(chnk.line_count()  == EXPECTED_LINES_PER_FILE);
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
            assert!(chnk.line_count()  == lines_per_file);
            assert!(chnk.true_line_count() == lines_per_file);
        }
        tmp.close().expect("Failed to close the expected directory.");
    }

}