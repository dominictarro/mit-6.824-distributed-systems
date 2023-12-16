pub mod chunker;
use std::fs::File;
use std::io::*;

fn read_into_array() {
    let file = File::open("/Users/dominictarro/Documents/GitHub/mit-6.824-distributed-systems/Labs/mapreduce/data/unpacked/eng_news_2020_1M-sentences.txt").expect("failed to read path");
    let mut reader = BufReader::new(file);
    let mut i = 0;

    const MAX_CHUNK_SIZE: usize = 31_999_999;
    let mut remainder: Vec<u8> = Vec::with_capacity(MAX_CHUNK_SIZE);
    let mut buf = vec![0u8; MAX_CHUNK_SIZE];
    let mut byte_i = MAX_CHUNK_SIZE - 1;
    loop {
        let output = File::create(format!("test/{}.txt", i)).expect("couldn't open the output file");
        let mut writer = BufWriter::new(output);
   
        let (left, right) = buf.split_at_mut(remainder.len());
        // If there is a remainder from the last iteration, fill the left array with it
        // and clear the remainder vector
        if !remainder.is_empty() {
            left.copy_from_slice(remainder.as_slice());
            remainder.clear();
        }
        // Read values into the remaining allocated array space
        reader.read(right).expect("Couldn't read into buffer");
        while buf[byte_i].is_ascii_whitespace() {
            byte_i -= 1;
        }

        // If the byte index was walked back, add the excluded values to the remainder
        // so they are handled in the next iteration
        if byte_i < MAX_CHUNK_SIZE - 1 {
            remainder.append(&mut buf[byte_i + 1..].to_vec());
        }
        writer.write_all(&buf).expect("Failed to write chunk");
        writer.flush().expect("Failed to flush chunk");
        // println!("{} | {:?} == | == {} | {:?}", i, remainder, byte_i, buf);
        i += 1;
        // if i > 10 {break}
        byte_i = MAX_CHUNK_SIZE - 1;
        buf.fill(0);
    }
}

fn main() {
    // println!("Hello, world!");
    let src = String::from("/Users/dominictarro/Documents/GitHub/mit-6.824-distributed-systems/Labs/mapreduce/data/unpacked/eng_news_2020_1M-sentences.txt");
    let tmp = String::from("/Users/dominictarro/Documents/GitHub/mit-6.824-distributed-systems/Labs/mapreduce/data/tmp");
    // let mut chunks: Vec<chunker::FileChunk> = Vec::new();
    // let chnkr = chunker::SizeBySpaceChunker {max_bytes: 150_000};
    // chnkr.chunk(&src, &tmp, &chnkr, &mut chunks);

    // for chnk in chunks {
    //     println!("({}-{}) {}", chnk.line_start, chnk.line_end, chnk.path);
    // }
    // read_into_array();
}
