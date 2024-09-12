use std::{
    collections::HashMap, env::args, fs::File, sync::{Arc, Mutex}, thread::{self}
};

#[derive(Debug, Clone)]
struct Result {
    name: String,
    min: i32,
    max: i32,
    mean: i64,
    count: i64,
}

impl Result {
    fn new(name: &[u8]) -> Self {
        Self {
            name: String::from_utf8_lossy(name).to_string(),
            min: i32::MAX,
            max: i32::MIN,
            mean: 0,
            count: 0,
        }
    }

    fn update(&mut self, value: i32) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.mean += value as i64;
        self.count += 1;
    }

    fn merge(&mut self, other: &Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.mean += other.mean;
        self.count += other.count;
    }

    fn to_string(&self) -> String {
        let min = self.min as f64 / 10.0;
        let max = self.max as f64 / 10.0;
        let mean = self.mean as f64 / self.count as f64 / 10.0;
        format!("{}={:.1}/{:.1}/{:.1}", self.name, min, max, mean)
    }
}

fn find_next(data: &[u8], position: usize, char: u8) -> usize {
    for i in position..data.len() {
        if data[i] == char {
            return i;
        }
    }
    return position;
}

struct Chunk {
    data: Arc<memmap::Mmap>,
    end: usize,
    position: usize,
    result: HashMap<u64, Result>,
}

impl Chunk {
    fn new(data: Arc<memmap::Mmap>, start: usize, end: usize) -> Self {
        Self {
            data,
            end,
            position: start,
            result: HashMap::new(),
        }
    }

    fn parse_line(&mut self) -> bool {
        // Find next semicolon, skipped 3 bytes because town is at least 3 bytes
        let split_pos = find_next(&self.data, self.position+3, b';');
        let name = &self.data[self.position..split_pos];
        // Find next newline, skipped 3 bytes because temp at lest 3 bytes (x.x)
        self.position = find_next(&self.data, split_pos + 3, b'\n') + 1;
        let value = self.parse_value(&self.data[split_pos + 1..self.position - 1]);
        // Simple hash function, stolen from java
        let mut key: u64 = 0;
        for i in 0..name.len() {
            key *= 31;
            key += name[i] as u64;
        }
        // Update or insert new result
        self.result
            .entry(key)
            .and_modify(|fu: &mut Result| fu.update(value))
            .or_insert_with(|| Result::new(name));
        return self.position < self.end;
    }

    fn parse_value(&self, data: &[u8]) -> i32 {
        let neg = data[0] == b'-';
        let mut result: i32 = 0;
        // bool can be converted to usize because it is always 0 or 1
        for i in neg as usize..data.len() - 2 {
            // Convert ascii to int
            result = result * 10 + (data[i] - b'0') as i32;
        }
        // Convert to decimal (it's faster then using floats)
        result = result * 10 + (data[data.len() - 1] - b'0') as i32;
        if neg {
            -result
        } else {
            result
        }
    }
}

fn main() {
    let file_name = args().nth(1);
    if file_name.is_none() {
        eprintln!("Usage: {} <file>", args().next().unwrap());
        return;
    }
    let file_name = file_name.unwrap();
    let file = File::open(file_name).unwrap();

    // Map file to memory
    let mmaped = unsafe { memmap::Mmap::map(&file).unwrap() };
    let mmaped = Arc::new(mmaped);

    let max_threads: usize = thread::available_parallelism().unwrap().into();

    // Calculate chunk size for each thread
    let chunk_size = mmaped.len() / max_threads;

    // Split file into chunks by finding newlines at the end of each chunk
    let mut chunks = Vec::new();
    let mut next_start = 0;
    while next_start < mmaped.len() {
        let mut next_end = find_next(&mmaped, next_start + chunk_size, b'\n');
        if next_end > mmaped.len() {
            next_end = mmaped.len();
        }
        let chunk = Chunk::new(mmaped.clone(), next_start, next_end);
        chunks.push(Arc::new(Mutex::new(chunk)));
        next_start = next_end + 1;
    }

    // Start threads for each chunk
    let mut threads = Vec::new();
    for chunk in &chunks {
        let chunk = chunk.clone();
        threads.push(thread::spawn(move || {
            let mut chunk = chunk.lock().unwrap();
            while chunk.parse_line() {}
        }));
    }
    // Await all threads
    for thread in threads {
        thread.join().unwrap();
    }

    // Merge results
    let mut result = HashMap::new();
    for chunk in &chunks {
        let chunk = chunk.lock().unwrap();
        for (key, value) in &chunk.result {
            result
                .entry(key.clone())
                .and_modify(|fu: &mut Result| fu.merge(value))
                .or_insert_with(|| value.clone());
        }
    }
    let result = result
        .iter()
        .map(|(_, value)| value.to_string())
        .collect::<Vec<String>>();
    println!("{{{}}}", result.join(", "));
}
