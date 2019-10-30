#![allow(bare_trait_objects)]
pub extern crate arcon;

#[macro_use]
extern crate serde;

pub use arcon::data::*;
pub use arcon::macros::*;
use rand::Rng;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

pub mod file_item_source;
pub mod throughput_sink;

#[key_by(id)]
#[arcon]
pub struct Item {
    pub id: i32,
    pub number: u64,
    pub scaling_factor: f64,
}

#[arcon]
pub struct EnrichedItem {
    pub id: i32,
    pub total: u64,
}

pub struct FlinkMurmurHash(i32);

impl Hasher for FlinkMurmurHash {
    #[inline]
    fn finish(&self) -> u64 {
        flink_murmur_hash(self.0 as u32) as u64
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let FlinkMurmurHash(mut hash) = *self;
        for byte in bytes.iter() {
            hash = hash ^ (*byte as i32);
        }

        *self = FlinkMurmurHash(hash);
    }
}

impl Default for FlinkMurmurHash {
    #[inline]
    fn default() -> FlinkMurmurHash {
        FlinkMurmurHash(0)
    }
}

pub fn flink_murmur_hash(code: u32) -> i32 {
    let mut state = code;

    const C1: u32 = 0xcc9e2d51;
    const C2: u32 = 0x1b873593;
    const N: u32 = 0xe6546b64;
    const M: u32 = 5;

    state = state.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
    state = state.rotate_left(13);
    state = (state.wrapping_mul(M)).wrapping_add(N);

    state ^= 4;

    state ^= state.wrapping_shr(16);
    state = state.wrapping_mul(0x85ebca6b);
    state ^= state.wrapping_shr(13);
    state = state.wrapping_mul(0xc2b2ae35);
    state ^= state.wrapping_shr(16);

    let state: i32 = state as i32;
    if state >= 0 {
        return state;
    } else if state != std::i32::MIN {
        return -state;
    } else {
        return 0;
    }
}

pub fn skewed_items(total_items: u64, parallelism: u64) -> Vec<Item> {
    let mut items: Vec<Item> = Vec::new();
    let mut map: HashMap<u32, u64> = HashMap::with_capacity(4);
    let mut max: i32 = 0;
    let mut rng = rand::thread_rng();
    let mut counter: u64 = 0;

    while counter < total_items {
        let id: i32 = rng.gen_range(1, 51);
        if id > max {
            max = id;
        }

        let number = rng.gen_range(1, 100);
        items.push(Item {
            id,
            number,
            scaling_factor: 1.0,
        });

        let mut h = FlinkMurmurHash::default();
        id.hash(&mut h);
        let hash = h.finish();
        let hash_id = (hash % parallelism) as u32;
        map.insert(hash_id, map.get(&hash_id).unwrap_or(&0) + 1);

        counter += 1;

        if counter == total_items {
            break;
        }

        if hash_id == 1 {
            let mut skew_counter: u64 = 0;
            while skew_counter < 2 {
                let id: i32 = rng.gen_range(1, 51);
                let mut h = FlinkMurmurHash::default();
                id.hash(&mut h);
                let hash = h.finish();
                let hash_id = (hash % parallelism) as u32;
                if hash_id == 1 {
                    let number = rng.gen_range(1, 100);
                    items.push(Item {
                        id,
                        number,
                        scaling_factor: 1.0,
                    });
                    map.insert(hash_id, map.get(&hash_id).unwrap_or(&0) + 1);
                    skew_counter += 1;
                    counter += 1;
                    if counter == total_items {
                        break;
                    }
                }
            }
        }
    }
    println!("MAX {}", max);

    for (id, count) in map.iter() {
        println!("Partition ID {} with count {}", id, count);
    }
    items
}

pub fn uniform_items(total_items: u64, parallelism: u64) -> Vec<Item> {
    let mut items: Vec<Item> = Vec::new();
    let mut map: HashMap<u32, u64> = HashMap::with_capacity(4);
    let mut max: i32 = 0;
    let mut rng = rand::thread_rng();
    let mut counter: u64 = 0;

    while counter < total_items {
        let id: i32 = rng.gen_range(1, 51);
        if id > max {
            max = id;
        }

        let number = rng.gen_range(1, 100);
        items.push(Item {
            id,
            number,
            scaling_factor: 1.0,
        });
        counter += 1;

        let mut h = FlinkMurmurHash::default();
        id.hash(&mut h);
        let hash = h.finish();
        let hash_id = (hash % parallelism) as u32;
        map.insert(hash_id, map.get(&hash_id).unwrap_or(&0) + 1);
    }

    println!("MAX {}", max);

    for (id, count) in map.iter() {
        println!("Partition ID {} with count {}", id, count);
    }
    items
}

pub fn generate_data_file(items: Vec<Item>, file: &str) {
    use std::fs::OpenOptions;
    use std::io::Write;

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(file)
        .unwrap();

    for item in items {
        let row_fmt_str = format!("{} {}", item.id, item.number);
        let _ = writeln!(file, "{}", row_fmt_str);
    }
}

pub fn read_data(path: &str) -> Vec<Item> {
    let contents: String =
        std::fs::read_to_string(path).expect("Something went wrong reading the file");

    let mut data_fmt: Vec<String> = contents.split("\n").map(|x| x.to_string()).collect();
    let _ = data_fmt.pop();

    data_fmt
        .iter()
        .map(|line| {
            let s: Vec<&str> = line.split(" ").collect();
            let id = s[0].parse::<i32>().unwrap();
            let number = s[1].parse::<u64>().unwrap();
            Item {
                id,
                number,
                scaling_factor: 1.0,
            }
        })
        .collect()
}
