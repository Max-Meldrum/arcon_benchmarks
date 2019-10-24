#![allow(bare_trait_objects)]
pub extern crate arcon;

#[macro_use]
extern crate serde;

pub use arcon::data::*;
pub use arcon::macros::*;
use fasthash::{murmur3::Hasher32, FastHasher};
use rand::Rng;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

pub mod item_source;
pub mod throughput_sink;

#[key_by(id)]
#[arcon]
pub struct Item {
    pub id: i32,
    pub price: u64,
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

        let price = rng.gen_range(1, 100);
        items.push(Item { id, price });
        counter += 1;

        if counter == total_items {
            break;
        }

        let mut h = Hasher32::new();
        id.hash(&mut h);
        let hash = h.finish();
        let hash_id = (hash % parallelism) as u32;
        map.insert(hash_id, map.get(&hash_id).unwrap_or(&0) + 1);

        if hash_id == 1 {
            let mut skew_counter: u64 = 0;
            while skew_counter < 2 {
                let price = rng.gen_range(1, 100);
                let id: i32 = rng.gen_range(1, 51);
                let mut h = Hasher32::new();
                id.hash(&mut h);
                let hash = h.finish();
                let bucket = (hash % parallelism) as u32;
                if bucket == 1 {
                    items.push(Item { id, price });
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

        let price = rng.gen_range(1, 100);
        items.push(Item { id, price });
        counter += 1;

        let mut h = Hasher32::new();
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
        let row_fmt_str = format!("{} {}", item.id, item.price);
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
            let price = s[1].parse::<u64>().unwrap();
            Item { id, price }
        })
        .collect()
}
