#![allow(bare_trait_objects)]
pub extern crate arcon;

#[macro_use]
extern crate serde;

pub use arcon::data::*;
pub use arcon::macros::*;
use rand::Rng;

pub mod item_source;
pub mod throughput_sink;


#[key_by(id)]
#[arcon]
pub struct Item {
    pub id: u64,
    pub price: u64,
}

use rand::distributions::{Poisson, Distribution};
use rand::{thread_rng};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
pub fn skewed_items(total_items: u64) -> Vec<Item> {
    let mut items: Vec<Item> = Vec::new();
    let poi = Poisson::new(1.0);
    let mut map: HashMap<i32, u64> = HashMap::with_capacity(4);
    let mut rng = rand::thread_rng();
    for _i in 0..total_items {
        let id = poi.sample(&mut rng);
        let price = rng.gen_range(1, 100);
        items.push(Item { id, price });

        /*
        let mut h = DefaultHasher::new();
        v.hash(&mut h);
        let hash = h.finish();
        let id = (hash % 4) as i32;
        map.insert(id, map.get(&id).unwrap_or(&0) + 1);
        */
        //println!("value {}", v);
    }


    for (id, count) in map.iter() {
        println!("Partition ID {} with count {}", id, count);
    }

    items
}

pub fn item_row() -> (u64, u64) {
    let mut rng = rand::thread_rng();
    let id = rng.gen_range(1, 10);
    let price = rng.gen_range(1, 100);
    (id as u64, price as u64)
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
            let id = s[0].parse::<u64>().unwrap();
            let price = s[1].parse::<u64>().unwrap();
            Item { id, price }
        })
        .collect()
}
