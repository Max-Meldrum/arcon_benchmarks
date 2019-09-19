#![allow(bare_trait_objects)]
pub extern crate arcon;

#[macro_use]
extern crate serde;

pub use arcon::data::*;
pub use arcon::macros::*;
use rand::Rng;

pub mod throughput_sink;
pub mod source;

#[key_by(id)]
#[arcon]
pub struct SensorData {
    pub id: u64,
    pub vec: ArconVec<i32>,
}

#[key_by(id)]
#[arcon]
pub struct EnrichedSensor {
    pub id: u32,
    pub total: i32,
}

pub fn sensor_data_row() -> (u64, Vec<i32>) {
    let mut rng = rand::thread_rng();
    let id = rng.gen_range(1, 10);
    let mut values = || (0..20).map(|_| rng.gen_range(1, 100)).collect();
    (id as u64, values())
}

pub fn generate_sensor_data(total_rows: u64, file: &str) {
    use std::fs::OpenOptions;
    use std::io::Write;

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(file)
        .unwrap();

    for _i in 0..total_rows {
        let (id, vec) = sensor_data_row();
        let vec_fmt = format!("{:?}", vec);
        // Probably the most beautiful code I have ever seen
        let vecy = vec_fmt.replace("[", "").replace("]", "").replace(", ", ",");
        let row_fmt_str = format!("{} {}", id, vecy.trim());
        let _ = writeln!(file, "{}", row_fmt_str);
    }
}

pub fn read_sensor_data(path: &str) -> Vec<(u64, Vec<i32>)> {
    let contents: String =
        std::fs::read_to_string(path).expect("Something went wrong reading the file");

    let mut data_fmt: Vec<String> = contents.split("\n").map(|x| x.to_string()).collect();
    let _ = data_fmt.pop();

    data_fmt
        .iter()
        .map(|line| {
            let s: Vec<&str> = line.split(" ").collect();
            let id = s[0].parse::<u64>().unwrap();
            let numbers: Vec<i32> = s[1].split(",").map(|x| x.parse::<i32>().unwrap()).collect();
            (id, numbers)
        })
        .collect()
}

#[key_by(id)]
#[arcon]
pub struct Item {
    pub id: u64,
    pub price: u64,
}


pub fn item_row() -> (u64, u64) {
    let mut rng = rand::thread_rng();
    let id = rng.gen_range(1, 100);
    let price = rng.gen_range(1, 10);
    (id as u64, price as u64)
}

pub fn generate_data_file(items: Vec<(u64, u64)>, file: &str) {
    use std::fs::OpenOptions;
    use std::io::Write;

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(file)
        .unwrap();

    for item in items {
        let row_fmt_str = format!("{} {}", item.0, item.1);
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
            Item {
                id,
                price,
            }
        })
        .collect()
}
