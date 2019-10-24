extern crate arcon_local;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut total_items: u64 = 0;
    let mut parallelism: u64 = 0;

    match args.len() {
        3 => {
            let items: u64 = args[1].parse().unwrap();
            total_items = items;
            let p: u64 = args[2].parse().unwrap();
            parallelism = p;
        }
        _ => {
            panic!("Expected 2 arg");
        }
    }
    let items = arcon_local::skewed_items(total_items, parallelism);
    arcon_local::generate_data_file(items, "skewed_data");
}
