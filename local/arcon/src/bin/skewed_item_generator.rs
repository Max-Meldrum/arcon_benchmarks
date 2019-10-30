extern crate arcon_local;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    match args.len() {
        3 => {
            let total_items: u64 = args[1].parse().unwrap();
            let parallelism: u64 = args[2].parse().unwrap();
            let items = arcon_local::skewed_items(total_items, parallelism);
            arcon_local::generate_data_file(items, "skewed_data");
        }
        _ => {
            panic!("Expected 2 arg");
        }
    }
}
