extern crate arcon_local;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut total_items: u64 = 0;

    match args.len() {
        2 => {
            let items: u64 = args[1].parse().unwrap();
            total_items = items;
        }
        _ => {
            panic!("Expected 1 arg, that is number of items");
        }
    }
    let items: Vec<(u64, u64)> = (0..total_items).map(|_| arcon_local::item_row()).collect();
    arcon_local::generate_data_file(items, "data");
}