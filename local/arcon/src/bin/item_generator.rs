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
    let items = arcon_local::skewed_items(total_items);
    arcon_local::generate_data_file(items, "data");
}
