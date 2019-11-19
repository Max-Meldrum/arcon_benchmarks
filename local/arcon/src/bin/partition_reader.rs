extern crate arcon_local;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    match args.len() {
        3 => {
            let path: String = args[1].parse().unwrap();
            let parallelism: u64 = args[2].parse().unwrap();
            let items = arcon_local::read_data("data");
            arcon_local::partition_reader(parallelism, items);
        }
        _ => {
            panic!("Expected 2 arg");
        }
    }
}
