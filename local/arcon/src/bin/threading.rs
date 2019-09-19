extern crate arcon_local;
#[macro_use]
extern crate clap;

use arcon_local::arcon::prelude::*;
use arcon_local::throughput_sink::ThroughputSink;
use arcon_local::source::Source;
use arcon_local::Item;
use std::sync::Arc;
use clap::{App, AppSettings, Arg, SubCommand};


// Source -> Filter -> Map -> ThroughputSink
// scenario 1: deploy using dedicated threads, that is, Filter and Map
// scenario 2: deploy using normal workstealing 
// NOTE: Always create dedicated thread for Source and Sink
fn main() {

    let task_parallelism_arg = Arg::with_name("p")
        .required(false)
        .default_value("1")
        .takes_value(true)
        .long("How many filters/mappers to spawn")
        .short("p")
        .help("how many tasks to spawn");

    let log_frequency_arg = Arg::with_name("l")
        .required(false)
        .default_value("100000")
        .takes_value(true)
        .long("How often we log throughput")
        .short("l")
        .help("throughput log freq");

    let matches = App::new("Arcon Threading benchmark")
        .setting(AppSettings::ColoredHelp)
        .author(crate_authors!("\n"))
        .version(crate_version!())
        .setting(AppSettings::SubcommandRequired)
        .arg(
            Arg::with_name("d")
                .help("Daemonize the process")
                .long("daemonize")
                .short("d"),
        )
        .subcommand(
            SubCommand::with_name("dedicated")
                .setting(AppSettings::ColoredHelp)
                .arg(&task_parallelism_arg)
                .arg(&log_frequency_arg)
                .about("Run dedicated thread benchmark"),
        )
        .subcommand(
            SubCommand::with_name("workstealing")
                .setting(AppSettings::ColoredHelp)
                .arg(&task_parallelism_arg)
                .arg(&log_frequency_arg)
                .about("Run workstealing benchmark"),
        )
        .get_matches_from(fetch_args());


     match matches.subcommand() {
        ("dedicated", Some(arg_matches)) => {

        }
        ("workstealing", Some(arg_matches)) => {
             let log_freq_str = arg_matches
                .value_of("l")
                .expect("Should not happen as there is a default");

             let log_freq = log_freq_str.parse::<u64>().unwrap();

             let parallelism_str = arg_matches
                .value_of("p")
                .expect("Should not happen as there is a default");

             let parallelism = parallelism_str.parse::<u64>().unwrap();

             workstealing_execution(parallelism, log_freq);
        }
        _ => {
            panic!("Wrong arg");
        }
     }
}



fn fetch_args() -> Vec<String> {
    std::env::args().collect()
}

/// Source and Sinks run as dedicated threads,
/// while Mappers and Filters run on the workstealing scheduler..
fn workstealing_execution(parallelism: u64, log_freq: u64) {
    let system = KompactConfig::default().build().expect("KompactSystem");

    let sink = system.create_dedicated(move || ThroughputSink::<Item>::new(log_freq));
    system.start(&sink);
    std::thread::sleep(std::time::Duration::from_secs(1));

    let sink_ref: ActorRef<ArconMessage<Item>> = sink.actor_ref();
    let sink_channel = Channel::Local(sink_ref);

    // Mappers
    let code = String::from("|id: u64, price: u64| {id, price + u64(5)}");
    let mut map_comps: Vec<Arc<arcon::prelude::Component<Node<Item, Item>>>> = Vec::new();

    for _i in 0..parallelism {
        let channel_strategy: Box<dyn ChannelStrategy<Item>> = Box::new(Forward::new(sink_channel.clone()));
        let module = Arc::new(Module::new(code.clone()).unwrap());
        let map_node = system.create(move || {
                Node::<Item, Item>::new(
                    "mapper".to_string(),
                    vec!("filter".to_string()),
                    channel_strategy,
                    Box::new(Map::<Item, Item>::new(module))
                )
            });

        system.start(&map_node);
        std::thread::sleep(std::time::Duration::from_secs(1));
        map_comps.push(map_node);
    }

    // Filters 

    let mut channels: Vec<Channel<Item>> = Vec::new();

    for map_comp in map_comps {
        let actor_ref: ActorRef<ArconMessage<Item>> = map_comp.actor_ref();
        let channel = Channel::Local(actor_ref);
        channels.push(channel);
    }

    let code = String::from("|id: u64, price: u64| id > u64(5)");
    let mut filter_comps: Vec<Arc<arcon::prelude::Component<Node<Item, Item>>>> = Vec::new();

    for _i in 0..parallelism {
        let channel_strategy: Box<dyn ChannelStrategy<Item>> = Box::new(Shuffle::new(channels.clone()));
        let module = Arc::new(Module::new(code.clone()).unwrap());
        let filter = system.create(move || {
            Node::<Item, Item>::new(
                "filter".to_string(),
                vec!("source".to_string()),
                channel_strategy,
                Box::new(Filter::<Item>::new(module))
            )
        });

        system.start(&filter);
        filter_comps.push(filter);
    }

    std::thread::sleep(std::time::Duration::from_secs(1));

    // Source

    let mut channels: Vec<Channel<Item>> = Vec::new();

    for filter_comp in filter_comps {
        let actor_ref: ActorRef<ArconMessage<Item>> = filter_comp.actor_ref();
        let channel = Channel::Local(actor_ref);
        channels.push(channel);
    }

    let channel_strategy: Box<dyn ChannelStrategy<Item>> = Box::new(RoundRobin::new(channels));

    let mut items: Vec<Item> = Vec::new();
    for _i in 0..5000000 {
        let (id, price) = arcon_local::item_row();
        let item = Item { id: id, price: price}; 
        items.push(item);
    }

    let source = system.create_dedicated(move || Source::<Item>::new(items, channel_strategy));
    system.start(&source);

    std::thread::sleep(std::time::Duration::from_secs(1));
    let source_ref: ActorRef<String> = source.actor_ref();
    source_ref.tell(String::from("start"));

    system.await_termination();
}
