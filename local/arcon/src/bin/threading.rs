extern crate arcon_local;
#[macro_use]
extern crate clap;

use arcon_local::arcon::prelude::*;
use arcon_local::throughput_sink::ThroughputSink;
use arcon_local::source::Source;
use arcon_local::item_source::ItemSource;
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

    let kompact_throughput_arg = Arg::with_name("k")
        .required(false)
        .default_value("10000")
        .takes_value(true)
        .long("Kompact cfg throughput")
        .short("k")
        .help("kompact cfg throughput");

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
                .help("dedicated mode")
                .long("dedicated")
                .short("d"),
        )
        .subcommand(
            SubCommand::with_name("run")
                .setting(AppSettings::ColoredHelp)
                .arg(&task_parallelism_arg)
                .arg(&kompact_throughput_arg)
                .arg(&log_frequency_arg)
                .about("Run benchmark"),
        )
        .get_matches_from(fetch_args());


    let dedicated: bool = matches.is_present("d");

     match matches.subcommand() {
        ("run", Some(arg_matches)) => {
             let log_freq = arg_matches
                .value_of("l")
                .expect("Should not happen as there is a default")
                .parse::<u64>().unwrap();

             let parallelism = arg_matches
                .value_of("p")
                .expect("Should not happen as there is a default")
                .parse::<u64>().unwrap();

             let kompact_throughput = arg_matches
                .value_of("k")
                .expect("Should not happen as there is a default")
                .parse::<u64>().unwrap();

             exec(parallelism, log_freq, kompact_throughput, dedicated);
        }
        _ => {
            panic!("Wrong arg");
        }
     }
}

fn fetch_args() -> Vec<String> {
    std::env::args().collect()
}


fn exec(parallelism: u64, log_freq: u64, kompact_throughput: u64, dedicated: bool) {
    let mut cfg = KompactConfig::default();
    if !dedicated {
        cfg.throughput(kompact_throughput as usize);
        cfg.msg_priority(1.0);
    }

    let system = cfg.build().expect("KompactSystem");

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
        let node = Node::<Item, Item>::new(
                    "mapper".to_string(),
                    vec!("filter".to_string()),
                    channel_strategy,
                    Box::new(Map::<Item, Item>::new(module))
                );

        let map_node = if dedicated {
            system.create_dedicated(move || node)
        }  else {
            system.create(move || node)
        };
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

    let code = String::from("|id: u64, price: u64| id > u64(2)");
    let mut filter_comps: Vec<Arc<arcon::prelude::Component<Node<Item, Item>>>> = Vec::new();

    for _i in 0..parallelism {
        let channel_strategy: Box<dyn ChannelStrategy<Item>> = Box::new(Shuffle::new(channels.clone()));
        let module = Arc::new(Module::new(code.clone()).unwrap());
        let node = Node::<Item, Item>::new(
                "filter".to_string(),
                vec!("source".to_string()),
                channel_strategy,
                Box::new(Filter::<Item>::new(module))
            );

        let filter = if dedicated {
            system.create_dedicated(move || node)
        }  else {
            system.create(move || node)
        };

        system.start(&filter);
        filter_comps.push(filter);
    }

    std::thread::sleep(std::time::Duration::from_secs(1));

    // Source

    let mut filter_channels: Vec<Channel<Item>> = Vec::new();

    for filter_comp in filter_comps {
        let actor_ref: ActorRef<ArconMessage<Item>> = filter_comp.actor_ref();
        let channel = Channel::Local(actor_ref);
        filter_channels.push(channel);
    }

    let channel_strategy: Box<dyn ChannelStrategy<Item>> = Box::new(RoundRobin::new(filter_channels.clone()));

    let source = system.create_dedicated(move || ItemSource::new(channel_strategy));
    system.start(&source);

    std::thread::sleep(std::time::Duration::from_secs(1));
    let source_ref: ActorRef<String> = source.actor_ref();
    source_ref.tell(String::from("start"));

    system.await_termination();
}
