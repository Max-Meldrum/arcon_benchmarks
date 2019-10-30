extern crate arcon_local;
#[macro_use]
extern crate clap;

use arcon_local::arcon::prelude::*;
use arcon_local::file_item_source::FileItemSource;
use arcon_local::throughput_sink::Run;
use arcon_local::throughput_sink::ThroughputSink;
use arcon_local::EnrichedItem;
use arcon_local::FlinkMurmurHash;
use arcon_local::Item;
use clap::{App, AppSettings, Arg, SubCommand};
use std::sync::Arc;

// Source -> (KeyBy) Map -> ThroughputSink
fn main() {
    let task_parallelism_arg = Arg::with_name("p")
        .required(false)
        .default_value("1")
        .takes_value(true)
        .long("How many mappers to spawn")
        .short("p")
        .help("how many tasks to spawn");

    let kompact_throughput_arg = Arg::with_name("k")
        .required(false)
        .default_value("50")
        .takes_value(true)
        .long("Kompact cfg throughput")
        .short("k")
        .help("kompact cfg throughput");

    let kompact_system_threads_arg = Arg::with_name("t")
        .required(false)
        .default_value("num_cpus::get()")
        .takes_value(true)
        .long("KompactSystem threads")
        .short("t")
        .help("KompactSystem threads");

    let log_frequency_arg = Arg::with_name("l")
        .required(false)
        .default_value("100000")
        .takes_value(true)
        .long("How often we log throughput")
        .short("l")
        .help("throughput log freq");

    let scaling_factor_arg = Arg::with_name("s")
        .required(false)
        .default_value("1.0")
        .takes_value(true)
        .long("workload scaling")
        .short("s")
        .help("workload scaling");

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
        .arg(
            Arg::with_name("p")
                .help("dedicated-pinned mode")
                .long("dedicated-pinned")
                .short("p"),
        )
        .subcommand(
            SubCommand::with_name("run")
                .setting(AppSettings::ColoredHelp)
                .arg(&task_parallelism_arg)
                .arg(&kompact_throughput_arg)
                .arg(&log_frequency_arg)
                .arg(&kompact_system_threads_arg)
                .arg(&scaling_factor_arg)
                .about("Run benchmark"),
        )
        .get_matches_from(fetch_args());

    let dedicated: bool = matches.is_present("d");
    let pinned: bool = matches.is_present("dp");

    match matches.subcommand() {
        ("run", Some(arg_matches)) => {
            let log_freq = arg_matches
                .value_of("l")
                .expect("Should not happen as there is a default")
                .parse::<u64>()
                .unwrap();

            let parallelism = arg_matches
                .value_of("p")
                .expect("Should not happen as there is a default")
                .parse::<u64>()
                .unwrap();

            let kompact_throughput = arg_matches
                .value_of("k")
                .expect("Should not happen as there is a default")
                .parse::<u64>()
                .unwrap();

            let kompact_system_threads = arg_matches
                .value_of("t")
                .expect("Should not happen as there is a default")
                .parse::<usize>()
                .unwrap_or(num_cpus::get());

            let scaling_factor = arg_matches
                .value_of("s")
                .expect("Should not happen as there is a default")
                .parse::<f64>()
                .unwrap();

            exec(
                scaling_factor,
                parallelism,
                log_freq,
                kompact_throughput,
                kompact_system_threads,
                dedicated,
                pinned,
            );
        }
        _ => {
            panic!("Wrong arg");
        }
    }
}

fn fetch_args() -> Vec<String> {
    std::env::args().collect()
}

fn exec(
    scaling_factor: f64,
    parallelism: u64,
    log_freq: u64,
    kompact_throughput: u64,
    kompact_system_threads: usize,
    dedicated: bool,
    pinned: bool,
) {
    let core_ids = arcon_local::arcon::prelude::get_core_ids().unwrap();
    let mut core_counter: usize = 0;
    let timeout = std::time::Duration::from_millis(500);

    let mut cfg = KompactConfig::default();
    // one dedicated thread for the source
    let threads = kompact_system_threads - 1;
    cfg.threads(threads);
    if !dedicated {
        cfg.throughput(kompact_throughput as usize);
        cfg.msg_priority(1.0);
    }

    let system = cfg.build().expect("KompactSystem");

    let expected_msgs: u64 = 10000000;
    let sink = system.create(move || ThroughputSink::<EnrichedItem>::new(log_freq, expected_msgs));
    let sink_port = sink.on_definition(|cd| cd.sink_port.share());

    system
        .start_notify(&sink)
        .wait_timeout(timeout)
        .expect("sink never started!");

    let sink_ref: ActorRefStrong<ArconMessage<EnrichedItem>> = sink.actor_ref().hold().expect("no");
    let sink_channel = Channel::Local(sink_ref);

    fn map_fn(item: Item) -> EnrichedItem {
        // credit: https://github.com/eliovir/rust-examples/blob/master/fibonacci.rs
        #[inline(always)]
        fn fibonacci(n: u64) -> u64 {
            if n == 0 {
                panic!("zero is not a right argument to fibonacci()!");
            } else if n == 1 {
                return 1;
            }

            let mut sum = 0;
            let mut last = 0;
            let mut curr = 1;
            for _i in 1..n {
                sum = last + curr;
                last = curr;
                curr = sum;
            }
            sum
        }

        let fib  = (item.number as f64 / 100.0).ceil() * item.scaling_factor;
        let fib_sum = fibonacci(fib as u64);

        EnrichedItem {
            id: item.id,
            total: fib_sum,
        }
    }

    // Mappers
    let mut map_comps: Vec<Arc<arcon::prelude::Component<Node<Item, EnrichedItem>>>> = Vec::new();

    for _i in 0..parallelism {
        let channel_strategy: Box<dyn ChannelStrategy<EnrichedItem>> =
            Box::new(Forward::new(sink_channel.clone()));
        let node = Node::<Item, EnrichedItem>::new(
            1.into(),
            vec![0.into()],
            channel_strategy,
            Box::new(Map::new(&map_fn)),
        );

        let map_node = if dedicated {
            if pinned {
                assert!(core_counter < core_ids.len());
                core_counter += 1;
                system.create_dedicated_pinned(move || node, core_ids[core_counter - 1])
            } else {
                system.create_dedicated(move || node)
            }
        } else {
            system.create(move || node)
        };

        system
            .start_notify(&map_node)
            .wait_timeout(timeout)
            .expect("map_node never started!");
        map_comps.push(map_node);
    }

    // Source

    let mut map_channels: Vec<Channel<Item>> = Vec::new();

    for map_comp in &map_comps {
        let actor_ref: ActorRefStrong<ArconMessage<Item>> =
            map_comp.actor_ref().hold().expect("no");
        let channel = Channel::Local(actor_ref);
        map_channels.push(channel);
    }

    let channel_strategy: Box<dyn ChannelStrategy<Item>> =
        Box::new(KeyBy::with_hasher::<FlinkMurmurHash>(
            map_comps.len() as u32,
            map_channels.clone(),
        ));

    let source = system.create_dedicated(move || {
        FileItemSource::new("data".to_string(), scaling_factor, channel_strategy)
    });

    system
        .start_notify(&source)
        .wait_timeout(timeout)
        .expect("source never started!");

    // Set up start time at Sink
    let (promise, future) = kpromise();
    system.trigger_r(Run::new(promise), &sink_port);
    // Tell Source to start sending msgs
    let source_ref: ActorRef<String> = source.actor_ref();
    source_ref.tell(String::from("start"));

    // wait for sink to return completion msg.
    let res = future.wait();
    println!("Execution took {:?}", res.as_millis());
}
