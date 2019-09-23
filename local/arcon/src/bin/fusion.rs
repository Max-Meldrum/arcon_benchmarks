extern crate arcon_local;
#[macro_use]
extern crate clap;

use arcon_local::arcon::prelude::*;
use arcon_local::sensor_source::SensorSource;
use arcon_local::throughput_sink::ThroughputSink;
use arcon_local::{EnrichedSensor, SensorData};
use clap::{App, AppSettings, Arg, SubCommand};
use std::sync::Arc;

fn main() {
    let kompact_throughput_arg = Arg::with_name("k")
        .required(false)
        .default_value("100000")
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

    let matches = App::new("Arcon Fusion benchmark")
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
                .parse::<u64>()
                .unwrap();

            let kompact_throughput = arg_matches
                .value_of("k")
                .expect("Should not happen as there is a default")
                .parse::<u64>()
                .unwrap();

            exec_one(log_freq, kompact_throughput, dedicated);
        }
        _ => {
            panic!("Wrong arg");
        }
    }
}

fn fetch_args() -> Vec<String> {
    std::env::args().collect()
}

// Filter -> Map -> Map -> FlatMap
fn stream_transformations(log_freq: u64, kompact_throughput: u64, dedicated: bool) {
    let mut cfg = KompactConfig::default();
    if !dedicated {
        cfg.throughput(kompact_throughput as usize);
        cfg.msg_priority(1.0);
    }

    let system = cfg.build().expect("KompactSystem");
}

fn exec_one(log_freq: u64, kompact_throughput: u64, dedicated: bool) {
    let mut cfg = KompactConfig::default();
    cfg.threads(3 as usize);
    if !dedicated {
        cfg.throughput(kompact_throughput as usize);
        cfg.msg_priority(1.0);
    }

    let system = cfg.build().expect("KompactSystem");

    let sink = system.create(move || ThroughputSink::<EnrichedSensor>::new(log_freq));
    system.start(&sink);
    std::thread::sleep(std::time::Duration::from_secs(1));

    let code = "|id: u32, x:vec[i32]|
            let m = merger[i32, +];
            let op = for(x, m, |b: merger[i32, +], i, e|  let mapped = e + 5;
                if(mapped > 50, merge(b, mapped), b));
            {id, result(op)}";

    let module = Arc::new(Module::new(code.to_string()).unwrap());

    let sink_ref: ActorRef<ArconMessage<EnrichedSensor>> = sink.actor_ref();
    let channel = Channel::Local(sink_ref);
    let channel_strategy: Box<dyn ChannelStrategy<EnrichedSensor>> =
        Box::new(Forward::new(channel));

    let node = Node::<SensorData, EnrichedSensor>::new(
        "sensor".to_string(),
        vec!["source".to_string()],
        channel_strategy,
        Box::new(Map::new(module)),
    );

    let sensor = if dedicated {
        system.create_dedicated(move || node)
    } else {
        system.create(move || node)
    };
    system.start(&sensor);

    let sensor_ref: ActorRef<ArconMessage<SensorData>> = sensor.actor_ref();
    let sensor_channel = Channel::Local(sensor_ref);
    let sensor_strategy: Box<dyn ChannelStrategy<SensorData>> =
        Box::new(Forward::new(sensor_channel));

    let source = system.create_dedicated(move || SensorSource::new(sensor_strategy));
    system.start(&source);

    std::thread::sleep(std::time::Duration::from_secs(1));
    let source_ref: ActorRef<String> = source.actor_ref();
    source_ref.tell(String::from("start"));

    system.await_termination();
}
