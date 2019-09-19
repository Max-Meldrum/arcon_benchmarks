extern crate arcon_local;

use arcon_local::arcon::prelude::*;
use arcon_local::throughput_sink::ThroughputSink;

fn main() {
    let system = KompactConfig::default().build().expect("KompactSystem");
    let log_freq: u64 = 100000;
    /*
    let sink = system.create_dedicated(move || ThroughputSink::<u64>::new(log_freq));
    system.start(&sink);
    */
    let sink= system.create_and_start(move || ThroughputSink::<u64>::new(log_freq));
    std::thread::sleep(std::time::Duration::from_millis(5000));
    //system.start(&sink);
    let sink_ref: ActorRef<ArconMessage<u64>> = sink.actor_ref();

    //let sink = system.create(move || ThroughputSink::<u64>::new(log_freq));
    //let reg = system.start_notify(&sink);
    //reg.wait_timeout(std::time::Duration::from_millis(1000)).expect("failed to reg");
    //let sink_ref: ActorRef<ArconMessage<u64>> = sink.actor_ref();
    //std::thread::sleep(std::time::Duration::from_millis(5000));


    for n in 0..1000000 {
        let input = ArconMessage::element(n as u64,  None, "test".to_string());
        sink_ref.tell(input);
    }

    std::thread::sleep(std::time::Duration::from_millis(5000));


    let _ = system.shutdown();
}
