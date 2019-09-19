use arcon::prelude::*;
use std::time::{SystemTime, UNIX_EPOCH};


#[derive(ComponentDefinition)]
pub struct ThroughputSink<A>
where
    A: ArconType + 'static,
{
    ctx: ComponentContext<Self>,
    log_freq: u64,
    last_total_recv: u64,
    last_time: u64,
    total_recv: u64,
    avg_throughput: f32,
    throughput_counter: u64,
    throughput_sum: f32,
}

impl<A> ThroughputSink<A> 
where
    A: ArconType + 'static,
{
    pub fn new(log_freq: u64) -> Self {
        ThroughputSink {
            ctx: ComponentContext::new(),
            log_freq,
            last_total_recv: 0,
            last_time: 0,
            total_recv: 0,
            avg_throughput: 0.0,
            throughput_counter: 0,
            throughput_sum: 0.0,
        }
    }

    fn handle_event(&mut self, _event: &ArconEvent<A>) {
        if self.total_recv == 0 {
            println!("ThroughputLogging {}, {}", self.get_current_time(), self.total_recv);
        }

        self.total_recv += 1;

        if self.total_recv % self.log_freq == 0 {
            let current_time = self.get_current_time();
            let throughput = (self.total_recv - self.last_total_recv) as f32 / (current_time - self.last_time) as f32 * 1000.0;
            if throughput != 0.0 {
                self.throughput_counter +=1;
                self.throughput_sum += throughput;
                self.avg_throughput = self.throughput_sum / self.throughput_counter as f32;
            }
            println!("Throughput {}, Average {}", throughput, self.avg_throughput);
            self.last_time = current_time;
            self.last_total_recv = self.total_recv;
            println!("ThroughputLogging {}, {}", self.get_current_time(), self.total_recv);
        }
    }

    fn get_current_time(&self) -> u64 {
        let start = SystemTime::now();
        let since_the_epoch = start.duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        since_the_epoch.as_millis() as u64
    }
}

impl<A> Provide<ControlPort> for ThroughputSink<A>
where
    A: ArconType + 'static,
{
    fn handle(&mut self, _event: ControlEvent) -> () {}
}


impl<A> Actor for ThroughputSink<A> 
where
    A: ArconType + 'static,
{
    type Message = ArconMessage<A>;

    fn receive_local(&mut self, msg: Self::Message) {
        self.handle_event(&msg.event);
    }

    fn receive_network(&mut self, _msg: NetMessage) {
        panic!("only local exec");
    }
}
