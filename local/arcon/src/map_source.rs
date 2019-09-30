use arcon::prelude::*;

#[derive(ComponentDefinition)]
pub struct MapSource {
    ctx: ComponentContext<Self>,
    channel_strategy: Box<dyn ChannelStrategy<u32>>,
}

impl MapSource {
    pub fn new(channel_strategy: Box<dyn ChannelStrategy<u32>>) -> Self {
        MapSource {
            ctx: ComponentContext::new(),
            channel_strategy,
        }
    }

    fn send_elements(&mut self) {
        let mut counter: u64 = 0;
        let limit: u64 = 10000000;
        while counter < limit {
            let _ = self.channel_strategy.output(
                ArconMessage::element(10 as u32, None, "source".to_string()),
                &self.ctx.system(),
            );

            counter += 1;
        }
    }
}

impl Provide<ControlPort> for MapSource {
    fn handle(&mut self, _event: ControlEvent) -> () {}
}

impl Actor for MapSource {
    type Message = String;

    fn receive_local(&mut self, _msg: Self::Message) {
        println!("starting source");
        self.send_elements();
    }

    fn receive_network(&mut self, _msg: NetMessage) {
        panic!("only local exec");
    }
}
