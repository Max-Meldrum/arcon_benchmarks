use crate::SensorData;
use arcon::prelude::*;

#[derive(ComponentDefinition)]
pub struct SensorSource {
    ctx: ComponentContext<Self>,
    channel_strategy: Box<dyn ChannelStrategy<SensorData>>,
}

impl SensorSource {
    pub fn new(channel_strategy: Box<dyn ChannelStrategy<SensorData>>) -> Self {
        SensorSource {
            ctx: ComponentContext::new(),
            channel_strategy,
        }
    }

    fn send_elements(&mut self) {
        loop {
            let (id, values) = crate::sensor_data_row();
            let sensor_data = SensorData {
                id: id,
                vec: ArconVec::new(values),
            };
            let _ = self.channel_strategy.output(
                ArconMessage::element(sensor_data, None, "source".to_string()),
                &self.ctx.system(),
            );
        }
    }
}

impl Provide<ControlPort> for SensorSource {
    fn handle(&mut self, _event: ControlEvent) -> () {}
}

impl Actor for SensorSource {
    type Message = String;

    fn receive_local(&mut self, _msg: Self::Message) {
        println!("starting source");
        self.send_elements();
    }

    fn receive_network(&mut self, _msg: NetMessage) {
        panic!("only local exec");
    }
}
