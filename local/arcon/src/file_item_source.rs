use crate::Item;
use arcon::prelude::*;
use std::fs::File;
use std::io::{self, prelude::*, BufReader};

#[derive(ComponentDefinition)]
pub struct FileItemSource {
    ctx: ComponentContext<Self>,
    channel_strategy: Box<dyn ChannelStrategy<crate::Item>>,
    file_path: String,
    scaling_factor: f64,
}

impl FileItemSource {
    pub fn new(
        file_path: String,
        scaling_factor: f64,
        channel_strategy: Box<dyn ChannelStrategy<crate::Item>>,
    ) -> Self {
        FileItemSource {
            ctx: ComponentContext::new(),
            channel_strategy,
            file_path,
            scaling_factor,
        }
    }

    fn send_elements(&mut self) {
        let file = File::open(&self.file_path).expect("Failed to open");
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let s: Vec<&str> = line.as_ref().unwrap().split(" ").collect();
            let id = s[0].parse::<i32>().unwrap();
            let number = s[1].parse::<u64>().unwrap();
            let item = Item {
                id,
                number,
                scaling_factor: self.scaling_factor,
            };

            let _ = self.channel_strategy.output(
                ArconMessage::element(item, None, 0.into()),
                &self.ctx.system(),
            );
        }
    }
}

impl Provide<ControlPort> for FileItemSource {
    fn handle(&mut self, _event: ControlEvent) -> () {}
}

impl Actor for FileItemSource {
    type Message = String;

    fn receive_local(&mut self, _msg: Self::Message) {
        self.send_elements();
    }

    fn receive_network(&mut self, _msg: NetMessage) {
        panic!("only local exec");
    }
}
