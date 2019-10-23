use arcon::prelude::*;

#[derive(ComponentDefinition)]
pub struct ItemSource {
    ctx: ComponentContext<Self>,
    channel_strategy: Box<dyn ChannelStrategy<crate::Item>>,
    items: Option<Vec<crate::Item>>,
}

impl ItemSource {
    pub fn new(
        items: Vec<crate::Item>,
        channel_strategy: Box<dyn ChannelStrategy<crate::Item>>,
    ) -> Self {
        ItemSource {
            ctx: ComponentContext::new(),
            channel_strategy,
            items: Some(items),
        }
    }

    fn send_elements(&mut self) {
        let items = self.items.take().expect("no");
        for item in items {
            let _ = self.channel_strategy.output(
                ArconMessage::element(item, None, 0.into()),
                &self.ctx.system(),
            );
        }
    }
}

impl Provide<ControlPort> for ItemSource {
    fn handle(&mut self, _event: ControlEvent) -> () {}
}

impl Actor for ItemSource {
    type Message = String;

    fn receive_local(&mut self, _msg: Self::Message) {
        println!("starting source");
        self.send_elements();
    }

    fn receive_network(&mut self, _msg: NetMessage) {
        panic!("only local exec");
    }
}
