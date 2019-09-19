use arcon::prelude::*;

#[derive(ComponentDefinition)]
pub struct ItemSource
{
    ctx: ComponentContext<Self>,
    channel_strategy: Box<dyn ChannelStrategy<crate::Item>>,
}

impl ItemSource
{
    pub fn new(channel_strategy: Box<dyn ChannelStrategy<crate::Item>>) -> Self {
        ItemSource {
            ctx: ComponentContext::new(),
            channel_strategy,
        }
    }

    fn send_elements(&mut self) {
            loop {
                let (id, price) = crate::item_row();
                let item = crate::Item { id: id, price: price };
                let _ = self
                    .channel_strategy
                    .output(ArconMessage::element(item, None, "source".to_string()), &self.ctx.system());
            }
    }
}

impl Provide<ControlPort> for ItemSource
{
    fn handle(&mut self, _event: ControlEvent) -> () {}
}


impl Actor for ItemSource
{
    type Message = String;

    fn receive_local(&mut self, _msg: Self::Message) {
        println!("starting source");
        self.send_elements();
    }

    fn receive_network(&mut self, _msg: NetMessage) {
        panic!("only local exec");
    }
}
