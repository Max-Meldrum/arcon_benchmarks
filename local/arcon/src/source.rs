use arcon::prelude::*;

#[derive(ComponentDefinition)]
pub struct Source<A>
where
    A: ArconType + 'static,
{
    ctx: ComponentContext<Self>,
    data: Vec<A>,
    channel_strategy: Box<dyn ChannelStrategy<A>>,
}

impl<A> Source<A> 
where
    A: ArconType + 'static,
{
    pub fn new(data: Vec<A>, channel_strategy: Box<dyn ChannelStrategy<A>>) -> Self {
        Source {
            ctx: ComponentContext::new(),
            data,
            channel_strategy,
        }
    }

    fn send_elements(&mut self) {
        for element in self.data.iter() {
            let _ = self
                .channel_strategy
                .output(ArconMessage::element(element.clone(), None, "source".to_string()), &self.ctx.system());
        }
    }
}

impl<A> Provide<ControlPort> for Source<A>
where
    A: ArconType + 'static,
{
    fn handle(&mut self, _event: ControlEvent) -> () {}
}


impl<A> Actor for Source<A> 
where
    A: ArconType + 'static,
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
