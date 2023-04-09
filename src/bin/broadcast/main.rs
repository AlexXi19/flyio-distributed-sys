use flyio_distributed_sys::*;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{StdoutLock, Write};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast { message: u32 },
    BroadcastOk {},
    Read {},
    ReadOk { messages: Vec<u32> },
    Topology { topology: Value },
    TopologyOk {},
}

struct BroadcastNode {
    id: usize,
    topology: Option<Value>,
    messages: Vec<u32>,
}

impl Node<(), Payload> for BroadcastNode {
    fn from_init(
        _state: (),
        _init: Init,
        _tx: std::sync::mpsc::Sender<Event<Payload>>,
    ) -> anyhow::Result<Self> {
        Ok(BroadcastNode {
            id: 1,
            topology: None,
            messages: vec![],
        })
    }

    fn step(&mut self, input: Event<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            panic!("got injected event when there's no event injection");
        };

        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Broadcast { message } => {
                reply.body.payload = Payload::BroadcastOk {};
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to init")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.messages.push(message);
            }
            Payload::BroadcastOk { .. } => {}
            Payload::Read {} => {
                reply.body.payload = Payload::ReadOk {
                    messages: self.messages.clone(),
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to init")?;
                output.write_all(b"\n").context("write trailing newline")?;
            }
            Payload::ReadOk { .. } => {}
            Payload::Topology { topology } => {
                self.topology = Some(topology);
                reply.body.payload = Payload::TopologyOk {};
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to init")?;
                output.write_all(b"\n").context("write trailing newline")?;
            }
            Payload::TopologyOk {} => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _, _>(())
}
