use flyio_distributed_sys::*;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock, Write};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate {},
    GenerateOk { id: String },
}

struct GenerateIdNode {
    id: usize,
    timestamp: u64,
}

impl Node<(), Payload> for GenerateIdNode {
    fn from_init(
        _state: (),
        _init: Init,
        _tx: std::sync::mpsc::Sender<Event<Payload>>,
    ) -> anyhow::Result<Self> {
        Ok(GenerateIdNode {
            id: 1,
            timestamp: 0,
        })
    }

    fn step(&mut self, input: Event<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            panic!("got injected event when there's no event injection");
        };
        let node_id = input.dst.clone();

        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Generate {} => {
                let id = format!("{}-{}", node_id, self.timestamp);
                reply.body.payload = Payload::GenerateOk { id };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to init")?;
                output.write_all(b"\n").context("write trailing newline")?;
                self.timestamp += 1;
            }
            Payload::GenerateOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, GenerateIdNode, _, _>(())
}
