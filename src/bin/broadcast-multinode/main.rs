use flyio_distributed_sys::*;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashSet, io::StdoutLock};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast { message: u32 },
    BroadcastOk {},
    Gossip { message: u32 },
    Read {},
    ReadOk { messages: Vec<u32> },
    Topology { topology: Value },
    TopologyOk {},
}

struct BroadcastNode {
    id: usize,
    topology: Option<Value>,
    messages: HashSet<u32>,
    other_nodes: Vec<String>,
    curr_node: String,
}

impl Node<(), Payload> for BroadcastNode {
    fn from_init(
        _state: (),
        init: Init,
        _tx: std::sync::mpsc::Sender<Event<Payload>>,
    ) -> anyhow::Result<Self> {
        let mut other_nodes = init.node_ids.clone();
        let index = init
            .node_ids
            .iter()
            .position(|x| *x == init.node_id)
            .unwrap();
        other_nodes.remove(index);

        Ok(BroadcastNode {
            id: 1,
            topology: None,
            messages: HashSet::new(),
            other_nodes,
            curr_node: init.node_id,
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
                reply.send(output)?;
                self.messages.insert(message);
                // Send message to other nodes
                for node in &self.other_nodes {
                    let gossip = Message {
                        src: self.curr_node.clone(),
                        dst: node.clone(),
                        body: Body {
                            id: None,
                            in_reply_to: None,
                            payload: Payload::Gossip { message },
                        },
                    };
                    gossip.send(output)?;
                }
            }
            Payload::Gossip { message } => {
                self.messages.insert(message);
            }
            Payload::BroadcastOk { .. } => {}
            Payload::Read {} => {
                reply.body.payload = Payload::ReadOk {
                    messages: self.messages.clone().into_iter().collect(),
                };
                reply.send(output)?;
            }
            Payload::ReadOk { .. } => {}
            Payload::Topology { topology } => {
                self.topology = Some(topology);
                reply.body.payload = Payload::TopologyOk {};
                reply.send(output)?
            }
            Payload::TopologyOk {} => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _, _>(())
}
