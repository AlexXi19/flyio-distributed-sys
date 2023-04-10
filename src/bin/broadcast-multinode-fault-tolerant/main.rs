use flyio_distributed_sys::*;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    io::StdoutLock,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: u32,
    },
    BroadcastOk {},
    Gossip {
        seen: Vec<u32>,
    },
    GossipOk {
        seen: Vec<u32>,
    },
    Read {},
    ReadOk {
        messages: Vec<u32>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk {},
}

struct BroadcastNode {
    id: usize,
    messages: HashSet<u32>,
    neighbors: HashMap<String, Vec<String>>,
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
            messages: HashSet::new(),
            neighbors: HashMap::new(),
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
                self.sync_with_neighbors(output)?
            }
            Payload::Gossip { seen } => {
                reply.body.payload = Payload::GossipOk {
                    seen: self.messages.clone().into_iter().collect(),
                };
                reply.send(output)?;
                for message in seen {
                    self.messages.insert(message);
                }
            }
            Payload::GossipOk { seen } => {
                for message in seen {
                    self.messages.insert(message);
                }
            }
            Payload::BroadcastOk { .. } => {}
            Payload::Read {} => {
                self.sync_with_neighbors(output)?;
                reply.body.payload = Payload::ReadOk {
                    messages: self.messages.clone().into_iter().collect(),
                };
                reply.send(output)?;
            }
            Payload::ReadOk { .. } => {}
            Payload::Topology { topology } => {
                eprintln!("{:?}", topology);
                let empty_vec = vec![];
                let topology = topology.get(&self.curr_node).unwrap_or(&empty_vec);
                for node in topology {
                    self.neighbors.insert(node.clone(), vec![]);
                }
                reply.body.payload = Payload::TopologyOk {};
                reply.send(output)?
            }
            Payload::TopologyOk {} => {}
        }

        Ok(())
    }
}

impl BroadcastNode {
    fn sync_with_neighbors(&self, output: &mut StdoutLock) -> anyhow::Result<()> {
        for node in self.neighbors.keys().into_iter() {
            let gossip = Message {
                src: self.curr_node.clone(),
                dst: node.clone(),
                body: Body {
                    id: None,
                    in_reply_to: None,
                    payload: Payload::Gossip {
                        seen: self.messages.clone().into_iter().collect(),
                    },
                },
            };
            gossip.send(output)?;
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _, _>(())
}
