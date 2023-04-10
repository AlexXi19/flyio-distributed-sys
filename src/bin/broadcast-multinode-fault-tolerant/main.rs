use flyio_distributed_sys::*;

use serde::{Deserialize, Serialize};
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
    neighbors: HashMap<String, HashSet<u32>>,
    neighbor_names: Vec<String>,
    curr_node: String,
}

impl Node<(), Payload> for BroadcastNode {
    fn from_init(
        _state: (),
        init: Init,
        _tx: std::sync::mpsc::Sender<Event<Payload>>,
    ) -> anyhow::Result<Self> {
        let broadcast_node = BroadcastNode {
            id: 1,
            messages: HashSet::new(),
            neighbors: HashMap::new(),
            neighbor_names: vec![],
            curr_node: init.node_id,
        };

        Ok(broadcast_node)
    }

    fn step(&mut self, input: Event<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            panic!("got injected event when there's no event injection");
        };

        let message_src = input.src.clone();
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Broadcast { message } => {
                reply.body.payload = Payload::BroadcastOk {};
                reply.send(output)?;
                self.messages.insert(message);
                self.sync_with_neighbors(output)?
            }
            Payload::Gossip { seen } => {
                let neighbor_vec = match self.neighbors.get_mut(&message_src) {
                    Some(x) => x,
                    None => return Ok(()),
                };

                for message in seen.clone() {
                    self.messages.insert(message);
                    neighbor_vec.insert(message);
                }

                let mut gossip_ok = HashSet::new();
                for message in self.messages.clone().into_iter() {
                    // Need to send the seen back for the sender to know that we have seen it else it will keep sending it, making the message longer and longer
                    if !neighbor_vec.contains(&message) || seen.contains(&message) {
                        gossip_ok.insert(message);
                    }
                }

                reply.body.payload = Payload::GossipOk {
                    seen: gossip_ok.into_iter().collect(),
                };
                reply.send(output)?;
            }
            Payload::GossipOk { seen } => {
                let neighbor_vec = match self.neighbors.get_mut(&message_src) {
                    Some(x) => x,
                    None => return Ok(()),
                };
                for message in seen {
                    self.messages.insert(message);
                    neighbor_vec.insert(message);
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
                    self.neighbors.insert(node.clone(), HashSet::new());
                }
                self.neighbor_names = topology.clone();
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
        for node in &self.neighbor_names {
            let seen_values = match self.neighbors.get(node) {
                Some(x) => x,
                None => return Ok(()),
            };

            let mut unseen_values: Vec<u32> = vec![];
            for message in &self.messages {
                let message = *message;
                if !seen_values.contains(&message) {
                    unseen_values.extend(vec![message]);
                }
            }
            let gossip = Message {
                src: self.curr_node.clone(),
                dst: node.clone(),
                body: Body {
                    id: None,
                    in_reply_to: None,
                    payload: Payload::Gossip {
                        seen: unseen_values,
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
