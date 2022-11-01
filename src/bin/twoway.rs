use futures::{
    future::{self, Ready},
    prelude::*,
};
use std::{io, net::SocketAddr, time::Duration};
use tarpc::{
    client, context,
    server::{BaseChannel, Channel},
};
use tokio::net::TcpStream;
use tokio_serde::formats::Json;

#[tarpc::service]
pub trait Ping {
    async fn ping(count: u64);
}

#[derive(Clone)]
struct PingServer {
    peer: PingClient,
    peer_addr: SocketAddr,
}

impl Ping for PingServer {
    type PingFut = Ready<()>;

    fn ping(mut self, _: context::Context, count: u64) -> Self::PingFut {
        println!("{:?}: ping {}", self.peer_addr, count);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if let Err(e) = self.peer.ping(context::current(), count + 1).await {
                eprintln!("Ping failed: {}", e);
            }
        });
        future::ready(())
    }
}

// The message sent over the wire that allows the peer to distinguish between requests and responses.
#[derive(serde::Serialize, serde::Deserialize)]
enum Message<Req, Resp> {
    ClientMessage(tarpc::ClientMessage<Req>),
    Response(tarpc::Response<Resp>),
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut transport = tarpc::serde_transport::tcp::listen("localhost:0", Json::default).await?;
    let addr = transport.local_addr();

    // This function, make_server, multiplexes two logical transports over the given transport.
    let make_server = |mut peer: tarpc::serde_transport::Transport<TcpStream, Message<_, _>, Message<_, _>, _>, count| async move {
        let peer_addr = match peer.peer_addr() {
            Ok(peer) => peer,
            Err(e) => {
                eprintln!("Could not get peer addr: {}", e);
                return;
            }
        };
        let (mut server, server_) = tarpc::transport::channel::unbounded();
        let (mut client, client_) = tarpc::transport::channel::unbounded();
        tokio::spawn(async move {
            let e: anyhow::Result<()> = async move {
                loop {
                    tokio::select! {
                        // Handle incoming requests and responses
                        msg = peer.next() => {
                            match msg {
                                Some(msg) => match msg? {
                                    Message::ClientMessage(req) => {
                                        println!("{:?}: received request: {:?}", peer_addr, req);
                                        server.send(req).await?
                                    },
                                    Message::Response(resp) => client.send(resp).await?,
                                }
                                None => return Ok(()),
                            }
                        }
                        // Forward outbound requests
                        req = client.next() => {
                            match req {
                                Some(req) => peer.send(Message::ClientMessage(req?)).await?,
                                None => return Ok(()),
                            }
                        }
                        // Forward outbound responses.
                        resp = server.next() => {
                            match resp {
                                Some(resp) => peer.send(Message::Response(resp?)).await?,
                                None => return Ok(()),
                            }
                        }
                    }
                }
            }.await;
            eprintln!("Transport died: {:?}", e);
        });

        let mut peer = PingClient::new(client::Config::default(), client_).spawn();
        if let Some(count) = count {
            if let Err(e) = peer.ping(context::current(), count).await {
                eprintln!("Could not ping peer: {}", e);
            }
        }
        BaseChannel::with_defaults(server_)
            .execute(PingServer { peer, peer_addr, }.serve())
            .await;
    };
    let server = async move {
        let client = transport.next().await.unwrap().unwrap();
        make_server(client, None).await
    };
    tokio::spawn(server);

    let server = tarpc::serde_transport::tcp::connect(addr, Json::default).await?;
    let client = make_server(server, Some(1));
    client.await;

    Ok(())
}