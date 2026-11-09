// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    event,
    packet::Tag,
    path::secret::{stateless_reset::Signer, Map},
    psk::{client::Provider as ClientProvider, server::Provider as ServerProvider},
    stream::{
        client::tokio::{Builder as ClientBuilder, Client as TokioClient},
        server::tokio::{Builder as ServerBuilder, Server as TokioServer},
        testing::{Client, Server},
        Protocol,
    },
    testing::{ext::*, init_tracing, server_name, sim, spawn, NoopSubscriber, TestTlsProvider},
};
use core::num::NonZeroUsize;
use s2n_codec::DecoderBuffer;
use s2n_quic_core::time::StdClock;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UdpSocket,
};
use tracing::{info_span, Instrument};

#[test]
fn other_half_keep_alive() {
    sim(|| {
        async move {
            let client = Client::builder().build();
            let mut stream = client.connect_sim("server:443").await.unwrap();

            for _ in 0..120 {
                stream.write_all(b"ping").await.unwrap();
                1.s().sleep().await;
            }
            stream.shutdown().await.unwrap();

            let mut response = vec![];
            stream.read_to_end(&mut response).await.unwrap();

            assert_eq!(response, b"pong!"[..]);
        }
        .group("client")
        .instrument(info_span!("client"))
        .primary()
        .spawn();

        async move {
            let server = Server::udp().port(443).build();

            while let Ok((mut stream, peer_addr)) = server.accept().await {
                async move {
                    let mut request = vec![];
                    stream.read_to_end(&mut request).await.unwrap();

                    stream.write_from_fin(&mut &b"pong!"[..]).await.unwrap();
                }
                .instrument(info_span!("stream", ?peer_addr))
                .primary()
                .spawn();
            }
        }
        .group("server")
        .instrument(info_span!("server"))
        .spawn();
    });
}

#[test]
fn server_no_response() {
    sim(|| {
        async move {
            let client = Client::builder().build();
            let mut stream = client.connect_sim("server:443").await.unwrap();

            stream.write_all(b"ping").await.unwrap();
            stream.shutdown().await.unwrap();

            let mut response = vec![];
            stream.read_to_end(&mut response).await.unwrap_err();
        }
        .group("client")
        .instrument(info_span!("client"))
        .primary()
        .spawn();

        async move {
            let server = Server::udp().port(443).build();

            while let Ok((mut stream, peer_addr)) = server.accept().await {
                async move {
                    let mut request = vec![];
                    stream.read_to_end(&mut request).await.unwrap();

                    // sleep long enough to trigger the idle timer
                    120.s().sleep().await;
                }
                .instrument(info_span!("stream", ?peer_addr))
                .primary()
                .spawn();
            }
        }
        .group("server")
        .instrument(info_span!("server"))
        .spawn();
    });
}

const REQUEST: &[u8] = b"request";
const RESPONSE: &[u8] = &[42; 245];

#[derive(Debug)]
struct ServerCounts {
    transmitted: u64,
    #[allow(dead_code, reason = "only printed in failure messages")]
    sender_errored: u64,
    receiver_errored: u64,
}

impl ServerCounts {
    fn of(subscriber: &event::testing::Subscriber) -> Self {
        Self {
            transmitted: subscriber.stream_packet_transmitted.load(Ordering::Relaxed),
            sender_errored: subscriber.stream_sender_errored.load(Ordering::Relaxed),
            receiver_errored: subscriber.stream_receiver_errored.load(Ordering::Relaxed),
        }
    }
}

/// The client opens its stream once its endpoint has been up for longer than the idle timeout,
/// and drops it as soon as it has the response, before the server's FIN arrives. The server
/// only finishes its half after reading the client's FIN.
#[test]
fn server_fin_is_answered_after_client_drops_stream() {
    let server_subscriber = Arc::new(OnceLock::new());
    let observed = server_subscriber.clone();

    sim(move || {
        async move {
            let client = Client::builder().build();

            45.s().sleep().await;

            let mut stream = client.connect_sim("server:443").await.unwrap();
            stream.write_all(REQUEST).await.unwrap();
            let mut response = vec![0; RESPONSE.len()];
            stream.read_exact(&mut response).await.unwrap();
            assert_eq!(response, RESPONSE);
            drop(stream);

            60.s().sleep().await;
        }
        .group("client")
        .primary()
        .spawn();

        async move {
            let server = Server::udp().port(443).build();
            let _ = observed.set(server.subscriber());

            while let Ok((mut stream, _addr)) = server.accept().await {
                spawn(async move {
                    let mut request = vec![0; REQUEST.len()];
                    stream.read_exact(&mut request).await.unwrap();
                    stream.write_all(RESPONSE).await.unwrap();

                    let mut rest = vec![];
                    stream.read_to_end(&mut rest).await.unwrap();
                    assert!(rest.is_empty());
                });
            }
        }
        .group("server")
        .spawn();
    });

    let server = ServerCounts::of(server_subscriber.get().unwrap());

    assert_eq!(
        server.transmitted, 2,
        "the server retransmitted its FIN to a client that never answered: {server:?}"
    );
    assert_eq!(
        server.receiver_errored, 0,
        "the server's stream idled out: {server:?}"
    );
}

type TestServer = TokioServer<ServerProvider, NoopSubscriber>;
type TestClient = TokioClient<ClientProvider, NoopSubscriber>;

const CHUNK: usize = 64 * 1024;

fn map() -> Map {
    Map::new(
        Signer::new(b"default"),
        100,
        false,
        StdClock::default(),
        NoopSubscriber,
    )
}

async fn server() -> TestServer {
    let handshake = ServerProvider::builder()
        .start(
            "127.0.0.1:0".parse().unwrap(),
            TestTlsProvider {},
            NoopSubscriber,
            map(),
        )
        .await
        .unwrap();
    let server = ServerBuilder::default()
        .with_address("127.0.0.1:0".parse().unwrap())
        .with_protocol(Protocol::Udp)
        .with_workers(NonZeroUsize::new(2).unwrap())
        .build(handshake, NoopSubscriber)
        .unwrap();
    tokio::spawn({
        let server = server.clone();
        async move {
            while let Ok((mut stream, _)) = server.accept().await {
                tokio::spawn(async move {
                    let mut len = [0u8; 8];
                    while stream.read_exact(&mut len).await.is_ok() {
                        let mut remaining = u64::from_le_bytes(len) as usize;
                        let chunk = vec![1u8; CHUNK];
                        while remaining > 0 {
                            let n = remaining.min(CHUNK);
                            if stream.write_all(&chunk[..n]).await.is_err() {
                                return;
                            }
                            remaining -= n;
                        }
                    }
                });
            }
        }
    });
    server
}

fn client(data_window: u64) -> TestClient {
    let handshake = ClientProvider::builder()
        .with_data_window(data_window)
        .start(
            "127.0.0.1:0".parse().unwrap(),
            map(),
            TestTlsProvider {},
            NoopSubscriber,
            server_name(),
        )
        .unwrap();
    ClientBuilder::default()
        .with_background_threads(1)
        .build(handshake, NoopSubscriber)
        .unwrap()
}

fn tag(packet: &[u8]) -> Option<Tag> {
    DecoderBuffer::new(packet)
        .decode::<Tag>()
        .ok()
        .map(|(tag, _)| tag)
}

#[derive(Default)]
struct Proxy {
    drop_client_control: AtomicBool,
    dropped_client_control: AtomicU64,
}

impl Proxy {
    async fn start(server: SocketAddr) -> (Arc<Self>, SocketAddr) {
        let proxy = Arc::new(Self::default());
        let front = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let addr = front.local_addr().unwrap();
        tokio::spawn({
            let proxy = proxy.clone();
            async move {
                let mut backs: HashMap<SocketAddr, (Arc<UdpSocket>, Arc<Mutex<SocketAddr>>)> =
                    HashMap::new();
                let mut buf = vec![0u8; u16::MAX as usize];
                while let Ok((len, client)) = front.recv_from(&mut buf).await {
                    let (back, peer) = match backs.get(&client) {
                        Some(back) => back.clone(),
                        None => {
                            let back = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
                            let peer = Arc::new(Mutex::new(server));
                            tokio::spawn(Self::downstream(
                                back.clone(),
                                peer.clone(),
                                front.clone(),
                                client,
                            ));
                            backs.insert(client, (back.clone(), peer.clone()));
                            (back, peer)
                        }
                    };
                    let packet = &buf[..len];
                    if matches!(tag(packet), Some(Tag::Control(_)))
                        && proxy.drop_client_control.load(Ordering::Relaxed)
                    {
                        proxy.dropped_client_control.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let peer = *peer.lock().unwrap();
                    let _ = back.send_to(packet, peer).await;
                }
            }
        });
        (proxy, addr)
    }

    async fn downstream(
        back: Arc<UdpSocket>,
        peer: Arc<Mutex<SocketAddr>>,
        front: Arc<UdpSocket>,
        client: SocketAddr,
    ) {
        let mut buf = vec![0u8; u16::MAX as usize];
        while let Ok((len, from)) = back.recv_from(&mut buf).await {
            *peer.lock().unwrap() = from;
            let _ = front.send_to(&buf[..len], client).await;
        }
    }
}

async fn request(stream: &mut crate::stream::application::Stream<NoopSubscriber>, len: usize) {
    stream.write_all(&(len as u64).to_le_bytes()).await.unwrap();
}

async fn read_response(
    stream: &mut crate::stream::application::Stream<NoopSubscriber>,
    mut remaining: usize,
    proxy: &Proxy,
    what: &str,
) {
    let mut buf = vec![0u8; CHUNK];
    while remaining > 0 {
        let len = remaining.min(CHUNK);
        let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf[..len]))
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{what}: stalled with {remaining} bytes left after dropping {} client control packets",
                    proxy.dropped_client_control.load(Ordering::Relaxed),
                )
            })
            .unwrap();
        assert_ne!(n, 0, "{what}: unexpected end of stream");
        assert!(buf[..n].iter().all(|b| *b == 1));
        remaining -= n;
    }
}

/// The sender has filled the client's flow-control window and everything it sent was ACKed, so
/// it has nothing in flight. The client's MAX_DATA updates for the window it frees are all lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_max_data_is_recovered() {
    init_tracing();
    const WINDOW: usize = 4 * CHUNK;
    let server = server().await;
    let client = client(WINDOW as u64);
    let (proxy, acceptor) = Proxy::start(server.acceptor_addr().unwrap()).await;
    let mut stream = client
        .connect(server.handshake_addr().unwrap(), acceptor, server_name())
        .await
        .unwrap();

    request(&mut stream, CHUNK).await;
    read_response(&mut stream, CHUNK, &proxy, "warm-up").await;

    for round in 0..4 {
        request(&mut stream, 4 * WINDOW).await;
        // let the sender fill the window and get every packet ACKed
        tokio::time::sleep(Duration::from_millis(200)).await;
        proxy.drop_client_control.store(true, Ordering::Relaxed);
        // a loaded host may not have filled the window yet, and that data needs ACKs to arrive
        let restore = tokio::spawn({
            let proxy = proxy.clone();
            async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                proxy.drop_client_control.store(false, Ordering::Relaxed);
            }
        });
        read_response(
            &mut stream,
            WINDOW,
            &proxy,
            &format!("round {round} window"),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        restore.abort();
        proxy.drop_client_control.store(false, Ordering::Relaxed);
        read_response(
            &mut stream,
            3 * WINDOW,
            &proxy,
            &format!("round {round} rest"),
        )
        .await;
    }
}
