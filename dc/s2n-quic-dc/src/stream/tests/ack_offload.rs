// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    path::secret::{stateless_reset::Signer, Map},
    psk::{client::Provider as ClientProvider, server::Provider as ServerProvider},
    stream::{
        client::tokio::{Builder as ClientBuilder, Client},
        recv::application::AckMode,
        server::tokio::{Builder as ServerBuilder, Server},
        Protocol,
    },
    testing::{init_tracing, server_name, NoopSubscriber, TestTlsProvider},
};
use core::num::NonZeroUsize;
use s2n_quic_core::time::StdClock;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type TestServer = Server<ServerProvider, NoopSubscriber>;
type TestClient = Client<ClientProvider, NoopSubscriber>;

fn map() -> Map {
    Map::new(
        Signer::new(b"default"),
        100,
        false,
        StdClock::default(),
        NoopSubscriber,
    )
}

async fn offload_server(handler_delay: Duration) -> TestServer {
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
        .with_workers(NonZeroUsize::new(1).unwrap())
        .with_ack_mode(AckMode::Offload)
        .build(handshake, NoopSubscriber)
        .unwrap();
    tokio::spawn({
        let server = server.clone();
        async move {
            while let Ok((mut stream, _)) = server.accept().await {
                tokio::spawn(async move {
                    let mut header = [0; 8];
                    while stream.read_exact(&mut header).await.is_ok() {
                        let request_len = u32::from_le_bytes(header[..4].try_into().unwrap());
                        let response_len = u32::from_le_bytes(header[4..].try_into().unwrap());
                        let mut request = vec![0; request_len as usize];
                        if stream.read_exact(&mut request).await.is_err() {
                            return;
                        }
                        // handle the request without yielding, like a CPU-bound handler would
                        std::thread::sleep(handler_delay);
                        let response = vec![request[0]; response_len as usize];
                        if stream.write_all(&response).await.is_err() {
                            return;
                        }
                    }
                });
            }
        }
    });
    server
}

fn client() -> TestClient {
    let handshake = ClientProvider::builder()
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

async fn exchange(rounds: usize, request_len: usize, response_len: usize, handler_delay: Duration) {
    let server = offload_server(handler_delay).await;
    let client = client();
    let mut stream = client
        .connect(
            server.handshake_addr().unwrap(),
            server.acceptor_addr().unwrap(),
            server_name(),
        )
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(30), async {
        for round in 0..rounds {
            let round = round as u8;
            let mut request = vec![round; 8 + request_len];
            request[..4].copy_from_slice(&(request_len as u32).to_le_bytes());
            request[4..8].copy_from_slice(&(response_len as u32).to_le_bytes());
            stream.write_all(&request).await.unwrap();
            let mut response = vec![0; response_len];
            stream.read_exact(&mut response).await.unwrap();
            assert!(response.iter().all(|b| *b == round));
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_response_completes_with_offloaded_acks() {
    init_tracing();
    exchange(200, 1_000, 2_000, Duration::ZERO).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flow_control_advances_with_offloaded_acks() {
    init_tracing();
    exchange(4, 16 * 1024 * 1024, 1_000, Duration::ZERO).await;
}

#[tokio::test(flavor = "current_thread")]
async fn offloaded_acks_with_a_handler_that_blocks_before_yielding() {
    init_tracing();
    exchange(20, 64 * 1024, 1_000, Duration::from_millis(5)).await;
}
