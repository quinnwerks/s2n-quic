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

const REQUEST_LEN: usize = 1_000;

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

async fn echo_server() -> TestServer {
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
        .build(handshake, NoopSubscriber)
        .unwrap();
    tokio::spawn({
        let server = server.clone();
        async move {
            while let Ok((mut stream, _)) = server.accept().await {
                tokio::spawn(async move {
                    let mut request = vec![0; REQUEST_LEN];
                    while stream.read_exact(&mut request).await.is_ok() {
                        let len = u32::from_le_bytes(request[..4].try_into().unwrap()) as usize;
                        if stream.write_all(&vec![request[4]; len]).await.is_err() {
                            return;
                        }
                    }
                });
            }
        }
    });
    server
}

fn deferred_ack_client() -> TestClient {
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
        .with_ack_mode(AckMode::Deferred)
        .build(handshake, NoopSubscriber)
        .unwrap()
}

async fn exchange(rounds: usize, response_len: usize) {
    let server = echo_server().await;
    let client = deferred_ack_client();
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
            let mut request = vec![round; REQUEST_LEN];
            request[..4].copy_from_slice(&(response_len as u32).to_le_bytes());
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
async fn request_response_completes_with_deferred_acks() {
    init_tracing();
    exchange(200, 2_000).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flow_control_advances_with_deferred_acks() {
    init_tracing();
    exchange(4, 16 * 1024 * 1024).await;
}

#[tokio::test(flavor = "current_thread")]
async fn deferred_acks_run_on_a_current_thread_runtime() {
    init_tracing();
    exchange(4, 4 * 1024 * 1024).await;
}
