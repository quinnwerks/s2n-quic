// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    path::secret::{stateless_reset::Signer, Map},
    psk::{client::Provider as ClientProvider, server::Provider as ServerProvider},
    stream::{
        client::tokio::{Builder as ClientBuilder, Client},
        environment::tokio::adaptive::Thresholds,
        server::tokio::{Builder as ServerBuilder, Server},
        Protocol,
    },
    testing::{init_tracing, server_name, NoopSubscriber, TestTlsProvider},
};
use core::num::NonZeroUsize;
use s2n_quic_core::time::StdClock;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    runtime::Runtime,
};

const REQUEST_LEN: usize = 1_000;

fn map() -> Map {
    Map::new(
        Signer::new(b"default"),
        100,
        false,
        StdClock::default(),
        NoopSubscriber,
    )
}

fn runtime(workers: usize) -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap()
}

type TestServer = Server<ServerProvider, NoopSubscriber>;
type TestClient = Client<ClientProvider, NoopSubscriber>;

fn echo_server(rt: &Runtime, configure: impl FnOnce(ServerBuilder) -> ServerBuilder) -> TestServer {
    let server = rt.block_on(async {
        let handshake = ServerProvider::builder()
            .start(
                "127.0.0.1:0".parse().unwrap(),
                TestTlsProvider {},
                NoopSubscriber,
                map(),
            )
            .await
            .unwrap();
        configure(
            ServerBuilder::default()
                .with_address("127.0.0.1:0".parse().unwrap())
                .with_protocol(Protocol::Udp)
                .with_workers(NonZeroUsize::new(1).unwrap()),
        )
        .build(handshake, NoopSubscriber)
        .unwrap()
    });
    rt.spawn({
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

fn client(rt: &Runtime, configure: impl FnOnce(ClientBuilder) -> ClientBuilder) -> TestClient {
    let _guard = rt.enter();
    let handshake = ClientProvider::builder()
        .start(
            "127.0.0.1:0".parse().unwrap(),
            map(),
            TestTlsProvider {},
            NoopSubscriber,
            server_name(),
        )
        .unwrap();
    configure(ClientBuilder::default().with_background_threads(1))
        .build(handshake, NoopSubscriber)
        .unwrap()
}

async fn connect(
    client: &TestClient,
    server: &TestServer,
) -> crate::stream::application::Stream<NoopSubscriber> {
    client
        .connect(
            server.handshake_addr().unwrap(),
            server.acceptor_addr().unwrap(),
            server_name(),
        )
        .await
        .unwrap()
}

async fn round_trip(
    stream: &mut crate::stream::application::Stream<NoopSubscriber>,
    round: u8,
    response_len: usize,
) {
    let mut request = vec![round; REQUEST_LEN];
    request[..4].copy_from_slice(&(response_len as u32).to_le_bytes());
    stream.write_all(&request).await.unwrap();
    let mut response = vec![0; response_len];
    stream.read_exact(&mut response).await.unwrap();
    assert!(response.iter().all(|b| *b == round));
}

fn exchange_until(
    caller: &Runtime,
    client: &TestClient,
    server: &TestServer,
    mut until: impl FnMut(&Runtime) -> bool,
) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    caller.block_on(async {
        let mut stream = connect(client, server).await;
        let mut round = 0u8;
        while Instant::now() < deadline {
            for _ in 0..10 {
                round_trip(&mut stream, round, 2_000).await;
                round = round.wrapping_add(1);
            }
            if until(caller) {
                return true;
            }
        }
        false
    })
}

fn caller_runs_dc_tasks(caller: &Runtime, baseline: usize) -> bool {
    // the exchange itself runs in `block_on`, which isn't counted as a task
    caller.metrics().num_alive_tasks() > baseline
}

#[test]
fn stream_tasks_stay_off_the_caller_runtime_by_default() {
    init_tracing();
    let server_rt = runtime(2);
    let server = echo_server(&server_rt, |builder| builder);
    let caller = runtime(1);
    let client = client(&caller, |builder| builder);
    let baseline = caller.metrics().num_alive_tasks();

    let mut rounds = 0;
    assert!(exchange_until(&caller, &client, &server, |caller| {
        assert!(!caller_runs_dc_tasks(caller, baseline));
        rounds += 1;
        rounds == 100
    }));
}

#[test]
fn stream_tasks_move_to_an_idle_caller_runtime() {
    init_tracing();
    let server_rt = runtime(2);
    let server = echo_server(&server_rt, |builder| builder);
    let caller = runtime(1);
    let client = client(&caller, |builder| {
        builder.with_adaptive_placement(caller.handle().clone(), Thresholds::default())
    });
    let baseline = caller.metrics().num_alive_tasks();

    assert!(exchange_until(&caller, &client, &server, |caller| {
        caller_runs_dc_tasks(caller, baseline)
    }));
}

#[test]
fn stream_tasks_leave_a_saturated_caller_runtime() {
    init_tracing();
    let server_rt = runtime(2);
    let server = echo_server(&server_rt, |builder| builder);
    let caller = runtime(1);
    let client = client(&caller, |builder| {
        builder.with_adaptive_placement(caller.handle().clone(), Thresholds::default())
    });
    let baseline = caller.metrics().num_alive_tasks();
    assert!(exchange_until(&caller, &client, &server, |caller| {
        caller_runs_dc_tasks(caller, baseline)
    }));

    let stop = Arc::new(AtomicBool::new(false));
    caller.spawn({
        let stop = stop.clone();
        async move {
            while !stop.load(Ordering::Relaxed) {
                let spin = Instant::now();
                while spin.elapsed() < Duration::from_micros(20) {}
                tokio::task::yield_now().await;
            }
        }
    });
    let spinner = 1;
    let left = exchange_until(&caller, &client, &server, |caller| {
        !caller_runs_dc_tasks(caller, baseline + spinner)
    });
    stop.store(true, Ordering::Relaxed);
    assert!(left);
}

#[test]
fn adaptive_placement_on_both_sides_with_lazy_worker_wake() {
    init_tracing();
    let server_rt = runtime(2);
    let server = echo_server(&server_rt, |builder| {
        builder
            .with_adaptive_placement(server_rt.handle().clone(), Thresholds::default())
            .with_lazy_worker_wake(true)
    });
    let caller = runtime(2);
    let client = client(&caller, |builder| {
        builder
            .with_adaptive_placement(caller.handle().clone(), Thresholds::default())
            .with_lazy_worker_wake(true)
    });

    caller.block_on(async {
        let mut tasks = vec![];
        for _ in 0..4 {
            let mut stream = connect(&client, &server).await;
            tasks.push(tokio::spawn(async move {
                for round in 0..20u8 {
                    round_trip(&mut stream, round, 2_000).await;
                }
                round_trip(&mut stream, 0xff, 4 * 1024 * 1024).await;
            }));
        }
        tokio::time::timeout(Duration::from_secs(30), async {
            for task in tasks {
                task.await.unwrap();
            }
        })
        .await
        .unwrap();
    });
}
