// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::stream::{testing::dcquic::Context, Protocol};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const STREAMS: usize = 300;
const REQUEST_LEN: usize = 100;
const RESPONSE_LEN: usize = 64 * 1024;

#[tokio::test]
async fn large_responses_on_many_streams() {
    let context = Context::new(Protocol::Udp).await;
    let mut streams = vec![];

    for idx in 0..STREAMS {
        let (mut client, mut server) = context.pair().await;

        let server_task = tokio::spawn(async move {
            let mut request = [0u8; REQUEST_LEN];
            server.read_exact(&mut request).await?;
            server.write_all(&[1u8; RESPONSE_LEN]).await?;
            Ok::<_, std::io::Error>(server)
        });

        let mut response = vec![0u8; RESPONSE_LEN];
        tokio::time::timeout(Duration::from_secs(5), async {
            client.write_all(&[0u8; REQUEST_LEN]).await?;
            client.read_exact(&mut response).await?;
            Ok::<_, std::io::Error>(())
        })
        .await
        .unwrap_or_else(|_| panic!("stream {idx} stalled"))
        .unwrap();
        assert!(response.iter().all(|&b| b == 1));

        let server = server_task.await.unwrap().unwrap();
        streams.push((client, server));
    }
}
