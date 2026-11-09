// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    clock::Timer,
    event, msg,
    stream::{
        recv::{
            ack_offload::Offload,
            application::{Inner, LocalState, Reader},
        },
        runtime,
        shared::ArcShared,
        socket,
    },
};
use core::mem::ManuallyDrop;
use s2n_quic_core::endpoint;
use std::sync::Arc;

use super::{AckMode, ReadMode};

pub struct Builder<Sub> {
    endpoint: endpoint::Type,
    runtime: runtime::ArcHandle<Sub>,
    ack_mode: AckMode,
    ack_offload: Option<Arc<Offload>>,
}

impl<Sub> Builder<Sub> {
    #[inline]
    pub fn new(endpoint: endpoint::Type, runtime: runtime::ArcHandle<Sub>) -> Self {
        Self {
            endpoint,
            runtime,
            ack_mode: AckMode::default(),
            ack_offload: None,
        }
    }

    #[inline]
    pub fn with_ack_mode(mut self, ack_mode: AckMode) -> Self {
        self.ack_mode = ack_mode;
        self
    }

    #[inline]
    pub fn with_ack_offload(mut self, ack_offload: Option<Arc<Offload>>) -> Self {
        self.ack_offload = ack_offload;
        self
    }
}

impl<Sub> Builder<Sub>
where
    Sub: event::Subscriber,
{
    #[inline]
    pub fn build(self, shared: ArcShared<Sub>, sockets: socket::ArcApplication) -> Reader<Sub> {
        let Self {
            endpoint,
            runtime,
            ack_mode,
            ack_offload,
        } = self;

        let remote_addr = shared.remote_addr();
        // we only need a timer for unreliable transports
        let is_reliable = sockets.features().is_reliable();
        let timer = if is_reliable {
            None
        } else {
            Some(Timer::new(&shared.clock))
        };
        let gso = shared.gso.clone();
        let send_buffer = msg::send::Message::new(remote_addr, gso);
        // If the transport is reliable then it's handling ACKs. Otherwise, the application is sending
        // ACKs so we want to do a little more compute per `read` call, if the application buffer allows
        // for it.
        let read_mode = if is_reliable {
            ReadMode::Once
        } else {
            ReadMode::UntilFull
        };
        let local_state = match endpoint {
            // reliable transports on the client need to read at least one packet in order to
            // process secret control packets
            endpoint::Type::Client if is_reliable => LocalState::Ready,
            // unreliable transports use background workers to drive state
            endpoint::Type::Client => LocalState::Reading,
            // the server acceptor already read from the socket at least once
            endpoint::Type::Server => LocalState::Reading,
        };

        Reader(ManuallyDrop::new(Box::new(Inner {
            shared,
            sockets,
            send_buffer,
            read_mode,
            ack_mode,
            ack_offload,
            timer,
            local_state,
            runtime,
        })))
    }
}
