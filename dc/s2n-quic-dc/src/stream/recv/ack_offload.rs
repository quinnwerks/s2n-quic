// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{io, sync::mpsc, thread};

pub type Job = Box<dyn FnOnce() + Send>;

/// A thread that sends stream ACKs on behalf of readers using [`super::shared::AckMode::Offload`]
///
/// The thread exits once the `Offload` and every stream holding it have been dropped.
pub struct Offload {
    queue: mpsc::Sender<Job>,
}

impl Offload {
    pub fn new(thread_name: String) -> io::Result<Self> {
        let (queue, jobs) = mpsc::channel::<Job>();
        thread::Builder::new().name(thread_name).spawn(move || {
            while let Ok(job) = jobs.recv() {
                job();
            }
        })?;
        Ok(Self { queue })
    }

    #[inline]
    pub fn submit(&self, job: Job) -> Result<(), Job> {
        self.queue.send(job).map_err(|mpsc::SendError(job)| job)
    }
}
