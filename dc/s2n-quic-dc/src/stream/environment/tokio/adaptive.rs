// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use core::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
    time::Duration,
};
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::runtime::Handle;

const WINDOW: Duration = Duration::from_millis(1);
const CALM_WINDOWS: u32 = 8;

/// Busy ratios of the caller's runtime that move stream tasks between it and the dedicated
/// runtimes.
///
/// The busy ratio is the fraction of the sampling window the caller's workers spent running
/// tasks, averaged across workers.
#[derive(Clone, Copy, Debug)]
pub struct Thresholds {
    /// Tasks move to the dedicated runtimes once the caller's busy ratio exceeds this value
    pub leave_above: f64,
    /// Tasks move back to the caller's runtime once its busy ratio stays below this value
    pub enter_below: f64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            leave_above: 0.9,
            enter_below: 0.7,
        }
    }
}

pub struct Placement {
    caller: Handle,
    inline: AtomicBool,
    thresholds: Thresholds,
    sample: Mutex<Sample>,
}

struct Sample {
    at: Instant,
    busy: Duration,
    ratio: Option<f64>,
    calm: u32,
}

impl Placement {
    pub fn new(caller: Handle, thresholds: Thresholds) -> Arc<Self> {
        let busy = busy_total(&caller);
        Arc::new(Self {
            caller,
            inline: AtomicBool::new(false),
            thresholds,
            sample: Mutex::new(Sample {
                at: Instant::now(),
                busy,
                ratio: None,
                calm: 0,
            }),
        })
    }

    #[inline]
    pub fn is_inline(&self) -> bool {
        self.inline.load(Ordering::Relaxed)
    }

    pub fn refresh(&self) -> bool {
        let Ok(mut sample) = self.sample.try_lock() else {
            return self.is_inline();
        };
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(sample.at);
        if elapsed < WINDOW {
            return self.is_inline();
        }
        let busy = busy_total(&self.caller);
        // an idle gap says nothing about the load that follows it, and keeping the placement
        // across it means a saturating burst starts on the dedicated runtimes
        if elapsed > WINDOW * 4 {
            sample.at = now;
            sample.busy = busy;
            return self.is_inline();
        }
        let workers = self.caller.metrics().num_workers().max(1) as f64;
        let ratio =
            busy.saturating_sub(sample.busy).as_secs_f64() / (elapsed.as_secs_f64() * workers);
        let ratio = sample.ratio.map_or(ratio, |prev| (prev + ratio) / 2.0);
        sample.at = now;
        sample.busy = busy;
        sample.ratio = Some(ratio);

        // leave quickly and come back slowly: saturating the caller costs far more than a few
        // milliseconds of cross-thread wakes, and every move costs a re-registration
        let inline = self.is_inline();
        if inline {
            if ratio > self.thresholds.leave_above {
                sample.calm = 0;
                self.inline.store(false, Ordering::Relaxed);
                return false;
            }
        } else if ratio < self.thresholds.enter_below {
            sample.calm += 1;
            if sample.calm >= CALM_WINDOWS {
                sample.calm = 0;
                self.inline.store(true, Ordering::Relaxed);
                return true;
            }
        } else {
            sample.calm = 0;
        }
        inline
    }

    pub fn handle<'a>(&'a self, dedicated: &'a Handle) -> (&'a Handle, bool) {
        if self.is_inline() {
            (&self.caller, true)
        } else {
            (dedicated, false)
        }
    }
}

#[cfg(target_has_atomic = "64")]
fn busy_total(handle: &Handle) -> Duration {
    let metrics = handle.metrics();
    (0..metrics.num_workers())
        .map(|worker| metrics.worker_total_busy_duration(worker))
        .sum()
}

#[cfg(not(target_has_atomic = "64"))]
fn busy_total(_handle: &Handle) -> Duration {
    Duration::ZERO
}

type Task = Pin<Box<dyn Future<Output = ()> + Send>>;

pub fn spawn(placement: Arc<Placement>, dedicated: Handle, task: Task) {
    let (handle, inline) = placement.handle(&dedicated);
    let handle = handle.clone();
    handle.spawn(Hop {
        task: Some(task),
        placement,
        dedicated,
        inline,
    });
}

struct Hop {
    task: Option<Task>,
    placement: Arc<Placement>,
    dedicated: Handle,
    inline: bool,
}

impl Future for Hop {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        if this.placement.is_inline() == this.inline {
            return match this.task.as_mut() {
                Some(task) => task.as_mut().poll(cx),
                None => Poll::Ready(()),
            };
        }
        if let Some(task) = this.task.take() {
            spawn(this.placement.clone(), this.dedicated.clone(), task);
        }
        Poll::Ready(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refresh_until(placement: &Placement, inline: bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while placement.refresh() != inline && Instant::now() < deadline {
            std::thread::sleep(WINDOW + WINDOW / 4);
        }
        placement.is_inline() == inline
    }

    #[test]
    fn leaves_a_saturated_runtime_and_returns_once_it_quiets() {
        let caller = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let placement = Placement::new(caller.handle().clone(), Thresholds::default());
        assert!(!placement.is_inline());
        assert!(refresh_until(&placement, true));

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
        assert!(refresh_until(&placement, false));

        stop.store(true, Ordering::Relaxed);
        std::thread::sleep(WINDOW * 10);
        assert!(!placement.refresh());
        assert!(refresh_until(&placement, true));
    }
}
