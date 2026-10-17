use std::{any::Any, sync::{atomic::Ordering, Arc}, thread::{self, JoinHandle}};

use super::sound::Shared;

struct Worker {
	shared: Arc<Shared>,
	thread: JoinHandle<()>,
}

/// Owns streaming decoder threads on the gameplay thread, not the audio callback.
///
/// Pass this owner to [`super::StreamingSoundData::with_decoder_owner`]. Stop and
/// join it after dropping the audio manager/backend. Dropping a sound handle
/// retains its existing semantics and does not stop playback.
#[derive(Default)]
pub struct DecoderOwner {
	workers: Vec<Worker>,
	panic: Option<Box<dyn Any + Send + 'static>>,
}

impl DecoderOwner {
	/// Creates an owner without starting threads.
	pub fn new() -> Self { Self::default() }

	pub(super) fn register(&mut self, thread: JoinHandle<()>, shared: Arc<Shared>) {
		let mut index = 0;
		while index < self.workers.len() {
			if self.workers[index].thread.is_finished() {
				let worker = self.workers.swap_remove(index);
				if let Err(panic) = worker.thread.join() {
					if self.panic.is_none() { self.panic = Some(panic); }
				}
			} else { index += 1; }
		}
		self.workers.push(Worker { shared, thread });
	}

	/// Cancels all decoders before joining any. Never call from an audio callback.
	/// Returns the first worker panic after every worker has been joined.
	pub fn stop_and_join(&mut self) -> thread::Result<()> {
		for worker in &self.workers {
			worker.shared.decoder_stopped.store(true, Ordering::Release);
			worker.thread.thread().unpark();
		}
		for worker in self.workers.drain(..) {
			if let Err(panic) = worker.thread.join() {
				if self.panic.is_none() { self.panic = Some(panic); }
			}
		}
		match self.panic.take() { Some(panic) => Err(panic), None => Ok(()) }
	}
}

impl Drop for DecoderOwner {
	fn drop(&mut self) { let _ = self.stop_and_join(); }
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::{sync::atomic::AtomicUsize, time::Duration};

	fn register_waiter(owner: &mut DecoderOwner, finished: Arc<AtomicUsize>) {
		let shared = Arc::new(Shared::new());
		let worker_shared = shared.clone();
		let worker = thread::spawn(move || {
			while !worker_shared.decoder_stopped.load(Ordering::Acquire) {
				thread::park_timeout(Duration::from_secs(60));
			}
			finished.fetch_add(1, Ordering::Release);
		});
		owner.register(worker, shared);
	}

	#[test]
	fn stop_wakes_and_joins_all_workers() {
		let finished = Arc::new(AtomicUsize::new(0));
		let mut owner = DecoderOwner::new();
		register_waiter(&mut owner, finished.clone());
		register_waiter(&mut owner, finished.clone());
		assert!(owner.stop_and_join().is_ok());
		assert_eq!(finished.load(Ordering::Acquire), 2);
		assert!(owner.stop_and_join().is_ok());
	}

	#[test]
	fn panic_does_not_skip_remaining_joins() {
		let finished = Arc::new(AtomicUsize::new(0));
		let mut owner = DecoderOwner::new();
		owner.register(thread::spawn(|| panic!("decoder panic")), Arc::new(Shared::new()));
		register_waiter(&mut owner, finished.clone());
		assert!(owner.stop_and_join().is_err());
		assert_eq!(finished.load(Ordering::Acquire), 1);
	}

	#[test]
	fn drop_joins_outstanding_workers() {
		let finished = Arc::new(AtomicUsize::new(0));
		let mut owner = DecoderOwner::new();
		register_waiter(&mut owner, finished.clone());
		drop(owner);
		assert_eq!(finished.load(Ordering::Acquire), 1);
	}
}
