use super::*;

fn controller(thread: std::thread::JoinHandle<()>, stop: Arc<AtomicBool>) -> StreamManagerController {
	let (_, errors) = RingBuffer::new(1);
	StreamManagerController {
		should_drop: stop,
		num_stream_errors_discarded: Arc::new(AtomicU64::new(0)),
		handled_stream_error_consumer: Mutex::new(errors),
		thread: Some(thread),
	}
}

#[test]
fn explicit_shutdown_joins_worker_owned_resources_and_is_idempotent() {
	struct Resource(Arc<AtomicBool>);
	impl Drop for Resource {
		fn drop(&mut self) { self.0.store(false, Ordering::Release); }
	}
	let alive = Arc::new(AtomicBool::new(true));
	let resource = Resource(alive.clone());
	let stop = Arc::new(AtomicBool::new(false));
	let worker_stop = stop.clone();
	let thread = std::thread::spawn(move || {
		let _resource = resource;
		while !worker_stop.load(Ordering::Acquire) { std::thread::park(); }
	});
	let mut controller = controller(thread, stop);
	controller.stop_and_join().unwrap();
	assert!(!alive.load(Ordering::Acquire));
	controller.stop_and_join().unwrap();
}

#[test]
fn default_stop_does_not_join_a_busy_worker() {
	let (release, wait) = std::sync::mpsc::sync_channel(1);
	let thread = std::thread::spawn(move || { wait.recv().unwrap(); });
	let mut controller = controller(thread, Arc::new(AtomicBool::new(false)));
	controller.stop();
	release.send(()).unwrap();
	controller.stop_and_join().unwrap();
}

#[test]
fn explicit_shutdown_reports_worker_panic() {
	let thread = std::thread::spawn(|| panic!("worker failure"));
	let mut controller = controller(thread, Arc::new(AtomicBool::new(false)));
	assert!(matches!(controller.stop_and_join(), Err(Error::StreamManagerPanicked)));
	controller.stop_and_join().unwrap();
}
