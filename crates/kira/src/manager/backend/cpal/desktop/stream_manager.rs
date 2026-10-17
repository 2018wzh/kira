mod renderer_wrapper;

use std::{
	sync::{
		atomic::{AtomicBool, Ordering},
		Arc,
	},
	time::Duration,
};

use crate::manager::backend::Renderer;
use cpal::{
	traits::{DeviceTrait, HostTrait, StreamTrait},
	BufferSize, Device, Stream, StreamConfig, StreamError,
};
use ringbuf::{HeapConsumer, HeapRb};

use super::super::Error;

use self::renderer_wrapper::RendererWrapper;

const CHECK_STREAM_INTERVAL: Duration = Duration::from_millis(500);

#[allow(clippy::large_enum_variant)]
enum State {
	Empty,
	Idle {
		renderer: Renderer,
	},
	Running {
		stream: Stream,
		stream_error_consumer: HeapConsumer<StreamError>,
		renderer_consumer: HeapConsumer<Renderer>,
	},
}

pub(super) struct StreamManagerController {
	should_drop: Arc<AtomicBool>,
	worker: Option<std::thread::JoinHandle<Result<(), Error>>>,
}

impl StreamManagerController {
	pub fn stop(&mut self) -> Result<(), Error> {
		self.should_drop.store(true, Ordering::Release);
		if let Some(worker) = self.worker.take() {
			worker.thread().unpark();
			worker.join().map_err(|_| Error::WorkerPanicked)??;
		}
		Ok(())
	}
	pub fn check(&mut self) -> Result<(), Error> {
		if self.worker.as_ref().is_some_and(|worker| worker.is_finished()) {
			return self.stop();
		}
		Ok(())
	}
}

impl Drop for StreamManagerController {
	fn drop(&mut self) { let _ = self.stop(); }
}

/// Starts a cpal stream and restarts it if needed
/// in the case of device changes or disconnections.
pub(super) struct StreamManager {
	state: State,
	device_name: String,
	sample_rate: u32,
	custom_device: bool,
	buffer_size: BufferSize,
	suspended: Arc<AtomicBool>,
}

impl StreamManager {
	pub fn start(
		renderer: Renderer,
		device: Device,
		mut config: StreamConfig,
		custom_device: bool,
		buffer_size: BufferSize,
		suspended: Arc<AtomicBool>,
	) -> Result<StreamManagerController, Error> {
		let should_drop = Arc::new(AtomicBool::new(false));
		let worker_stop = Arc::clone(&should_drop);
		let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
		let worker = std::thread::Builder::new().name("kira-cpal".into()).spawn(move || {
			let mut stream_manager = StreamManager {
				state: State::Idle { renderer },
				device_name: device_name(&device),
				sample_rate: config.sample_rate.0,
				custom_device,
				buffer_size,
				suspended,
			};
			if let Err(error) = stream_manager.start_stream(&device, &mut config) {
				let _ = ready_tx.send(Err(error));
				return Ok(());
			}
			if ready_tx.send(Ok(())).is_err() { return Ok(()); }
			while !worker_stop.load(Ordering::Acquire) {
				std::thread::park_timeout(CHECK_STREAM_INTERVAL);
				if worker_stop.load(Ordering::Acquire) { break; }
				stream_manager.check_stream()?;
			}
			Ok(())
		}).map_err(Error::WorkerSpawn)?;
		let mut controller = StreamManagerController { should_drop, worker: Some(worker) };
		match ready_rx.recv() {
			Ok(Ok(())) => Ok(controller),
			Ok(Err(error)) => { let _ = controller.stop(); Err(error) }
			Err(_) => { controller.stop()?; Err(Error::WorkerStopped) }
		}
	}

	/// Restarts the stream if the audio device gets disconnected.
	fn check_stream(&mut self) -> Result<(), Error> {
		if let State::Running {
			stream_error_consumer,
			..
		} = &mut self.state
		{
			// check for device disconnection
			if let Some(StreamError::DeviceNotAvailable) = stream_error_consumer.pop() {
				self.stop_stream();
				if let Ok((device, mut config)) = default_device_and_config() {
					self.start_stream(&device, &mut config)?;
				}
			}
			// check for device changes if a custom device hasn't been specified
			// Disabled on macos due to audio artifacts that seem to occur when the device is
			// queried while playing.
			// see: https://github.com/tesselode/kira/issues/38
			#[cfg(not(target_os = "macos"))]
			if !self.custom_device {
				if let Ok((device, mut config)) = default_device_and_config() {
					let device_name = device_name(&device);
					let sample_rate = config.sample_rate.0;
					if device_name != self.device_name || sample_rate != self.sample_rate {
						self.stop_stream();
						self.start_stream(&device, &mut config)?;
					}
				}
			}
		}
		Ok(())
	}

	fn start_stream(&mut self, device: &Device, config: &mut StreamConfig) -> Result<(), Error> {
		let mut renderer =
			if let State::Idle { renderer } = std::mem::replace(&mut self.state, State::Empty) {
				renderer
			} else {
				panic!("trying to start a stream when the stream manager is not idle");
			};
		config.buffer_size = self.buffer_size; // this won't change anything if the buffer size is BufferSize::Default
		let device_name = device_name(device);
		let sample_rate = config.sample_rate.0;
		if sample_rate != self.sample_rate {
			renderer.on_change_sample_rate(sample_rate);
		}
		self.device_name = device_name;
		self.sample_rate = sample_rate;
		let (mut renderer_wrapper, renderer_consumer) = RendererWrapper::new(renderer);
		let (mut stream_error_producer, stream_error_consumer) = HeapRb::new(1).split();
		let channels = config.channels;
		let suspended = Arc::clone(&self.suspended);
		let stream = device.build_output_stream(
			config,
			move |data: &mut [f32], _| {
				if suspended.load(Ordering::Acquire) { data.fill(0.0); return; }
				#[cfg(feature = "assert_no_alloc")]
				assert_no_alloc::assert_no_alloc(|| {
					process_renderer(&mut renderer_wrapper, data, channels);
				});
				#[cfg(not(feature = "assert_no_alloc"))]
				process_renderer(&mut renderer_wrapper, data, channels);
			},
			move |error| {
				stream_error_producer
					.push(error)
					.expect("Stream error producer is full");
			},
			None,
		)?;
		stream.play()?;
		self.state = State::Running {
			stream,
			stream_error_consumer,
			renderer_consumer,
		};
		Ok(())
	}

	fn stop_stream(&mut self) {
		if let State::Running {
			mut renderer_consumer,
			stream,
			..
		} = std::mem::replace(&mut self.state, State::Empty)
		{
			drop(stream);
			let renderer = renderer_consumer
				.pop()
				.expect("Could not retrieve the renderer after dropping a stream");
			self.state = State::Idle { renderer };
		} else {
			panic!("Trying to stop the stream when it's not running")
		}
	}
}

fn default_device_and_config() -> Result<(Device, StreamConfig), Error> {
	let host = cpal::default_host();
	let device = host
		.default_output_device()
		.ok_or(Error::NoDefaultOutputDevice)?;
	let config = device.default_output_config()?.config();
	Ok((device, config))
}

fn device_name(device: &Device) -> String {
	device
		.name()
		.unwrap_or_else(|_| "device name unavailable".to_string())
}

fn process_renderer(renderer_wrapper: &mut RendererWrapper, data: &mut [f32], channels: u16) {
	renderer_wrapper.on_start_processing();
	for frame in data.chunks_exact_mut(channels as usize) {
		let out = renderer_wrapper.process();
		if channels == 1 {
			frame[0] = (out.left + out.right) / 2.0;
		} else {
			frame[0] = out.left;
			frame[1] = out.right;
			/*
				if there's more channels, send silence to them. if we don't,
				we might get bad sounds outputted to those channels.
				(https://github.com/tesselode/kira/issues/50)
			*/
			for channel in frame.iter_mut().skip(2) {
				*channel = 0.0;
			}
		}
	}
}

#[cfg(test)]
mod lifetime_tests {
	use super::*;
	#[test]
	fn controller_stop_wakes_and_joins_worker() {
		let stopped = Arc::new(AtomicBool::new(false));
		let finished = Arc::new(AtomicBool::new(false));
		let worker_stop = Arc::clone(&stopped);
		let worker_finished = Arc::clone(&finished);
		let worker = std::thread::spawn(move || {
			while !worker_stop.load(Ordering::Acquire) {
				std::thread::park_timeout(Duration::from_secs(60));
			}
			worker_finished.store(true, Ordering::Release);
			Ok(())
		});
		let mut controller = StreamManagerController { should_drop: stopped, worker: Some(worker) };
		controller.stop().unwrap();
		assert!(finished.load(Ordering::Acquire));
		assert!(controller.worker.is_none());
		controller.stop().unwrap();
	}
	#[test]
	fn controller_panic_is_joined_and_reported() {
		let worker = std::thread::spawn(|| -> Result<(), Error> { panic!("device worker panic fixture") });
		let mut controller = StreamManagerController { should_drop: Arc::new(AtomicBool::new(false)), worker: Some(worker) };
		assert!(matches!(controller.stop(), Err(Error::WorkerPanicked)));
		assert!(controller.worker.is_none());
	}
}
