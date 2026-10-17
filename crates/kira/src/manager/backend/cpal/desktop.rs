mod stream_manager;

use stream_manager::{StreamManager, StreamManagerController};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

use crate::manager::backend::{Backend, Renderer};
use cpal::{
	traits::{DeviceTrait, HostTrait},
	BufferSize, Device, StreamConfig,
};

use super::{CpalBackendSettings, Error};

enum State {
	Empty,
	Uninitialized {
		device: Device,
		config: StreamConfig,
	},
	Initialized {
		stream_manager_controller: StreamManagerController,
	},
}

/// A backend that uses [cpal](https://crates.io/crates/cpal) to
/// connect a [`Renderer`] to the operating system's audio driver.
pub struct CpalBackend {
	state: State,
	/// Whether the device was specified by the user.
	custom_device: bool,
	buffer_size: BufferSize,
	suspended: Arc<AtomicBool>,
}

impl Backend for CpalBackend {
	type Settings = CpalBackendSettings;

	type Error = Error;

	fn setup(settings: Self::Settings) -> Result<(Self, u32), Self::Error> {
		let host = cpal::default_host();

		let (device, custom_device) = if let Some(device) = settings.device {
			(device, true)
		} else {
			(
				host.default_output_device()
					.ok_or(Error::NoDefaultOutputDevice)?,
				false,
			)
		};

		let config = device.default_output_config()?.config();
		let sample_rate = config.sample_rate.0;
		Ok((
			Self {
				state: State::Uninitialized { device, config },
				custom_device,
				buffer_size: settings.buffer_size,
				suspended: Arc::new(AtomicBool::new(false)),
			},
			sample_rate,
		))
	}

	fn start(&mut self, renderer: Renderer) -> Result<(), Self::Error> {
		let state = std::mem::replace(&mut self.state, State::Empty);
		if let State::Uninitialized { device, config } = state {
			self.state = State::Initialized {
				stream_manager_controller: StreamManager::start(
					renderer,
					device,
					config,
					self.custom_device,
					self.buffer_size,
					Arc::clone(&self.suspended),
				)?,
			};
		} else {
			panic!("Cannot initialize the backend multiple times")
		}
		Ok(())
	}
}

impl CpalBackend {
	/// Suspends the complete mixer without advancing sounds, fades or clocks.
	pub fn set_suspended(&mut self, suspended: bool) {
		self.suspended.store(suspended, Ordering::Release);
	}
	/// Shares the mixer suspension control with a native lifecycle owner.
	pub fn suspension_state(&self) -> Arc<AtomicBool> { Arc::clone(&self.suspended) }
	/// Reports a stopped/panicked device worker on the calling non-audio thread.
	pub fn check(&mut self) -> Result<(), Error> {
		if let State::Initialized { stream_manager_controller } = &mut self.state {
			stream_manager_controller.check()?;
		}
		Ok(())
	}
	/// Stops the device and joins its worker. Call only outside audio callbacks.
	pub fn shutdown(&mut self) -> Result<(), Error> {
		if let State::Initialized { mut stream_manager_controller } = std::mem::replace(&mut self.state, State::Empty) {
			stream_manager_controller.stop()?;
		}
		Ok(())
	}
}
impl Drop for CpalBackend {
	fn drop(&mut self) { let _ = self.shutdown(); }
}
