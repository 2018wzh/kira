use std::fmt::{Display, Formatter};

use cpal::{BuildStreamError, DefaultStreamConfigError, PlayStreamError};

/// Errors that can occur when using the cpal backend.
#[derive(Debug)]
pub enum Error {
	/// A default audio output device could not be determined.
	NoDefaultOutputDevice,
	/// An error occurred when getting the default output configuration.
	DefaultStreamConfigError(DefaultStreamConfigError),
	/// An error occurred when building the audio stream.
	BuildStreamError(BuildStreamError),
	/// An error occurred when starting the audio stream.
	PlayStreamError(PlayStreamError),
	/// The stream-manager thread could not be started.
	StreamManagerSpawn(std::io::Error),
	/// The stream-manager thread panicked.
	StreamManagerPanicked,
}

impl Display for Error {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			Error::NoDefaultOutputDevice => {
				f.write_str("Cannot find the default audio output device")
			}
			Error::DefaultStreamConfigError(error) => error.fmt(f),
			Error::BuildStreamError(error) => error.fmt(f),
			Error::PlayStreamError(error) => error.fmt(f),
			Error::StreamManagerSpawn(error) => write!(f, "Cannot start audio stream manager: {error}"),
			Error::StreamManagerPanicked => f.write_str("Audio stream manager panicked"),
		}
	}
}

impl std::error::Error for Error {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Error::DefaultStreamConfigError(error) => Some(error),
			Error::BuildStreamError(error) => Some(error),
			Error::PlayStreamError(error) => Some(error),
			Error::StreamManagerSpawn(error) => Some(error),
			_ => None,
		}
	}
}

impl From<DefaultStreamConfigError> for Error {
	fn from(v: DefaultStreamConfigError) -> Self {
		Self::DefaultStreamConfigError(v)
	}
}

impl From<BuildStreamError> for Error {
	fn from(v: BuildStreamError) -> Self {
		Self::BuildStreamError(v)
	}
}

impl From<PlayStreamError> for Error {
	fn from(v: PlayStreamError) -> Self {
		Self::PlayStreamError(v)
	}
}
