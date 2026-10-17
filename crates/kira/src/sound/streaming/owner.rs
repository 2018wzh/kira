use std::{any::Any, sync::{atomic::Ordering, Arc}, thread::JoinHandle};
use super::{sound::Shared, StreamingSoundData, StreamingSoundHandle};
use crate::sound::{Sound, SoundData};

/// Owns streaming decoder threads independently of playback handles.
///
/// Create, reap, stop and drop this owner on a non-audio thread. Dropping a
/// playback handle still allows its sound to play. Cancellation is observed
/// between decoder operations; custom Decoder implementations must return from
/// decode/seek for shutdown to complete.
#[derive(Default)]
pub struct DecoderOwner {
    workers: Vec<Worker>,
    panic: Option<Box<dyn Any + Send + 'static>>,
}
struct Worker { shared: Arc<Shared>, join: JoinHandle<()> }
impl DecoderOwner {
    /// Creates an empty decoder owner. This does not start any threads.
    pub fn new() -> Self { Self::default() }
    pub(crate) fn register(&mut self, shared: Arc<Shared>, join: JoinHandle<()>) {
        let mut index = 0;
        while index < self.workers.len() {
            if self.workers[index].join.is_finished() {
                let worker = self.workers.swap_remove(index);
                if let Err(panic) = worker.join.join() {
                    if self.panic.is_none() { self.panic = Some(panic); }
                }
            } else { index += 1; }
        }
        self.workers.push(Worker { shared, join });
    }
    /// Requests every decoder to stop, then joins every thread. Even when one
    /// thread panics, all remaining threads are joined before returning.
    pub fn stop_and_join(&mut self) -> std::thread::Result<()> {
        for worker in &self.workers {
            worker.shared.decoder_stopped.store(true, Ordering::Release);
            worker.join.thread().unpark();
        }
        for worker in self.workers.drain(..) {
            if let Err(panic) = worker.join.join() {
                if self.panic.is_none() { self.panic = Some(panic); }
            }
        }
        match self.panic.take() { Some(panic) => Err(panic), None => Ok(()) }
    }
}
impl Drop for DecoderOwner {
    fn drop(&mut self) { let _ = self.stop_and_join(); }
}

/// A streaming sound whose decode thread belongs to a non-audio-thread owner.
/// Produced by [`StreamingSoundData::with_decoder_owner`].
pub struct OwnedStreamingSoundData<'a, Error: Send + 'static> {
    pub(crate) data: StreamingSoundData<Error>,
    pub(crate) owner: &'a mut DecoderOwner,
}
impl<Error: Send + 'static> SoundData for OwnedStreamingSoundData<'_, Error> {
    type Error = Error;
    type Handle = StreamingSoundHandle<Error>;
    fn into_sound(self) -> Result<(Box<dyn Sound>, Self::Handle), Error> {
        let (sound, handle, scheduler) = self.data.split()?;
        scheduler.start_owned(self.owner);
        Ok((Box::new(sound), handle))
    }
}

#[cfg(test)]
#[path = "owner_tests.rs"]
mod tests;
