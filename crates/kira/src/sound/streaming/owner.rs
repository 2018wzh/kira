use std::{any::Any, sync::{atomic::Ordering, Arc}, thread::JoinHandle};
use super::{sound::Shared, StreamingSoundData, StreamingSoundHandle};
use crate::sound::{Sound, SoundData};

/// Owns decoder workers on a non-audio thread independently of playback handles.
/// Stop the audio device before closing this owner. Custom decoders must return
/// from decode/seek before cancellation can finish. Handle-drop playback semantics
/// are unchanged; dropping a Sound only signals cancellation and never joins.
#[derive(Default)]
pub struct DecoderOwner {
    workers: Vec<Worker>,
    panic: Option<Box<dyn Any + Send + 'static>>,
}
struct Worker { shared: Arc<Shared>, join: JoinHandle<()> }
impl DecoderOwner {
    /// Creates an owner without starting threads.
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
    /// Cancel all workers, wake their waits, and join every worker even if one
    /// panics. Call on the application owner, never the device callback.
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

/// Streaming sound with an explicit non-audio-thread decoder owner.
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
