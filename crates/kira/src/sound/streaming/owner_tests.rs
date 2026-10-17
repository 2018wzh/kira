use super::*;
use crate::{frame::Frame, sound::streaming::Decoder};
use std::sync::mpsc;
struct TrackedDecoder { dropped: mpsc::Sender<std::thread::ThreadId>, panic: bool }
impl Decoder for TrackedDecoder {
    type Error = ();
    fn sample_rate(&self) -> u32 { 48000 }
    fn num_frames(&self) -> usize { 48000 * 3600 }
    fn decode(&mut self) -> Result<Vec<Frame>, ()> {
        assert!(!self.panic, "decoder panic fixture");
        Ok(vec![Frame::ZERO; 1024])
    }
    fn seek(&mut self, index: usize) -> Result<usize, ()> { Ok(index) }
}
impl Drop for TrackedDecoder {
    fn drop(&mut self) { let _ = self.dropped.send(std::thread::current().id()); }
}
#[test]
fn dropping_playback_handle_keeps_decoder_owned_until_shutdown() {
    let (tx, rx) = mpsc::channel();
    let mut owner = DecoderOwner::new();
    let (sound, handle) = StreamingSoundData::from_decoder(TrackedDecoder { dropped: tx, panic: false }).with_decoder_owner(&mut owner).into_sound().unwrap();
    drop(handle);
    assert!(rx.try_recv().is_err(), "playback handle drop must not stop the sound");
    owner.stop_and_join().unwrap();
    assert_ne!(rx.recv().unwrap(), std::thread::current().id());
    assert!(owner.workers.is_empty());
    drop(sound);
}
#[test]
fn panic_still_joins_every_decoder() {
    let (tx, rx) = mpsc::channel();
    let mut owner = DecoderOwner::new();
    let (bad, _) = StreamingSoundData::from_decoder(TrackedDecoder { dropped: tx.clone(), panic: true }).with_decoder_owner(&mut owner).into_sound().unwrap();
    // Wait for the panic fixture to be destroyed before registering another
    // decoder, exercising both completed-worker reaping and final shutdown.
    rx.recv().unwrap();
    let (good, _) = StreamingSoundData::from_decoder(TrackedDecoder { dropped: tx, panic: false }).with_decoder_owner(&mut owner).into_sound().unwrap();
    assert!(owner.stop_and_join().is_err());
    rx.recv().unwrap();
    assert!(owner.workers.is_empty());
    drop((bad, good));
}
#[test]
fn owner_drop_joins_without_a_playback_handle() {
    let (tx, rx) = mpsc::channel();
    let mut owner = DecoderOwner::new();
    let (sound, _) = StreamingSoundData::from_decoder(TrackedDecoder { dropped: tx, panic: false }).with_decoder_owner(&mut owner).into_sound().unwrap();
    drop(owner);
    rx.try_recv().expect("decoder destructor completed before owner Drop returns");
    drop(sound);
}
