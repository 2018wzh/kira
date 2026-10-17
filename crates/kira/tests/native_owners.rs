use kira::{Frame, sound::{SoundData, streaming::{Decoder, DecoderOwner, StreamingSoundData}}};
use std::{sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}}, thread, time::{Duration, Instant}};

struct FixtureDecoder {
    dropped: Arc<AtomicBool>,
    reads: Arc<AtomicUsize>,
    empty: bool,
    panic: bool,
}
impl Drop for FixtureDecoder {
    fn drop(&mut self) { self.dropped.store(true, Ordering::Release); }
}
impl Decoder for FixtureDecoder {
    type Error = std::convert::Infallible;
    fn sample_rate(&self) -> u32 { 48_000 }
    fn num_frames(&self) -> usize { usize::MAX / 2 }
    fn seek(&mut self, index: usize) -> Result<usize, Self::Error> { Ok(index) }
    fn decode(&mut self) -> Result<Vec<Frame>, Self::Error> {
        self.reads.fetch_add(1, Ordering::AcqRel);
        assert!(!self.panic, "decoder panic fixture");
        Ok(if self.empty { Vec::new() } else { vec![Frame::ZERO; 256] })
    }
}
fn fixture(empty: bool, panic: bool) -> (StreamingSoundData<std::convert::Infallible>, Arc<AtomicBool>, Arc<AtomicUsize>) {
    let dropped = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicUsize::new(0));
    (StreamingSoundData::from_decoder(FixtureDecoder { dropped: dropped.clone(), reads: reads.clone(), empty, panic }), dropped, reads)
}
fn wait_until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !predicate() {
        assert!(Instant::now() < deadline, "native worker deadline");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn playback_handle_drop_does_not_cancel_but_owner_join_does() {
    let mut owner = DecoderOwner::new();
    let (data, dropped, reads) = fixture(false, false);
    let (sound, handle) = data.with_decoder_owner(&mut owner).into_sound().unwrap();
    drop(handle);
    wait_until(|| reads.load(Ordering::Acquire) > 0);
    assert!(!dropped.load(Ordering::Acquire));
    owner.stop_and_join().unwrap();
    assert!(dropped.load(Ordering::Acquire));
    drop(sound);
}

#[test]
fn sound_drop_signals_without_joining_and_owner_reaps() {
    let mut owner = DecoderOwner::new();
    let (data, dropped, reads) = fixture(false, false);
    let (sound, _handle) = data.with_decoder_owner(&mut owner).into_sound().unwrap();
    wait_until(|| reads.load(Ordering::Acquire) > 0);
    drop(sound);
    wait_until(|| dropped.load(Ordering::Acquire));
    owner.stop_and_join().unwrap();
}

#[test]
fn empty_decode_loop_is_cancellable_and_all_workers_join_after_panic() {
    let mut owner = DecoderOwner::new();
    let (data, dropped, reads) = fixture(true, false);
    let (_sound, _handle) = data.with_decoder_owner(&mut owner).into_sound().unwrap();
    wait_until(|| reads.load(Ordering::Acquire) > 10);
    let (bad, bad_dropped, _) = fixture(false, true);
    let (_bad_sound, _bad_handle) = bad.with_decoder_owner(&mut owner).into_sound().unwrap();
    wait_until(|| bad_dropped.load(Ordering::Acquire));
    assert!(owner.stop_and_join().is_err());
    assert!(dropped.load(Ordering::Acquire));
}

#[cfg(all(feature = "cpal", not(target_arch = "wasm32")))]
#[test]
#[ignore = "requires an actual audio output device; silent fixture tests lifecycle, not audibility"]
fn real_cpal_suspends_and_joins_before_same_process_reopen() {
    for _ in 0..3 {
        let mut owner = DecoderOwner::new();
        let mut manager = kira::AudioManager::<kira::DefaultBackend>::new(kira::AudioManagerSettings::default()).unwrap();
        let (data, dropped, reads) = fixture(false, false);
        let handle = manager.play(data.with_decoder_owner(&mut owner)).unwrap();
        wait_until(|| reads.load(Ordering::Acquire) > 0 && handle.position() > 0.0);
        manager.backend_mut().set_suspended(true);
        thread::sleep(Duration::from_millis(100));
        let paused = handle.position();
        thread::sleep(Duration::from_millis(100));
        assert_eq!(handle.position(), paused);
        manager.backend_mut().set_suspended(false);
        wait_until(|| handle.position() > paused);
        manager.backend_mut().stop_and_join().unwrap();
        owner.stop_and_join().unwrap();
        assert!(dropped.load(Ordering::Acquire));
        let stopped = handle.position();
        thread::sleep(Duration::from_millis(30));
        assert_eq!(handle.position(), stopped);
    }
}
