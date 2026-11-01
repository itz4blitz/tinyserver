//! Video routing without the network: the speaker who holds the floor, which simulcast layer
//! a listener gets, what the upload budget allows, and the RTP time a listener's one video
//! slot carries across switches.

use std::time::{Duration, Instant};

/// Nominal camera bitrates from `docs/calls.md`, in kb/s. The upload budget plans with them.
pub(crate) const HIGH_KBPS: u64 = 1200;
pub(crate) const LOW_KBPS: u64 = 350;
/// The nominal screen share bitrate, in kb/s.
pub(crate) const SHARE_KBPS: u64 = 2500;
/// One forwarded audio stream with its headers, in kb/s, for the upload budget.
pub(crate) const AUDIO_KBPS: u64 = 50;
/// A listener on `l` moves up to `h` at this estimate, and back down below `DOWN_KBPS`.
pub(crate) const UP_KBPS: u64 = 1600;
pub(crate) const DOWN_KBPS: u64 = 1300;
/// What a listener's bandwidth estimator probes for while it wants `h`.
pub(crate) const PROBE_KBPS: u64 = 2000;
/// A layer with no frame for this long is not chosen while the other one is sending.
pub(crate) const STALE: Duration = Duration::from_secs(1);
/// Keyframe requests to one layer of one publisher go out at most this often.
pub(crate) const KEYFRAME_EVERY: Duration = Duration::from_millis(300);
/// The same while the layer is over budget: asking faster than a keyframe can fit only makes
/// the encoder send more.
pub(crate) const KEYFRAME_BROKEN_EVERY: Duration = Duration::from_secs(1);
/// A challenger takes the floor after being the loudest speaker this long.
pub(crate) const TAKE_FLOOR: Duration = Duration::from_millis(800);
/// While the holder still speaks, a challenger must be this much louder, in dB.
pub(crate) const FLOOR_MARGIN: f32 = 6.0;
/// The 90 kHz RTP clock of every video codec.
const VIDEO_HZ: u64 = 90_000;

/// One simulcast layer of a camera, or a share's one layer. `h` is high, `l` is low; a
/// camera without rids is high.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Layer {
    High,
    Low,
    Share,
}

impl Layer {
    pub fn of_rid(rid: Option<&str>) -> Self {
        match rid {
            Some("l") => Self::Low,
            _ => Self::High,
        }
    }

    pub fn index(self) -> usize {
        match self {
            Self::High => 0,
            Self::Low => 1,
            Self::Share => 2,
        }
    }

    /// The camera's other layer. A share has only one.
    pub fn other(self) -> Self {
        match self {
            Self::High => Self::Low,
            Self::Low => Self::High,
            Self::Share => Self::Share,
        }
    }
}

/// What video a listener can use: theirs from `call.quality`, or the server's from its
/// upload budget. `Off` is audio only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Quality {
    Off,
    Low,
    High,
}

/// The layer a listener should get. `current` is the one it wants now, for hysteresis.
/// `estimate` is its bandwidth estimate in kb/s; with none yet it starts on `l`.
pub(crate) fn pick_layer(
    current: Option<Layer>,
    estimate: Option<u64>,
    quality: Quality,
    ceiling: Quality,
) -> Option<Layer> {
    match quality.min(ceiling) {
        Quality::Off => None,
        Quality::Low => Some(Layer::Low),
        Quality::High => {
            let Some(estimate) = estimate else {
                return Some(Layer::Low);
            };
            let floor = if current == Some(Layer::High) {
                DOWN_KBPS
            } else {
                UP_KBPS
            };
            Some(if estimate >= floor {
                Layer::High
            } else {
                Layer::Low
            })
        }
    }
}

/// What the server's upload budget lets every listener have.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Ceiling {
    /// Listeners in a room with a share get it.
    pub share: bool,
    /// The most a camera listener may get.
    pub camera: Quality,
}

/// The plan under the server's upload budget. Shares come first, because text on a screen
/// matters more than a face: they fit after audio, or nobody gets one. Then cameras, with
/// what is left: `h` if every camera listener fits on `h`, `l` if they fit on `l`, else
/// audio only. `audio_streams` is the number of audio streams forwarded, `share_viewers` the
/// listeners with a share to watch, and `camera_viewers` those with only a camera.
pub(crate) fn ceiling(
    budget_kbps: Option<u32>,
    audio_streams: u64,
    share_viewers: u64,
    camera_viewers: u64,
) -> Ceiling {
    let Some(budget) = budget_kbps else {
        return Ceiling {
            share: true,
            camera: Quality::High,
        };
    };
    let budget = u64::from(budget);
    let audio = audio_streams.saturating_mul(AUDIO_KBPS);
    let shares = audio.saturating_add(share_viewers.saturating_mul(SHARE_KBPS));
    let share = shares <= budget;
    let planned = if share { shares } else { audio };
    let fits = |layer_kbps: u64| {
        planned.saturating_add(camera_viewers.saturating_mul(layer_kbps)) <= budget
    };
    let camera = if fits(HIGH_KBPS) {
        Quality::High
    } else if fits(LOW_KBPS) {
        Quality::Low
    } else {
        Quality::Off
    };
    Ceiling { share, camera }
}

/// Maps each source's RTP time onto one continuous timeline for a listener's video slot.
/// A new source starts where the last frame left off, plus the wall time since.
#[derive(Debug, Clone)]
pub(crate) struct Rebase<S> {
    source: Option<S>,
    offset: u64,
    last: Option<(u64, Instant)>,
}

impl<S> Default for Rebase<S> {
    fn default() -> Self {
        Self {
            source: None,
            offset: 0,
            last: None,
        }
    }
}

impl<S: PartialEq + Copy> Rebase<S> {
    /// The output time, at 90 kHz, of a frame from `source` at `time`.
    pub fn map(&mut self, source: S, time: u64, now: Instant) -> u64 {
        if self.source != Some(source) {
            let start = match self.last {
                Some((at, then)) => {
                    let gap = now.saturating_duration_since(then).as_micros();
                    let ticks = u64::try_from(gap.saturating_mul(u128::from(VIDEO_HZ)) / 1_000_000)
                        .unwrap_or(u64::MAX);
                    at.wrapping_add(ticks.max(1))
                }
                None => time,
            };
            self.offset = start.wrapping_sub(time);
            self.source = Some(source);
        }
        let out = time.wrapping_add(self.offset);
        self.last = Some((out, now));
        out
    }
}

/// Who holds the floor in a room, from audio levels, and who held it before. A challenger
/// takes it after `TAKE_FLOOR` as the loudest speaker, and must beat a holder who is still
/// speaking by `FLOOR_MARGIN`, so the video does not flap between two people talking.
#[derive(Debug, Clone)]
pub(crate) struct Floor<K> {
    /// Holders, newest first. The first is the active speaker.
    recent: Vec<K>,
    challenger: Option<(K, Instant)>,
}

impl<K: PartialEq + Copy> Floor<K> {
    pub fn new() -> Self {
        Self {
            recent: Vec::new(),
            challenger: None,
        }
    }

    /// Everyone who has held the floor, newest first.
    pub fn recent(&self) -> &[K] {
        &self.recent
    }

    /// `speakers` are the people speaking now, with their smoothed level in dBov. True if
    /// the floor changed hands.
    pub fn hear(&mut self, speakers: &[(K, f32)], now: Instant) -> bool {
        let Some(&(loudest, level)) = speakers.iter().max_by(|a, b| a.1.total_cmp(&b.1)) else {
            self.challenger = None;
            return false;
        };
        let Some(&holder) = self.recent.first() else {
            self.take(loudest);
            return true;
        };
        if loudest == holder {
            self.challenger = None;
            return false;
        }
        let holder_level = speakers
            .iter()
            .find(|(key, _)| *key == holder)
            .map(|(_, level)| *level);
        if holder_level.is_some_and(|held| level < held + FLOOR_MARGIN) {
            self.challenger = None;
            return false;
        }
        match self.challenger {
            Some((key, since)) if key == loudest => {
                if now.saturating_duration_since(since) >= TAKE_FLOOR {
                    self.take(loudest);
                    return true;
                }
                false
            }
            _ => {
                self.challenger = Some((loudest, now));
                false
            }
        }
    }

    /// Someone left.
    pub fn forget(&mut self, key: &K) {
        self.recent.retain(|held| held != key);
        if self.challenger.is_some_and(|(held, _)| held == *key) {
            self.challenger = None;
        }
    }

    fn take(&mut self, key: K) {
        self.recent.retain(|held| *held != key);
        self.recent.insert(0, key);
        self.challenger = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rids_name_layers() {
        assert_eq!(Layer::of_rid(Some("h")), Layer::High);
        assert_eq!(Layer::of_rid(Some("l")), Layer::Low);
        assert_eq!(Layer::of_rid(None), Layer::High);
        assert_eq!(Layer::High.other(), Layer::Low);
        assert_eq!(Layer::Share.other(), Layer::Share);
    }

    #[test]
    fn the_layer_follows_the_estimate_with_hysteresis() {
        let high = Quality::High;
        assert_eq!(pick_layer(None, None, high, high), Some(Layer::Low));
        assert_eq!(pick_layer(None, Some(500), high, high), Some(Layer::Low));
        assert_eq!(
            pick_layer(Some(Layer::Low), Some(1500), high, high),
            Some(Layer::Low)
        );
        assert_eq!(
            pick_layer(Some(Layer::Low), Some(UP_KBPS), high, high),
            Some(Layer::High)
        );
        assert_eq!(
            pick_layer(Some(Layer::High), Some(1400), high, high),
            Some(Layer::High)
        );
        assert_eq!(
            pick_layer(Some(Layer::High), Some(DOWN_KBPS - 1), high, high),
            Some(Layer::Low)
        );
    }

    #[test]
    fn quality_and_the_budget_cap_the_layer() {
        let lots = Some(10_000);
        assert_eq!(
            pick_layer(None, lots, Quality::Low, Quality::High),
            Some(Layer::Low)
        );
        assert_eq!(pick_layer(None, lots, Quality::Off, Quality::High), None);
        assert_eq!(
            pick_layer(None, lots, Quality::High, Quality::Low),
            Some(Layer::Low)
        );
        assert_eq!(pick_layer(None, lots, Quality::High, Quality::Off), None);
    }

    fn camera(budget: Option<u32>, audio: u64, viewers: u64) -> Quality {
        ceiling(budget, audio, 0, viewers).camera
    }

    #[test]
    fn the_budget_drops_to_low_then_to_audio_only() {
        assert_eq!(camera(None, 100, 100), Quality::High);
        // Two listeners, one audio stream each.
        assert_eq!(camera(Some(2500), 2, 2), Quality::High);
        assert_eq!(camera(Some(2000), 2, 2), Quality::Low);
        assert_eq!(camera(Some(500), 2, 2), Quality::Off);
        assert_eq!(camera(Some(500), 2, 0), Quality::High);
    }

    #[test]
    fn shares_are_planned_before_cameras() {
        let all = ceiling(None, 100, 100, 100);
        assert!(all.share);
        assert_eq!(all.camera, Quality::High);
        // Two share viewers and one camera viewer, the sharer, with three audio streams.
        let roomy = ceiling(Some(6400), 3, 2, 1);
        assert_eq!((roomy.share, roomy.camera), (true, Quality::High));
        let tight = ceiling(Some(5500), 3, 2, 1);
        assert_eq!((tight.share, tight.camera), (true, Quality::Low));
        let shares_only = ceiling(Some(5200), 3, 2, 1);
        assert_eq!(
            (shares_only.share, shares_only.camera),
            (true, Quality::Off)
        );
        // No room for the shares: cameras get the budget instead.
        let cameras = ceiling(Some(2000), 3, 2, 1);
        assert_eq!((cameras.share, cameras.camera), (false, Quality::High));
    }

    #[test]
    fn a_new_source_continues_the_timeline() {
        let start = Instant::now();
        let mut rebase = Rebase::default();
        assert_eq!(rebase.map('a', 1000, start), 1000);
        assert_eq!(rebase.map('a', 4000, start), 4000);
        let later = start + Duration::from_millis(100);
        assert_eq!(rebase.map('b', 7_000_000, later), 4000 + 9000);
        assert_eq!(rebase.map('b', 7_003_000, later), 4000 + 12_000);
        // Back to the first source, which is now far behind.
        assert_eq!(rebase.map('a', 2000, later), 4000 + 12_001);
    }

    #[test]
    fn rtp_time_wraps() {
        let start = Instant::now();
        let mut rebase = Rebase::default();
        rebase.map('a', u64::MAX - 10, start);
        assert_eq!(rebase.map('b', 5, start), u64::MAX - 9);
        assert_eq!(rebase.map('b', 20, start), 5);
    }

    #[test]
    fn the_first_speaker_takes_the_floor_at_once() {
        let start = Instant::now();
        let mut floor = Floor::new();
        assert!(!floor.hear(&[], start));
        assert!(floor.hear(&[(1, -20.0)], start));
        assert_eq!(floor.recent(), &[1]);
    }

    #[test]
    fn a_challenger_needs_time_and_a_margin_while_the_holder_speaks() {
        let start = Instant::now();
        let mut floor = Floor::new();
        floor.hear(&[(1, -20.0)], start);
        let both = [(1, -20.0), (2, -17.0)];
        assert!(!floor.hear(&both, start));
        assert!(
            !floor.hear(&both, start + TAKE_FLOOR * 2),
            "not loud enough"
        );
        let louder = [(1, -20.0), (2, -10.0)];
        assert!(!floor.hear(&louder, start));
        assert!(!floor.hear(&louder, start + TAKE_FLOOR / 2));
        assert!(floor.hear(&louder, start + TAKE_FLOOR));
        assert_eq!(floor.recent(), &[2, 1]);
    }

    #[test]
    fn a_challenger_takes_a_silent_holders_floor_after_the_wait() {
        let start = Instant::now();
        let mut floor = Floor::new();
        floor.hear(&[(1, -20.0)], start);
        assert!(!floor.hear(&[(2, -40.0)], start));
        assert!(
            !floor.hear(&[], start + TAKE_FLOOR / 2),
            "a pause resets it"
        );
        assert!(!floor.hear(&[(2, -40.0)], start + TAKE_FLOOR));
        assert!(floor.hear(&[(2, -40.0)], start + TAKE_FLOOR * 2));
        assert_eq!(floor.recent(), &[2, 1]);
        floor.forget(&2);
        assert_eq!(floor.recent(), &[1]);
    }
}
