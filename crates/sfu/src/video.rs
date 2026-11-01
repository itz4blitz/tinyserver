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
/// A listener on `l` moves up to `h` at this much room for video, and back down below
/// `DOWN_KBPS`.
pub(crate) const UP_KBPS: u64 = 1600;
pub(crate) const DOWN_KBPS: u64 = 1300;
/// A share gives way to a camera below `SHARE_DOWN_KBPS`, and comes back at `SHARE_UP_KBPS`.
pub(crate) const SHARE_UP_KBPS: u64 = 1000;
pub(crate) const SHARE_DOWN_KBPS: u64 = 800;
/// Video pauses below `PAUSE_KBPS`, so it cannot starve the audio of a link that small, and
/// resumes on a keyframe at `RESUME_KBPS`.
pub(crate) const PAUSE_KBPS: u64 = 250;
pub(crate) const RESUME_KBPS: u64 = 450;
/// What a listener's bandwidth estimator probes for while it wants `h`.
pub(crate) const PROBE_KBPS: u64 = 2000;
/// How far past `RESUME_KBPS` it probes while paused, and the steps it moves in.
const RESUME_MARGIN_KBPS: u64 = 250;
const RESUME_STEP_KBPS: u64 = 50;
/// What the link was asked to carry, as a percentage of the estimate, before the estimate says
/// anything about the link. Below this the sender is what limits the estimate.
const BUSY_PERCENT: u64 = 80;
/// The loss that shows a link is full. Less is what a lossy link does at any rate.
const FULL_LOSS: f32 = 0.10;
/// The round trip above its lowest that shows a queue building, which jitter stays under.
const FULL_QUEUE: Duration = Duration::from_millis(150);
/// How many holds the wait before trying again after a withdrawal can grow to.
const RETRY_MOST: u32 = 12;
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

/// What a paused listener probes for, to find `RESUME_KBPS` above the `audio_kbps` it gets. The
/// estimate settles about here, so a fixed target would stop short of it in a talkative room.
pub(crate) fn resume_probe_kbps(audio_kbps: u64) -> u64 {
    let target = RESUME_KBPS + RESUME_MARGIN_KBPS + audio_kbps;
    target.div_ceil(RESUME_STEP_KBPS) * RESUME_STEP_KBPS
}

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

/// What a listener's link has room for beside its audio, lowest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Fit {
    /// No video at all.
    Audio,
    /// `l`.
    Low,
    /// `l`, or a share.
    Share,
    /// Anything.
    High,
}

impl Fit {
    /// Each step above audio, highest first: the kb/s that reaches it, and what keeps it.
    const STEPS: [(Self, u64, u64); 3] = [
        (Self::High, UP_KBPS, DOWN_KBPS),
        (Self::Share, SHARE_UP_KBPS, SHARE_DOWN_KBPS),
        (Self::Low, RESUME_KBPS, PAUSE_KBPS),
    ];

    /// The most `kbps` carries for a listener at `from`.
    fn of(kbps: u64, from: Self) -> Self {
        Self::STEPS
            .iter()
            .find(|(step, up, down)| kbps >= if from >= *step { *down } else { *up })
            .map_or(Self::Audio, |(step, _, _)| *step)
    }

    /// The camera layers this leaves room for.
    pub fn quality(self) -> Quality {
        match self {
            Self::Audio => Quality::Off,
            Self::Low | Self::Share => Quality::Low,
            Self::High => Quality::High,
        }
    }
}

/// Whether a link is really full. An estimate alone cannot say: it follows what is sent, so a
/// still screen or a quiet call reads as a slow link. So the link must have been asked for about
/// what the estimate says it holds, and must show it: packets lost, or a queue building. str0m
/// has no way to read its own pacer's queue, so the queue is the one on the path. `sent` and
/// `estimate` are in kb/s, `loss` the share lost, and `queue` the round trip above its lowest.
pub(crate) fn congested(
    sent: u64,
    estimate: Option<u64>,
    loss: Option<f32>,
    queue: Option<Duration>,
) -> bool {
    let busy = estimate.is_some_and(|estimate| sent * 100 >= estimate * BUSY_PERCENT);
    let full = loss.is_some_and(|loss| loss >= FULL_LOSS)
        || queue.is_some_and(|queue| queue >= FULL_QUEUE);
    busy && full
}

/// One look at a listener's link, each in kb/s less the audio it gets.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Reading {
    /// What the receiver reported itself, which needs no evidence.
    pub told: Option<u64>,
    /// str0m's estimate.
    pub estimate: Option<u64>,
    /// See [`congested`].
    pub congested: bool,
}

/// What was taken from a listener, and when it is tried again.
#[derive(Debug, Clone, Copy)]
struct Withdrawn {
    from: Option<Fit>,
    retry: Instant,
}

/// Follows what one listener's link can carry. Down at once on evidence, because a full link is
/// losing audio now, and never on an estimate alone. Up as soon as an estimate has held for a
/// hold, so the video does not flap on a link that only looks better for a moment. Whatever was
/// taken comes back to be tried again after a wait that grows while it keeps failing, so no
/// listener is left without video because of how a link once looked.
#[derive(Debug, Clone, Default)]
pub(crate) struct Link {
    /// What the link is known to have room for. `None` until there is word of that: `l`, and
    /// any share.
    fit: Option<Fit>,
    rising: Option<Instant>,
    withdrawn: Option<Withdrawn>,
    /// The wait after the last withdrawal, and when what it took was last tried again.
    wait: Duration,
    tried: Option<Instant>,
}

impl Link {
    pub fn update(&mut self, reading: Reading, hold: Duration, now: Instant) -> Option<Fit> {
        let room = match (reading.told, reading.estimate) {
            (Some(told), Some(estimate)) => Some(told.min(estimate)),
            (told, estimate) => told.or(estimate),
        };
        let Some(room) = room else {
            return self.retry(reading, now);
        };
        let held = self.fit.unwrap_or(Fit::High);
        let limit = if reading.congested {
            Some(room)
        } else {
            reading.told
        };
        match limit.map(|limit| Fit::of(limit, held)) {
            Some(target) if target < held => {
                self.withdraw(target, hold, now);
                return self.fit;
            }
            Some(target) if self.fit.is_none() => self.fit = Some(target),
            _ => {}
        }
        let Some(fit) = self.fit else {
            // Good news needs no wait the first time. Anything less says nothing yet.
            let target = Fit::of(room, Fit::High);
            self.fit = (target >= Fit::Share).then_some(target);
            return self.fit;
        };
        let target = Fit::of(room, fit);
        if target <= fit {
            self.rising = None;
            return self.retry(reading, now);
        }
        let since = *self.rising.get_or_insert(now);
        if now.saturating_duration_since(since) >= hold {
            self.fit = Some(target);
            self.rising = None;
            if self.withdrawn.is_some_and(|gone| gone.from <= Some(target)) {
                self.withdrawn = None;
            }
        }
        self.fit
    }

    /// Takes the link down to `fit`, to be tried again later, sooner if it held for a while.
    fn withdraw(&mut self, fit: Fit, hold: Duration, now: Instant) {
        let failing = self
            .tried
            .is_some_and(|at| now.saturating_duration_since(at) < self.wait * 2);
        self.wait = if failing {
            (self.wait * 2).min(hold * RETRY_MOST)
        } else {
            hold
        };
        let from = self.withdrawn.map_or(self.fit, |gone| gone.from);
        self.withdrawn = Some(Withdrawn {
            from,
            retry: now + self.wait,
        });
        self.fit = Some(fit);
        self.rising = None;
    }

    /// Puts back what was taken, if its wait is over and the receiver has not said otherwise.
    fn retry(&mut self, reading: Reading, now: Instant) -> Option<Fit> {
        if let Some(gone) = self.withdrawn {
            let forbidden = reading
                .told
                .is_some_and(|told| Fit::of(told, Fit::Audio) < gone.from.unwrap_or(Fit::High));
            if now >= gone.retry && !forbidden && !reading.congested {
                self.fit = gone.from;
                self.withdrawn = None;
                self.tried = Some(now);
            }
        }
        self.fit
    }

    pub fn fit(&self) -> Option<Fit> {
        self.fit
    }
}

/// The layer a listener should get. `fit` is what its link has room for; with no estimate yet
/// it starts on `l`.
pub(crate) fn pick_layer(fit: Option<Fit>, quality: Quality, ceiling: Quality) -> Option<Layer> {
    match (quality.min(ceiling), fit) {
        (Quality::Off, _) | (_, Some(Fit::Audio)) => None,
        (Quality::High, Some(Fit::High)) => Some(Layer::High),
        _ => Some(Layer::Low),
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

    const HOLD: Duration = Duration::from_secs(5);

    #[test]
    fn what_fits_follows_the_room_for_video_with_hysteresis() {
        assert_eq!(Fit::of(5000, Fit::Audio), Fit::High);
        assert_eq!(Fit::of(UP_KBPS - 1, Fit::Low), Fit::Share);
        assert_eq!(Fit::of(UP_KBPS - 1, Fit::High), Fit::High);
        assert_eq!(Fit::of(DOWN_KBPS, Fit::High), Fit::High);
        assert_eq!(Fit::of(DOWN_KBPS - 1, Fit::High), Fit::Share);
        assert_eq!(Fit::of(SHARE_UP_KBPS - 1, Fit::Low), Fit::Low);
        assert_eq!(Fit::of(SHARE_DOWN_KBPS, Fit::Share), Fit::Share);
        assert_eq!(Fit::of(SHARE_DOWN_KBPS - 1, Fit::Share), Fit::Low);
        assert_eq!(Fit::of(RESUME_KBPS - 1, Fit::Audio), Fit::Audio);
        assert_eq!(Fit::of(RESUME_KBPS, Fit::Audio), Fit::Low);
        assert_eq!(Fit::of(PAUSE_KBPS, Fit::Low), Fit::Low);
        assert_eq!(Fit::of(PAUSE_KBPS - 1, Fit::Low), Fit::Audio);
    }

    fn told(kbps: u64) -> Reading {
        Reading {
            told: Some(kbps),
            ..Reading::default()
        }
    }

    /// The estimator's figure with nothing to back it, as on a still screen.
    fn idle(kbps: u64) -> Reading {
        Reading {
            estimate: Some(kbps),
            ..Reading::default()
        }
    }

    fn full(kbps: u64) -> Reading {
        Reading {
            estimate: Some(kbps),
            congested: true,
            ..Reading::default()
        }
    }

    #[test]
    fn a_link_drops_at_once_and_climbs_only_after_the_hold() {
        let start = Instant::now();
        let mut link = Link::default();
        let none = Reading::default();
        assert_eq!(link.update(none, HOLD, start), None);
        assert_eq!(link.update(told(5000), HOLD, start), Some(Fit::High));
        assert_eq!(link.update(told(200), HOLD, start), Some(Fit::Audio));
        assert_eq!(link.update(told(5000), HOLD, start), Some(Fit::Audio));
        let almost = start + HOLD - Duration::from_millis(1);
        assert_eq!(link.update(told(5000), HOLD, almost), Some(Fit::Audio));
        assert_eq!(link.update(told(5000), HOLD, start + HOLD), Some(Fit::High));
    }

    #[test]
    fn a_dip_starts_the_hold_over() {
        let start = Instant::now();
        let mut link = Link::default();
        link.update(told(500), HOLD, start);
        assert_eq!(link.update(told(5000), HOLD, start), Some(Fit::Low));
        let dip = start + HOLD / 2;
        assert_eq!(link.update(told(500), HOLD, dip), Some(Fit::Low));
        assert_eq!(link.update(told(5000), HOLD, dip), Some(Fit::Low));
        assert_eq!(link.update(told(5000), HOLD, start + HOLD), Some(Fit::Low));
        assert_eq!(link.update(told(5000), HOLD, dip + HOLD), Some(Fit::High));
    }

    #[test]
    fn good_news_is_taken_at_once_and_a_low_first_estimate_means_nothing() {
        let start = Instant::now();
        // A little audio off the estimator's 1,000 kb/s start.
        assert_eq!(
            Link::default().update(idle(950), HOLD, start),
            Some(Fit::Share)
        );
        assert_eq!(Link::default().update(idle(150), HOLD, start), None);
        assert_eq!(Link::default().update(idle(500), HOLD, start), None);
    }

    /// The call that found this: a still screen, whose estimate sat under 300 kb/s.
    #[test]
    fn an_estimate_with_nothing_behind_it_takes_nothing_away() {
        let start = Instant::now();
        let mut link = Link::default();
        link.update(idle(5000), HOLD, start);
        for second in 1..120 {
            let at = start + Duration::from_secs(second);
            assert_eq!(link.update(idle(150), HOLD, at), Some(Fit::High));
        }
    }

    #[test]
    fn what_was_withdrawn_is_always_tried_again_and_the_wait_grows() {
        let start = Instant::now();
        let mut link = Link::default();
        link.update(idle(5000), HOLD, start);
        let mut at = start;
        for wait in [1, 2, 4, 8, 12, 12] {
            assert_eq!(link.update(full(200), HOLD, at), Some(Fit::Audio));
            let due = at + HOLD * wait;
            let before = due - Duration::from_millis(1);
            assert_eq!(link.update(idle(200), HOLD, before), Some(Fit::Audio));
            assert_eq!(
                link.update(idle(200), HOLD, due),
                Some(Fit::High),
                "after {wait} holds"
            );
            at = due + Duration::from_secs(1);
        }
        // One that holds starts the wait over.
        at += HOLD * RETRY_MOST * 3;
        link.update(full(200), HOLD, at);
        assert_eq!(link.update(idle(200), HOLD, at + HOLD), Some(Fit::High));
    }

    #[test]
    fn no_retry_while_the_link_is_full_or_the_receiver_says_no() {
        let start = Instant::now();
        let later = start + HOLD * 2;
        let mut link = Link::default();
        link.update(told(5000), HOLD, start);
        link.update(told(200), HOLD, start);
        assert_eq!(link.update(told(200), HOLD, later), Some(Fit::Audio));
        let mut link = Link::default();
        link.update(idle(5000), HOLD, start);
        link.update(full(200), HOLD, start);
        assert_eq!(link.update(full(200), HOLD, later), Some(Fit::Audio));
        assert_eq!(link.update(idle(200), HOLD, later), Some(Fit::High));
    }

    #[test]
    fn a_link_is_full_only_when_it_was_asked_to_carry_its_estimate_and_shows_it() {
        let lost = Some(0.2);
        let queue = Some(Duration::from_millis(300));
        // A still screen, however low the estimate, or lost packets on a lossy link.
        assert!(!congested(100, Some(250), lost, queue));
        // Sent all it could, and nothing shows.
        assert!(!congested(500, Some(250), Some(0.0), None));
        assert!(!congested(
            500,
            Some(250),
            Some(0.03),
            Some(Duration::from_millis(60))
        ));
        assert!(congested(500, Some(250), lost, None));
        assert!(congested(500, Some(250), None, queue));
        assert!(!congested(500, None, lost, queue), "no estimate");
    }

    #[test]
    fn the_resume_probe_reaches_resume_above_the_audio() {
        assert_eq!(resume_probe_kbps(0), 700);
        for audio in [0, 49, 50, 128, 304, 1000] {
            let spare = resume_probe_kbps(audio) - audio;
            assert_eq!(
                Fit::of(spare, Fit::Audio),
                Fit::Low,
                "{audio} kb/s of audio"
            );
        }
        // Audio wobbles from one second to the next. The target need not.
        assert_eq!(resume_probe_kbps(301), resume_probe_kbps(304));
    }

    #[test]
    fn the_layer_follows_what_fits() {
        let high = Quality::High;
        assert_eq!(pick_layer(None, high, high), Some(Layer::Low));
        assert_eq!(pick_layer(Some(Fit::Audio), high, high), None);
        assert_eq!(pick_layer(Some(Fit::Low), high, high), Some(Layer::Low));
        assert_eq!(pick_layer(Some(Fit::Share), high, high), Some(Layer::Low));
        assert_eq!(pick_layer(Some(Fit::High), high, high), Some(Layer::High));
    }

    #[test]
    fn quality_and_the_budget_cap_the_layer() {
        let lots = Some(Fit::High);
        assert_eq!(
            pick_layer(lots, Quality::Low, Quality::High),
            Some(Layer::Low)
        );
        assert_eq!(pick_layer(lots, Quality::Off, Quality::High), None);
        assert_eq!(
            pick_layer(lots, Quality::High, Quality::Low),
            Some(Layer::Low)
        );
        assert_eq!(pick_layer(lots, Quality::High, Quality::Off), None);
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
