//! Camera video through the SFU on loopback with headless str0m clients. No database: the
//! test plays the API's part, and turns cameras on and off with renegotiations.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::*;
use sfu::client::{Client, VideoFrame};
use sfu::{Quality, VideoKind};
use tokio::time::Instant;

/// Waits until `person` receives frames from `marker` on `layer` only, for half a second.
async fn settles_on(person: &Person, marker: u8, layer: u8) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last = Vec::new();
    while Instant::now() < deadline {
        last = seen_over(person, Duration::from_millis(500)).await;
        if !last.is_empty()
            && last
                .iter()
                .all(|frame| frame.marker == marker && frame.layer == layer)
        {
            return;
        }
    }
    panic!(
        "never settled on {} {}: {last:?}",
        marker as char, layer as char
    );
}

fn layers(frames: &[VideoFrame]) -> Vec<u8> {
    let mut seen: Vec<u8> = frames.iter().map(|frame| frame.layer).collect();
    seen.dedup();
    seen
}

#[tokio::test]
async fn a_camera_reaches_the_other_listener_and_not_its_owner() {
    let mut sfu = start(config());
    let a = join(&mut sfu, "r", "a", b'A').await;
    let b = join(&mut sfu, "r", "b", b'B').await;
    sfu.camera(&a, true).await;
    let slot = sfu.video_for(&b.call_id, Some("a")).await.expect("a slot");
    assert_eq!(slot.kind, VideoKind::Camera);
    let frames = sees(&b, b'A').await;
    assert!(frames[0].keyframe, "the first frame is not a keyframe");
    assert!(frames.iter().all(|frame| frame.mid == slot.mid));
    let own = seen_over(&a, Duration::from_millis(500)).await;
    assert!(own.is_empty(), "A saw its own camera: {own:?}");
    assert_eq!(sfu.video.get(&a.call_id).cloned().flatten(), None);
}

#[tokio::test]
async fn the_video_slot_follows_the_speaker_and_switches_on_a_keyframe() {
    let mut sfu = start(config());
    let a = join(&mut sfu, "r", "a", b'A').await;
    let b = join(&mut sfu, "r", "b", b'B').await;
    let c = join(&mut sfu, "r", "c", b'C').await;
    sfu.camera(&a, true).await;
    sfu.camera(&c, true).await;
    // Nobody has spoken: the first camera in join order.
    sfu.video_for(&b.call_id, Some("a")).await;
    sfu.video_for(&c.call_id, Some("a")).await;
    sees(&b, b'A').await;

    c.running.sending(true);
    let spoke = Instant::now();
    sfu.video_for(&b.call_id, Some("c")).await;
    let after = sees(&b, b'C').await;
    assert!(
        spoke.elapsed() < Duration::from_secs(2),
        "{:?}",
        spoke.elapsed()
    );
    assert!(after[0].keyframe, "the switch did not start on a keyframe");
    assert!(after.iter().all(|frame| frame.marker == b'C'), "{after:?}");
    // The speaker never sees themself; they keep the camera they had.
    let c_video = sfu.video.get(&c.call_id).cloned().flatten();
    assert_eq!(c_video.map(|slot| slot.user_id).as_deref(), Some("a"));
    sees(&c, b'A').await;

    // A takes the floor once C has been quiet past the hold. A's camera sent its only
    // keyframe long ago, so the one B switches on answers the SFU's request.
    let asked = a.running.keyframes_asked();
    c.running.sending(false);
    a.running.sending(true);
    sfu.video_for(&b.call_id, Some("a")).await;
    let back = sees(&b, b'A').await;
    assert!(
        back[0].keyframe,
        "the switch back did not start on a keyframe"
    );
    assert!(a.running.keyframes_asked() > asked, "no keyframe request");
    // A, the speaker, sees the previous speaker.
    sfu.video_for(&a.call_id, Some("c")).await;
    let previous = sees(&a, b'C').await;
    assert!(previous[0].keyframe);
}

#[tokio::test]
async fn a_listener_on_a_slow_link_gets_the_low_layer() {
    let mut sfu = start(config());
    let a = join(&mut sfu, "r", "a", b'A').await;
    let slow = join(&mut sfu, "r", "slow", b'S').await;
    let fast = join(&mut sfu, "r", "fast", b'F').await;
    slow.running.remb(Some(500));
    fast.running.remb(Some(5000));
    sfu.camera(&a, true).await;
    settles_on(&fast, b'A', b'h').await;
    settles_on(&slow, b'A', b'l').await;

    // The link recovers: the estimate climbs and the listener moves up on a keyframe.
    slow.running.remb(Some(5000));
    settles_on(&slow, b'A', b'h').await;
}

#[tokio::test]
async fn a_link_too_small_for_video_pauses_it_and_it_resumes_on_a_keyframe() {
    let mut sfu = start(config());
    let a = join(&mut sfu, "r", "a", b'A').await;
    let b = join(&mut sfu, "r", "b", b'B').await;
    b.running.remb(Some(1000));
    sfu.camera(&a, true).await;
    sfu.video_for(&b.call_id, Some("a")).await;
    sees(&b, b'A').await;

    b.running.remb(Some(200));
    sfu.video_for(&b.call_id, None).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let paused = seen_over(&b, Duration::from_millis(500)).await;
    assert!(paused.is_empty(), "video on a starved link: {paused:?}");

    // Enough to stay on, not to come back.
    b.running.remb(Some(400));
    let held = seen_over(&b, Duration::from_millis(1500)).await;
    assert!(held.is_empty(), "video below the resume point: {held:?}");

    b.running.remb(Some(1000));
    sfu.video_for(&b.call_id, Some("a")).await;
    assert!(sees(&b, b'A').await[0].keyframe);
}

#[tokio::test]
async fn call_quality_caps_the_layer_or_turns_video_off() {
    let mut sfu = start(config());
    let a = join(&mut sfu, "r", "a", b'A').await;
    let b = join(&mut sfu, "r", "b", b'B').await;
    b.running.remb(Some(5000));
    sfu.camera(&a, true).await;
    settles_on(&b, b'A', b'h').await;
    sfu.media.quality(&b.call_id, Quality::Low);
    settles_on(&b, b'A', b'l').await;
    sfu.media.quality(&b.call_id, Quality::Off);
    sfu.video_for(&b.call_id, None).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let off = seen_over(&b, Duration::from_millis(500)).await;
    assert!(off.is_empty(), "video while off: {off:?}");
    sfu.media.quality(&b.call_id, Quality::High);
    sfu.video_for(&b.call_id, Some("a")).await;
    assert!(sees(&b, b'A').await[0].keyframe);
}

#[tokio::test]
async fn video_over_the_publisher_budget_is_dropped() {
    let mut config = config();
    // Smaller than one frame of either layer, so nothing fits.
    config.camera_high_kbps = 8;
    config.camera_low_kbps = 8;
    let mut sfu = start(config);
    let a = join(&mut sfu, "r", "a", b'A').await;
    let b = join(&mut sfu, "r", "b", b'B').await;
    let b_slots = sfu.slots_for(&b.call_id, 1).await;
    sfu.camera(&a, true).await;
    a.running.sending(true);
    hears(&b, &mid_of(&b_slots, "a"), b'A').await;
    let seen = seen_over(&b, Duration::from_millis(1500)).await;
    assert!(seen.is_empty(), "over-budget video was forwarded: {seen:?}");
    assert_eq!(sfu.video.get(&b.call_id).cloned().flatten(), None);
}

#[tokio::test]
async fn a_layer_over_budget_gives_way_to_the_other() {
    let mut config = config();
    config.camera_high_kbps = 8;
    let mut sfu = start(config);
    let a = join(&mut sfu, "r", "a", b'A').await;
    let b = join(&mut sfu, "r", "b", b'B').await;
    b.running.remb(Some(5000));
    sfu.camera(&a, true).await;
    sfu.video_for(&b.call_id, Some("a")).await;
    settles_on(&b, b'A', b'l').await;
    let held = seen_over(&b, Duration::from_millis(1500)).await;
    assert_eq!(layers(&held), vec![b'l'], "{held:?}");
}

#[tokio::test]
async fn the_upload_budget_drops_listeners_to_low_then_to_audio_only() {
    let mut low = config();
    // Two listeners' audio, and one camera viewer on `l` but not on `h`.
    low.egress_kbps = Some(1000);
    let mut sfu = start(low);
    let a = join(&mut sfu, "r", "a", b'A').await;
    let b = join(&mut sfu, "r", "b", b'B').await;
    b.running.remb(Some(5000));
    sfu.camera(&a, true).await;
    settles_on(&b, b'A', b'l').await;
    let held = seen_over(&b, Duration::from_millis(1500)).await;
    assert_eq!(layers(&held), vec![b'l'], "{held:?}");

    let mut none = config();
    none.egress_kbps = Some(300);
    let mut sfu = start(none);
    let a = join(&mut sfu, "r", "a", b'A').await;
    let b = join(&mut sfu, "r", "b", b'B').await;
    sfu.camera(&a, true).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let seen = seen_over(&b, Duration::from_millis(1000)).await;
    assert!(seen.is_empty(), "video over the upload budget: {seen:?}");
    assert_eq!(sfu.video.get(&b.call_id).cloned().flatten(), None);
}

#[tokio::test]
async fn camera_off_clears_the_slot_and_on_brings_it_back() {
    let mut sfu = start(config());
    let a = join(&mut sfu, "r", "a", b'A').await;
    let b = join(&mut sfu, "r", "b", b'B').await;
    sfu.camera(&a, true).await;
    sfu.video_for(&b.call_id, Some("a")).await;
    sees(&b, b'A').await;
    sfu.camera(&a, false).await;
    sfu.video_for(&b.call_id, None).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let after = seen_over(&b, Duration::from_millis(500)).await;
    assert!(after.is_empty(), "video after camera off: {after:?}");
    sfu.camera(&a, true).await;
    sfu.video_for(&b.call_id, Some("a")).await;
    assert!(sees(&b, b'A').await[0].keyframe);
}

#[tokio::test]
async fn a_leaving_publisher_clears_the_slot_and_a_deafened_listener_sees_nothing() {
    let mut sfu = start(config());
    let a = join(&mut sfu, "r", "a", b'A').await;
    let b = join(&mut sfu, "r", "b", b'B').await;
    sfu.camera(&a, true).await;
    sees(&b, b'A').await;
    sfu.media.state(&b.call_id, false, true);
    sfu.video_for(&b.call_id, None).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let deaf = seen_over(&b, Duration::from_millis(500)).await;
    assert!(deaf.is_empty(), "video while deafened: {deaf:?}");
    sfu.media.state(&b.call_id, false, false);
    sfu.video_for(&b.call_id, Some("a")).await;
    assert!(sees(&b, b'A').await[0].keyframe);
    sfu.media.leave(&a.call_id);
    sfu.video_for(&b.call_id, None).await;
}

#[tokio::test]
async fn tcp_only_clients_send_and_receive_video() {
    let mut sfu = start(config());
    let client = Client::tcp(LOOPBACK.into(), SLOTS).expect("TCP client");
    let a = join_with(&mut sfu, client, "r", "a", b'A').await;
    let client = Client::tcp(LOOPBACK.into(), SLOTS).expect("TCP client");
    let b = join_with(&mut sfu, client, "r", "b", b'B').await;
    let client = Client::new(SocketAddr::from((LOOPBACK, 0)), SLOTS).expect("UDP client");
    let c = join_with(&mut sfu, client, "r", "c", b'C').await;
    sfu.camera(&a, true).await;
    sfu.video_for(&b.call_id, Some("a")).await;
    assert!(sees(&b, b'A').await[0].keyframe);
    assert!(sees(&c, b'A').await[0].keyframe);
    sfu.camera(&c, true).await;
    sfu.video_for(&a.call_id, Some("c")).await;
    assert!(sees(&a, b'C').await[0].keyframe);
}
