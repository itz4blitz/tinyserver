//! Screen share through the SFU on loopback with headless str0m clients. No database: the
//! test plays the API's part, and starts and stops shares with renegotiations. Shares here
//! send 300 kb/s, so eight tests at once do not starve a small machine; `share_rate.rs`
//! sends the full rate.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::*;
use sfu::client::{Client, VideoFrame};
use sfu::{Refused, VideoKind};

/// Waits until `person`'s video slot carries `user_id`'s share, and returns the frames from
/// its first one on.
async fn shown(sfu: &mut Sfu, person: &Person, user_id: &str, marker: u8) -> Vec<VideoFrame> {
    sfu.video_is(&person.call_id, user_id, VideoKind::Share)
        .await;
    let frames = sees_share(person, marker).await;
    assert!(
        frames[0].keyframe,
        "the share did not start on a keyframe: {:?}",
        &frames[..frames.len().min(4)]
    );
    frames
}

/// Waits until `person` has received a share frame from `marker`, and returns the frames from
/// the first one on.
async fn sees_share(person: &Person, marker: u8) -> Vec<VideoFrame> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut seen = Vec::new();
    while tokio::time::Instant::now() < deadline {
        seen.extend(person.running.take_video());
        let first = seen
            .iter()
            .position(|frame| frame.marker == marker && frame.layer == b's');
        if let Some(first) = first {
            return seen.split_off(first);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no share frame from {marker} within 3 s; saw {seen:?}");
}

/// Waits for half a second in which `person` receives no video. Frames already on the way,
/// or recovered late by a retransmission, may land first; a stream still forwarded never
/// leaves such a gap.
async fn goes_quiet(person: &Person) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    person.running.take_video();
    loop {
        let seen = seen_over(person, Duration::from_millis(500)).await;
        if seen.is_empty() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "video still forwarded: {seen:?}"
        );
    }
}

#[tokio::test]
async fn a_share_replaces_the_speaker_camera_for_everyone_but_the_sharer() {
    let mut sfu = start(config());
    let a = join(&mut sfu, "r", "a", b'A').await;
    let b = join(&mut sfu, "r", "b", b'B').await;
    let c = join(&mut sfu, "r", "c", b'C').await;
    sfu.camera(&b, true).await;
    sfu.video_for(&c.call_id, Some("b")).await;
    sees(&c, b'B').await;

    assert_eq!(sfu.share(&a, true).await, Ok(true));
    shown(&mut sfu, &b, "a", b'A').await;
    let frames = shown(&mut sfu, &c, "a", b'A').await;
    assert!(frames
        .iter()
        .all(|frame| frame.marker == b'A' && frame.layer == b's'));
    // The sharer keeps the camera they had.
    let a_video = sfu.video.get(&a.call_id).cloned().flatten();
    assert_eq!(a_video.map(|slot| slot.kind), Some(VideoKind::Camera));
    let own = seen_over(&a, Duration::from_millis(500)).await;
    assert!(!own.is_empty());
    assert!(own.iter().all(|frame| frame.marker == b'B'), "{own:?}");
}

#[tokio::test]
async fn a_second_share_is_refused() {
    let mut sfu = start(config());
    let a = join(&mut sfu, "r", "a", b'A').await;
    let b = join(&mut sfu, "r", "b", b'B').await;
    let c = join(&mut sfu, "r", "c", b'C').await;
    assert_eq!(sfu.share(&a, true).await, Ok(true));
    shown(&mut sfu, &c, "a", b'A').await;
    assert_eq!(sfu.share(&b, true).await, Err(Refused::ShareTaken));
    let held = seen_over(&c, Duration::from_millis(500)).await;
    assert!(held.iter().all(|frame| frame.marker == b'A'), "{held:?}");
    // Another room has its own share.
    let d = join(&mut sfu, "other", "d", b'D').await;
    assert_eq!(sfu.share(&d, true).await, Ok(true));
    // Once the first one stops, the room is free.
    assert_eq!(sfu.share(&a, false).await, Ok(false));
    assert_eq!(sfu.share(&b, true).await, Ok(true));
    shown(&mut sfu, &c, "b", b'B').await;
}

#[tokio::test]
async fn stopping_the_share_returns_to_the_speaker_camera() {
    let mut sfu = start(config());
    let a = join(&mut sfu, "r", "a", b'A').await;
    let b = join(&mut sfu, "r", "b", b'B').await;
    let c = join(&mut sfu, "r", "c", b'C').await;
    sfu.camera(&b, true).await;
    b.running.sending(true);
    sfu.share(&a, true).await.expect("share");
    shown(&mut sfu, &c, "a", b'A').await;
    let asked = b.running.keyframes_asked();

    assert_eq!(sfu.share(&a, false).await, Ok(false));
    let slot = sfu.video_for(&c.call_id, Some("b")).await.expect("a slot");
    assert_eq!(slot.kind, VideoKind::Camera);
    let back = sees(&c, b'B').await;
    assert!(
        back[0].keyframe,
        "the camera did not come back on a keyframe"
    );
    assert!(back.iter().all(|frame| frame.marker == b'B'), "{back:?}");
    assert!(b.running.keyframes_asked() > asked, "no keyframe request");
}

#[tokio::test]
async fn the_sharer_leaving_ends_the_share() {
    let mut sfu = start(config());
    let a = join(&mut sfu, "r", "a", b'A').await;
    let b = join(&mut sfu, "r", "b", b'B').await;
    let c = join(&mut sfu, "r", "c", b'C').await;
    sfu.camera(&b, true).await;
    sfu.share(&a, true).await.expect("share");
    shown(&mut sfu, &c, "a", b'A').await;
    sfu.media.leave(&a.call_id);
    sfu.video_for(&c.call_id, Some("b")).await;
    assert!(sees(&c, b'B').await[0].keyframe);
    assert_eq!(sfu.share(&b, true).await, Ok(true));
    shown(&mut sfu, &c, "b", b'B').await;
}

#[tokio::test]
async fn an_ended_share_stays_off_until_the_sharer_starts_it_again() {
    let mut sfu = start(config());
    let a = join(&mut sfu, "r", "a", b'A').await;
    let b = join(&mut sfu, "r", "b", b'B').await;
    sfu.share(&a, true).await.expect("share");
    shown(&mut sfu, &b, "a", b'A').await;
    sfu.media.end_share(&a.call_id);
    sfu.video_for(&b.call_id, None).await;
    goes_quiet(&b).await;
    // Another renegotiation with the share m-line still sending does not restart it.
    sfu.camera(&a, true).await;
    let slot = sfu.video_for(&b.call_id, Some("a")).await.expect("a slot");
    assert_eq!(slot.kind, VideoKind::Camera);
    assert_eq!(sfu.share(&a, false).await, Ok(false));
    assert_eq!(sfu.share(&a, true).await, Ok(true));
    shown(&mut sfu, &b, "a", b'A').await;
}

#[tokio::test]
async fn the_upload_budget_keeps_the_share_and_drops_cameras() {
    let mut config = config();
    // Six audio streams and two share viewers fit, and the sharer's camera does not.
    config.egress_kbps = Some(5400);
    let mut sfu = start(config);
    let a = join(&mut sfu, "r", "a", b'A').await;
    let b = join(&mut sfu, "r", "b", b'B').await;
    let c = join(&mut sfu, "r", "c", b'C').await;
    sfu.camera(&b, true).await;
    sfu.share(&a, true).await.expect("share");
    shown(&mut sfu, &c, "a", b'A').await;
    shown(&mut sfu, &b, "a", b'A').await;
    sfu.video_for(&a.call_id, None).await;
    goes_quiet(&a).await;
}

#[tokio::test]
async fn a_tcp_only_client_gets_the_share() {
    let mut sfu = start(config());
    let a = join(&mut sfu, "r", "a", b'A').await;
    let client = Client::tcp(LOOPBACK.into(), SLOTS).expect("TCP client");
    let b = join_with(&mut sfu, client, "r", "b", b'B').await;
    sfu.share(&a, true).await.expect("share");
    shown(&mut sfu, &b, "a", b'A').await;
    // And a TCP-only sharer reaches a UDP listener.
    let client = Client::new(SocketAddr::from((LOOPBACK, 0)), SLOTS).expect("UDP client");
    let c = join_with(&mut sfu, client, "r", "c", b'C').await;
    shown(&mut sfu, &c, "a", b'A').await;
    sfu.share(&a, false).await.expect("stop");
    sfu.share(&b, true).await.expect("share");
    shown(&mut sfu, &c, "b", b'B').await;
}

#[tokio::test]
async fn a_share_gives_way_to_a_camera_on_a_link_too_small_for_it() {
    let mut sfu = start(config());
    let a = join(&mut sfu, "r", "a", b'A').await;
    let b = join(&mut sfu, "r", "b", b'B').await;
    let c = join(&mut sfu, "r", "c", b'C').await;
    sfu.camera(&c, true).await;
    b.running.remb(Some(5000));
    assert_eq!(sfu.share(&a, true).await, Ok(true));
    sfu.video_is(&b.call_id, "a", VideoKind::Share).await;

    b.running.remb(Some(600));
    sfu.video_is(&b.call_id, "c", VideoKind::Camera).await;
    assert!(sees(&b, b'C').await[0].keyframe);

    b.running.remb(Some(5000));
    sfu.video_is(&b.call_id, "a", VideoKind::Share).await;
    sees(&b, b'A').await;
}
