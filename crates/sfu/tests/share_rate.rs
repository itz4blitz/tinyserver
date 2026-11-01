//! A screen share at its full rate under the default packet budget. Alone in its own test
//! binary, because 2.5 Mb/s through a debug build is heavy for a busy machine.

mod common;

use std::time::Duration;

use common::*;
use sfu::VideoKind;

/// A publisher with the camera on, the microphone on, and a share at 2.5 Mb/s sends about
/// 550 packets a second: 285 for the share, 210 for the two camera layers, 50 for audio, and
/// RTCP. The default budget of 2,000 a second takes all of it.
#[tokio::test]
async fn the_share_is_not_dropped_by_the_packet_limit_at_its_rate() {
    let mut config = config();
    let defaults = sfu::Config::new(config.bind, config.public_ip);
    config.packets_per_second = defaults.packets_per_second;
    config.packet_burst = defaults.packet_burst;
    let mut sfu = start(config);
    let a = join(&mut sfu, "r", "a", b'A').await;
    let b = join(&mut sfu, "r", "b", b'B').await;
    sfu.camera(&a, true).await;
    a.running.sending(true);
    sfu.share_at(&a, true, 2500).await.expect("share");
    sfu.video_is(&b.call_id, "a", VideoKind::Share).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while !b
        .running
        .take_video()
        .iter()
        .any(|frame| frame.layer == b's')
    {
        assert!(tokio::time::Instant::now() < deadline, "no share frame");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let seen = seen_over(&b, Duration::from_secs(4)).await;
    // 15 a second, 60 in all.
    assert!(seen.len() >= 54, "{} share frames in 4 s", seen.len());
    assert!(seen.iter().all(|frame| frame.layer == b's'));
}
