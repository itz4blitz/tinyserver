//! The SFU on loopback with headless str0m clients, and the test playing the API's part.

#![allow(dead_code)]

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use sfu::client::{Client, Frame, Running, VideoFrame};
use sfu::{Config, Event, Media, Refused, Slot, VideoKind, VideoSlot};
use tokio::sync::mpsc::Receiver;
use tokio::time::{timeout, Instant};

pub const LOOPBACK: Ipv4Addr = Ipv4Addr::LOCALHOST;
/// Longer than the consent timeout, so a drop is waited for in one go.
pub const WAIT: Duration = Duration::from_secs(10);
pub const SLOTS: usize = 11;
/// What most share tests send, so many at once stay light.
pub const LIGHT_SHARE_KBPS: u64 = 300;

pub fn config() -> Config {
    let mut config = Config::new(SocketAddr::from((LOOPBACK, 0)), LOOPBACK.into());
    config.connect_timeout = Duration::from_secs(2);
    // A silent str0m client still sends a STUN check or RTCP about every 3 s.
    config.consent_timeout = Duration::from_secs(5);
    // TWCC estimates depend on how busy the machine is. Layers follow the clients' REMB
    // instead, except in `bwe.rs`.
    config.bwe_start_kbps = None;
    config.layer_hold = Duration::from_millis(100);
    config
}

pub struct Sfu {
    pub media: Media,
    pub events: Receiver<Event>,
    /// The latest slot map for each call id, kept while waiting for other events.
    pub slots: HashMap<String, Vec<Slot>>,
    pub video: HashMap<String, Option<VideoSlot>>,
}

impl Drop for Sfu {
    fn drop(&mut self) {
        self.media.shutdown();
    }
}

pub fn start(config: Config) -> Sfu {
    let (media, events) = sfu::start(config).expect("start the SFU");
    Sfu {
        media,
        events,
        slots: HashMap::new(),
        video: HashMap::new(),
    }
}

impl Sfu {
    /// The next event `pick` returns something for. Others are skipped.
    pub async fn wait_for<T>(&mut self, mut pick: impl FnMut(&Event) -> Option<T>) -> T {
        let deadline = Instant::now() + WAIT;
        loop {
            let event = timeout(
                deadline.saturating_duration_since(Instant::now()),
                self.events.recv(),
            )
            .await
            .expect("an event in time")
            .expect("SFU running");
            if let Event::Slots {
                call_id,
                audio,
                video,
            } = &event
            {
                self.slots.insert(call_id.clone(), audio.clone());
                self.video.insert(call_id.clone(), video.clone());
            }
            if let Some(found) = pick(&event) {
                return found;
            }
        }
    }

    /// Waits until `call_id`'s slot map has `want` entries.
    pub async fn slots_for(&mut self, call_id: &str, want: usize) -> Vec<Slot> {
        if let Some(audio) = self.slots.get(call_id).filter(|audio| audio.len() == want) {
            return audio.clone();
        }
        self.wait_for(|event| match event {
            Event::Slots {
                call_id: id, audio, ..
            } if id == call_id && audio.len() == want => Some(audio.clone()),
            _ => None,
        })
        .await
    }

    /// Waits until `call_id`'s video slot carries `user_id`, or nobody for `None`.
    pub async fn video_for(&mut self, call_id: &str, user_id: Option<&str>) -> Option<VideoSlot> {
        let held =
            |video: &Option<VideoSlot>| video.as_ref().map(|slot| slot.user_id.as_str()) == user_id;
        if let Some(video) = self.video.get(call_id).filter(|video| held(video)) {
            return video.clone();
        }
        if user_id.is_none() && !self.video.contains_key(call_id) {
            return None;
        }
        self.wait_for(|event| match event {
            Event::Slots {
                call_id: id, video, ..
            } if id == call_id && held(video) => Some(video.clone()),
            _ => None,
        })
        .await
    }

    /// Waits until `call_id`'s video slot carries `user_id`'s `kind`.
    pub async fn video_is(&mut self, call_id: &str, user_id: &str, kind: VideoKind) -> VideoSlot {
        let held = |video: &Option<VideoSlot>| {
            video
                .as_ref()
                .filter(|slot| slot.user_id == user_id && slot.kind == kind)
                .cloned()
        };
        if let Some(slot) = self.video.get(call_id).and_then(held) {
            return slot;
        }
        self.wait_for(|event| match event {
            Event::Slots {
                call_id: id, video, ..
            } if id == call_id => held(video),
            _ => None,
        })
        .await
    }

    /// Turns `person`'s camera on or off with a renegotiation, as `call.offer` does.
    pub async fn camera(&mut self, person: &Person, on: bool) {
        let offer = person.running.camera(on).expect("a camera offer");
        let answer = self
            .media
            .offer(&person.call_id, &offer)
            .await
            .expect("an answer to the camera offer");
        assert_eq!(answer.camera, on);
        person
            .running
            .accept(&answer.sdp)
            .expect("accept the answer");
    }
}

impl Sfu {
    /// Starts or stops `person`'s share with a renegotiation, as `call.offer` does. `Ok` says
    /// whether they hold the room's share. A refused offer is undone on the client.
    pub async fn share(&mut self, person: &Person, on: bool) -> Result<bool, Refused> {
        self.share_at(person, on, LIGHT_SHARE_KBPS).await
    }

    /// `share` at `kbps`.
    pub async fn share_at(
        &mut self,
        person: &Person,
        on: bool,
        kbps: u64,
    ) -> Result<bool, Refused> {
        person.running.share_kbps(kbps);
        let offer = person.running.share(on).expect("a share offer");
        match self.media.offer(&person.call_id, &offer).await {
            Ok(answer) => {
                person
                    .running
                    .accept(&answer.sdp)
                    .expect("accept the answer");
                Ok(answer.share)
            }
            Err(refused) => {
                person.running.reject().expect("undo the offer");
                Err(refused)
            }
        }
    }
}

/// One person in a call: their client and the call id the SFU gave them.
pub struct Person {
    pub call_id: String,
    pub running: Running,
}

pub async fn join(sfu: &mut Sfu, room_id: &str, user_id: &str, marker: u8) -> Person {
    let client = Client::new(SocketAddr::from((LOOPBACK, 0)), SLOTS).expect("client");
    join_with(sfu, client, room_id, user_id, marker).await
}

/// Joins with `client`, which may be a TCP one.
pub async fn join_with(
    sfu: &mut Sfu,
    mut client: Client,
    room_id: &str,
    user_id: &str,
    marker: u8,
) -> Person {
    let answer = sfu
        .media
        .join(room_id, user_id, client.offer())
        .await
        .expect("answer");
    client.accept(&answer.sdp).expect("accept the answer");
    let running = client.start(marker).expect("client thread");
    let call_id = answer.call_id.clone();
    sfu.wait_for(|event| {
        matches!(event, Event::Connected { call_id: id } if *id == call_id).then_some(())
    })
    .await;
    Person {
        call_id: answer.call_id,
        running,
    }
}

pub fn mid_of(slots: &[Slot], user_id: &str) -> String {
    slots
        .iter()
        .find(|slot| slot.user_id == user_id)
        .map(|slot| slot.mid.clone())
        .expect("a slot for that person")
}

/// Waits until `person` has received a frame tagged `marker` on `mid`.
pub async fn hears(person: &Person, mid: &str, marker: u8) {
    let deadline = Instant::now() + Duration::from_secs(2);
    let want = Frame {
        mid: mid.to_string(),
        marker,
    };
    while Instant::now() < deadline {
        if person.running.take().contains(&want) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no frame from {marker} on mid {mid} within 2 s");
}

/// Waits until `person` has received a camera frame from `marker`, and returns the frames
/// from the first one of `marker`'s on.
pub async fn sees(person: &Person, marker: u8) -> Vec<VideoFrame> {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut seen = Vec::new();
    while Instant::now() < deadline {
        seen.extend(person.running.take_video());
        if let Some(first) = seen.iter().position(|frame| frame.marker == marker) {
            return seen.split_off(first);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no camera frame from {marker} within 3 s; saw {seen:?}");
}

/// Camera frames `person` receives over `period`.
pub async fn seen_over(person: &Person, period: Duration) -> Vec<VideoFrame> {
    person.running.take_video();
    tokio::time::sleep(period).await;
    person.running.take_video()
}

/// Frames `person` receives over `period`.
pub async fn heard_over(person: &Person, period: Duration) -> Vec<Frame> {
    person.running.take();
    tokio::time::sleep(period).await;
    person.running.take()
}
