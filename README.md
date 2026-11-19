# HA Alarm Clock

A centralised alarm clock and reminder integration for Home Assistant which plays alarms and reminders on media players, with HA Media Browser support for selecting media and companion cards. It builds on [HA-Alarms-and-Reminders][upstream] (thank you!), with changes via vibe coding.

## Overview

- Alarms and reminders repeat in a loop until stopped; stopping playback on the target player (“Hey Google, stop” or pressing the hardware button/satellite control) stops the alarm to imitate a traditional alarm clock feel (in addition to the ability to stop alarm and reminders via service calls, voice and companion cards).
- Playback sequence is configurable per-item: loop announce time → announce name → announce message → media playback; all announcements are optional
- HA Media Browser support for media file selection, supports multiple sources: local media, Music Assistant, DLNA, SpotifyPlus, Plex, Jellyfin, Radio Browser
- Pair it with the [Noise Generator](https://github.com/nirnachmani/noise-generator) for more traditional alarm sounds.
- Tested media players include Google Cast devices, Home Assistant Voice preview, Music Assistant media players, Spotify media players, and SpotifyPlus players; others may behave differently, especially around looping and stop detection.
- Entities (alarm/reminder and a switch) per item plus a dashboard sensor that exposes the entire schedule for automations.
- Services exist for creating, editing, rescheduling, snoozing, stopping, deleting, and stop-all actions, along with metadata helpers for the UI and HA Assist/LLM intents for voice/AI control.
- Repeat patterns cover one-off, daily, weekdays, weekends, or a custom list of days, and can be edited after creation.
- Override volume can be set for each item
- Snoozing is available per alarm/reminder via services, the companion cards or assist, and you can set a global default snooze duration in the options flow.
- Entities (lights, switches, script, etc.) can be turned on when an alarm or reminder fires; available entities are configured in the integration so only approved entities appear in the UI.
- Companion cards for alarms and reminders to display, add, edit, control and delete alarms and reminders including support for media browsing and searching and media sampling  

## Limitations

- The integration was only tested on Google cast devices and Home Assistant Voice PE. Amazon, Apple, Sonos, and other media players have not been verified; expect fragile looping or stop recognition if you try them.
- Some media players, especially untested ones, may mis-detect the transition to the next loop as a user stop (which means that the loop will only run once) or user stops may be missed (and the alarm/reminder will continue looping despite user stopping playback) 
- Spotify and SpotifyPlus cannot run TTS, so are not supported on reminders 
- Artwork and media icons in the HA Companion app appear only when the phone is on the local network.
- Limited testing overall, integration likely contains a lot of bugs 
- LLM functions has been updated but not tested at all, expect problems 

## Installation

### Marketplace (Home Assistant 2026.11 and later)

1. Open **Settings → Marketplace** and connect your GitHub account if prompted. Custom repositories require a GitHub connection.
2. Select **Add from link**, enter `https://github.com/nirnachmani/HA-Alarm-Clock`, and choose **Integration** if prompted.
3. Install **HA Alarm Clock** and follow its setup flow. If it does not open automatically, go to **Settings → Devices & Services → Add Integration** and search for **HA Alarm Clock**.

Marketplace replaces HACS starting with Home Assistant 2026.11. Existing HACS installations migrate automatically. Follow any restart prompt when updating an installed integration.

### HACS (Home Assistant 2026.10 and earlier)
1. Open **HACS → ⋮ → Custom repositories**.
2. Add `https://github.com/nirnachmani/HA-Alarm-Clock` and choose **Integration**.
3. Search and download “HA Alarm Clock” and restart Home Assistant.
4. Go to **Settings → Devices & Services → Add Integration**, search for **HA Alarm Clock** and install.

### Manual
1. Copy the `ha_alarm_clock` folder into `config/custom_components/ha_alarm_clock/` inside your Home Assistant config.
2. Restart Home Assistant.
3. Go to **Settings → Devices & Services → Add Integration**, search for **HA Alarm Clock** and install.

(Optional) Install the [companion Lovelace cards for alarms and reminders](https://github.com/nirnachmani/HA-Alarm-Clock-Cards). They give you full UI control, browsing, and media search.

### Media files

Marketplace and HACS install only the integration folder. They do not copy the sample audio from this repository's `media files/` folder into Home Assistant's media library. Download those files manually as described below, or choose media you already have in the integration settings.

- Download the media files and place them under /media/Alarms (not under /config/media but under the root folder [in HAOS]) - this will make them available in HA under /media/local/Alarms
- See https://www.home-assistant.io/integrations/media_source/ for further information  

## Configuration (Settings → Devices & services → HA Alarm Clock → Gear icon)

All options are optional 

- **alarm_sound**: path for default media to play for alarms; defaults to `/media/local/Alarms/birds.mp3` 
- **reminder_sound**: path for default media to play for reminder; defaults to `/media/local/Alarms/ringtone.mp3` 
- **media_player**: default media playe
- **allowed_activation_entities**: allow-list of entities that can be toggled when an alarm triggers; alarms and reminders can only toggle an entity from that list.
- **enable_llm**: to enable LLM support
- **default_snooze_minutes**: controls the duration of the snooze action, integration-wide (not configurable per item)
- **active press mode**: determine short vs long press actions in the companion cards (stop vs snooze).

## Services, Entities, and Assist

- Services include: set/edit alarm, set/edit reminder, reschedule, snooze, stop, delete (single or all), and stop-all playback. 
- Each alarm/reminder also exposes a switch entity (`ha_alarm_clock_alarm.*` / `ha_alarm_clock_reminder.*`) for enable/disable, and there is a dashboard sensor that includes structured JSON for every item.
- HA Assist intents and LLM tools are registered so you can ask Assist (or an LLM using the Home Assistant API) to list, add, delete, stop, or snooze alarms/reminders.

## Companion Cards

- The companion cards display, create, edit, delete, snooze, stop and enable/disable alarms and reminders
- Built-in HA Media Browser dialogs let you choose tracks from media sources like local media, Music Assistant, DLNA, SpotifyPlus, Plex, Jellyfin. The companion cards also offer local on-device search (very slow) and Music Assistant and SpotifyPlus search to find media quickly.
- ability to sample media from the card (if supported)
- Companion cards repository link: https://github.com/nirnachmani/HA-Alarm-Clock-Cards

## Debugging

If you find a bug, please enable debug logging via `configuration.yaml`:

```yaml
logger:
  logs:
    custom_components.ha_alarm_clock: debug
```

Reproduce the issue, collect `home-assistant.log` and raise an issue. I will try to look into issues although my time is limited and it may take me some time. 


## Attribution

Based on [HA-Alarms-and-Reminders][upstream] by @omaramin-2000 and modified by vibe coding .

[upstream]: https://github.com/omaramin-2000/HA-Alarms-and-Reminders







