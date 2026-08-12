# filepath: /ha-alarm-clock/custom_components/ha_alarm_clock/const.py

"""Constants for the HA Alarm Clock integration."""

DOMAIN = "ha_alarm_clock"

# Services
SERVICE_SET_ALARM = "set_alarm"
SERVICE_SET_REMINDER = "set_reminder"
SERVICE_STOP_ALARM = "stop_alarm"
SERVICE_SNOOZE_ALARM = "snooze_alarm"
SERVICE_STOP_REMINDER = "stop_reminder"
SERVICE_SNOOZE_REMINDER = "snooze_reminder"
SERVICE_STOP_ALL_ALARMS = "stop_all_alarms"
SERVICE_STOP_ALL_REMINDERS = "stop_all_reminders"
SERVICE_STOP_ALL = "stop_all"
SERVICE_EDIT_ALARM = "edit_alarm"
SERVICE_EDIT_REMINDER = "edit_reminder"
SERVICE_DUPLICATE_ALARM = "duplicate_alarm"
SERVICE_DUPLICATE_REMINDER = "duplicate_reminder"
SERVICE_DELETE_ALARM = "delete_alarm"
SERVICE_DELETE_REMINDER = "delete_reminder"
SERVICE_DELETE_ALL_ALARMS = "delete_all_alarms"
SERVICE_DELETE_ALL_REMINDERS = "delete_all_reminders"
SERVICE_DELETE_ALL = "delete_all"

# Attributes
ATTR_DATETIME = "datetime"      # A string containing the reminder time
ATTR_MESSAGE = "message"        # The announcement message (optional)
ATTR_ALARM_ID = "alarm_id"
ATTR_SNOOZE_MINUTES = "minutes"
ATTR_REMINDER_ID = "reminder_id"  
ATTR_MEDIA_PLAYER = "media_player"
ATTR_NAME = "name"             # Attribute name
ATTR_NOTIFY_DEVICE = "notify_device"
ATTR_NOTIFY_TITLE = "HA Alarm Clock"
ATTR_SPOTIFY_SOURCE = "spotify_source"
ATTR_VOLUME = "volume"
ATTR_USER_ID = "user_id"
ATTR_USER_NAME = "user_name"

# Configuration
CONF_ALARM_SOUND = "alarm_sound"
CONF_REMINDER_SOUND = "reminder_sound"
CONF_MEDIA_PLAYER = "media_player"
CONF_HIDDEN_MEDIA_PLAYERS = "hidden_media_players"
CONF_ALLOWED_ACTIVATION_ENTITIES = "allowed_activation_entities"
CONF_ENABLE_LLM = "enable_llm"
CONF_DEFAULT_SNOOZE_MINUTES = "default_snooze_minutes"
CONF_ACTIVE_PRESS_MODE = "active_press_mode"

ACTIVE_PRESS_MODE_SHORT_STOP_LONG_SNOOZE = "short_stop_long_snooze"
ACTIVE_PRESS_MODE_SHORT_SNOOZE_LONG_STOP = "short_snooze_long_stop"

# Defaults
DEFAULT_NAME = "HA Alarm Clock"  # Config flow
DEFAULT_MESSAGE = "Reminder!"
DEFAULT_ALARM_SOUND = "/media/local/Alarms/birds.mp3"
DEFAULT_REMINDER_SOUND = "/media/local/Alarms/ringtone.mp3"
DEFAULT_MEDIA_PLAYER = None
DEFAULT_HIDDEN_MEDIA_PLAYERS = []
DEFAULT_ALLOWED_ACTIVATION_ENTITIES = []
DEFAULT_SNOOZE_MINUTES = 5  # Default snooze time in minutes
DEFAULT_ACTIVE_PRESS_MODE = ACTIVE_PRESS_MODE_SHORT_STOP_LONG_SNOOZE
DEFAULT_NOTIFICATION_TITLE = "HA Alarm Clock"
DEFAULT_ENABLE_LLM = False

# An item still marked "active" in storage on startup only resumes ringing if
# its scheduled time is within this many minutes of now; older ones are stale
# (e.g. HA was down for hours/days) and get rescheduled or expired instead.
ACTIVE_RESUME_GRACE_MINUTES = 15

SPOTIFY_PLATFORMS = {
	"spotify",
	"spotifyplus",
}

# Entity domains and IDs
ALARM_ENTITY_DOMAIN = f"{DOMAIN}_alarm"
REMINDER_ENTITY_DOMAIN = f"{DOMAIN}_reminder"
DASHBOARD_ENTITY_ID = "sensor.ha_alarm_clock"
