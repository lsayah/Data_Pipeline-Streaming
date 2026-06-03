"""Fonctions de validation et enrichissement des événements d'écoute."""

from datetime import datetime, timezone

REQUIRED_FIELDS = {"event_id", "user_id", "track_id", "timestamp", "duration_ms"}
BOT_DURATION_THRESHOLD_MS = 5_000  # < 5s → pattern bot


def is_valid_listening_event(event: dict) -> bool:
    # Champs obligatoires
    if not REQUIRED_FIELDS.issubset(event.keys()):
        return False

    # Timestamp parseable et pas dans le futur
    try:
        ts = datetime.fromisoformat(str(event["timestamp"]).replace("Z", "+00:00"))
        if ts > datetime.now(timezone.utc):
            return False
    except (ValueError, TypeError):
        return False

    # duration_ms > seuil bot
    try:
        if int(event["duration_ms"]) < BOT_DURATION_THRESHOLD_MS:
            return False
    except (ValueError, TypeError):
        return False

    return True


def enrich_listening_event(event: dict, catalogue: dict) -> dict:
    info = catalogue.get(event.get("track_id", ""), {})
    return {**event, **info}
