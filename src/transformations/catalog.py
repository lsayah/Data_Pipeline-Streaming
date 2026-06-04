"""Fonctions de transformation du catalogue musical."""

MAX_DURATION_MS = 36_000_000  # 10 heures


def normalize_artist_name(name: str) -> str:
    if name is None:
        return None
    return name.strip().title()


def validate_track_schema(track: dict) -> list:
    errors = []
    for field in ("id", "artist_id", "title", "duration_ms"):
        if field not in track or track[field] is None:
            errors.append(f"champ manquant : {field}")
    try:
        duration = int(track.get("duration_ms", 0))
        if duration <= 0:
            errors.append("duration_ms doit être positif")
        elif duration > MAX_DURATION_MS:
            errors.append("duration_ms trop long (> 10h)")
    except (ValueError, TypeError):
        errors.append("duration_ms invalide")
    return errors


def deduplicate_tracks(artists: list) -> list:
    seen = set()
    result = []
    for artist in artists:
        key = (normalize_artist_name(artist.get("name", "")), artist.get("label", ""))
        if key not in seen:
            seen.add(key)
            result.append(artist)
    return result
