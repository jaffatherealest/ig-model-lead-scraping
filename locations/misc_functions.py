from datetime import datetime, timezone

def convert_taken_at_to_iso(taken_at: int) -> str:
    """
    Converts a UNIX timestamp (taken_at) into a date string formatted as YYYY-MM-DD.
    
    :param taken_at: UNIX timestamp (int)
    :return: Date string in the format YYYY-MM-DD (UTC)
    """
    dt = datetime.fromtimestamp(taken_at, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")  # Formats as YYYY-MM-DD