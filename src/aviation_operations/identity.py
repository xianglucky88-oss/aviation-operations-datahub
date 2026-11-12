def record_key(flight_id: str, flight_date: str, observed_at: str) -> str:
    return "|".join(value.strip().upper() for value in (flight_id, flight_date, observed_at))

def flight_key(flight_id: str, flight_date: str) -> str:
    return f"{flight_id.strip().upper()}|{flight_date.strip()}"

def airport_key(code: str) -> str:
    return code.strip().upper()
