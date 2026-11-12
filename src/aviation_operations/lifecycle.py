def event_family(status: str) -> str:
    if status in {"delayed", "cancelled"}:
        return "disruption"
    if status in {"departed", "arrived"}:
        return "movement"
    return "ground_operation"
