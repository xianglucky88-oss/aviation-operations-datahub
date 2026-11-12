from __future__ import annotations

from .lifecycle_model import LifecycleConfig
from .lifecycle_store import LifecycleStore
from .normalize import normalize
from .pipeline import IngestionPipeline
from .sources import FixtureSource, HttpJsonSource, RawLanding
from .storage import DataStore


def _run(batch, database: str, raw_dir: str | None) -> dict[str, int]:
    landing = RawLanding(raw_dir)
    digest, raw_path = landing.save(batch)
    store = DataStore(database)
    lifecycle_store = LifecycleStore(database)
    batch_id = store.start_batch(batch.source)
    store.save_manifest(digest, batch.source, batch.content_type, len(batch.raw_body), raw_path)
    try:
        return IngestionPipeline(
            batch.records,
            batch.source,
            store,
            batch_id,
            normalize,
            lifecycle_store=lifecycle_store,
        ).run()
    except Exception as exc:
        store.finish_batch(batch_id, "failed", 0, 0, 0, str(exc))
        raise
    finally:
        lifecycle_store.close()
        store.close()


def run_fixture(fixture: str, database: str, raw_dir: str | None = None) -> dict[str, int]:
    return _run(FixtureSource().fetch(fixture), database, raw_dir)


def run_url(url: str, database: str, raw_dir: str | None = None, timeout: float = 30.0, records_key: str | None = None) -> dict[str, int]:
    return _run(HttpJsonSource(url, timeout=timeout, records_key=records_key).fetch(), database, raw_dir)


def rebuild_flight_lifecycle(
    database: str,
    flight_id: str,
    flight_date: str,
    config: LifecycleConfig | None = None,
    *,
    force: bool = False,
    lock_owner: str = "lifecycle-worker",
) -> dict:
    """Rebuild (or verify) the lifecycle read model for a single flight."""

    store = LifecycleStore(database, lock_owner=lock_owner)
    try:
        return store.rebuild_flight(flight_id, flight_date, config, force=force)
    finally:
        store.close()


def load_flight_lifecycle(database: str, flight_id: str, flight_date: str, version_id: int | None = None) -> dict:
    from .identity import flight_key

    store = LifecycleStore(database)
    try:
        key = flight_key(flight_id, flight_date)
        return store.load_trajectory(key, version_id)
    finally:
        store.close()


def export_flight_lifecycle(database: str, flight_id: str, flight_date: str, version_id: int | None = None) -> dict:
    from .identity import flight_key

    store = LifecycleStore(database)
    try:
        key = flight_key(flight_id, flight_date)
        return store.export_trajectory(key, version_id)
    finally:
        store.close()
