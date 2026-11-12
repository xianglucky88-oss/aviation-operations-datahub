from __future__ import annotations

from .normalize import normalize
from .pipeline import IngestionPipeline
from .sources import FixtureSource, HttpJsonSource, RawLanding
from .storage import DataStore


def _run(batch, database: str, raw_dir: str | None) -> dict[str, int]:
    landing = RawLanding(raw_dir)
    digest, raw_path = landing.save(batch)
    store = DataStore(database)
    batch_id = store.start_batch(batch.source)
    store.save_manifest(digest, batch.source, batch.content_type, len(batch.raw_body), raw_path)
    try:
        return IngestionPipeline(batch.records, batch.source, store, batch_id, normalize).run()
    except Exception as exc:
        store.finish_batch(batch_id, "failed", 0, 0, 0, str(exc))
        raise
    finally:
        store.close()


def run_fixture(fixture: str, database: str, raw_dir: str | None = None) -> dict[str, int]:
    return _run(FixtureSource().fetch(fixture), database, raw_dir)


def run_url(url: str, database: str, raw_dir: str | None = None, timeout: float = 30.0, records_key: str | None = None) -> dict[str, int]:
    return _run(HttpJsonSource(url, timeout=timeout, records_key=records_key).fetch(), database, raw_dir)
