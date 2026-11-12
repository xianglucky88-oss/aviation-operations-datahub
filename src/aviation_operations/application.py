from __future__ import annotations

from .identity_service import IdentityService
from .normalize import normalize
from .pipeline import IngestionPipeline
from .sources import FixtureSource, HttpJsonSource, RawLanding
from .storage import DataStore


def _run(
    batch,
    database: str,
    raw_dir: str | None,
    identity_mapping: str | None = None,
) -> dict[str, int]:
    landing = RawLanding(raw_dir)
    digest, raw_path = landing.save(batch)
    store = DataStore(database)
    batch_id = store.start_batch(batch.source)
    store.save_manifest(digest, batch.source, batch.content_type, len(batch.raw_body), raw_path)
    identity_service = None
    if identity_mapping:
        identity_service = IdentityService(database)
        identity_service.load_bundle_from_file(identity_mapping)
    try:
        return IngestionPipeline(
            batch.records,
            batch.source,
            store,
            batch_id,
            normalize,
            identity_service=identity_service,
        ).run()
    except Exception as exc:
        store.finish_batch(batch_id, "failed", 0, 0, 0, str(exc))
        raise
    finally:
        store.close()
        if identity_service is not None:
            identity_service.close()


def run_fixture(
    fixture: str,
    database: str,
    raw_dir: str | None = None,
    identity_mapping: str | None = None,
) -> dict[str, int]:
    return _run(
        FixtureSource().fetch(fixture), database, raw_dir, identity_mapping=identity_mapping
    )


def run_url(
    url: str,
    database: str,
    raw_dir: str | None = None,
    timeout: float = 30.0,
    records_key: str | None = None,
    identity_mapping: str | None = None,
) -> dict[str, int]:
    return _run(
        HttpJsonSource(url, timeout=timeout, records_key=records_key).fetch(),
        database,
        raw_dir,
        identity_mapping=identity_mapping,
    )
