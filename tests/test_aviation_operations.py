from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

from aviation_operations.application import run_fixture, run_url
from aviation_operations.normalize import normalize
from aviation_operations.quality import validate


class Handler(BaseHTTPRequestHandler):
    body = b"[]"

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(self.body)))
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, *_args):
        return


def test_fixture_pipeline_is_idempotent(tmp_path):
    fixture = Path(__file__).parents[1] / "fixtures" / "sample.json"
    database = tmp_path / "data.sqlite3"
    first = run_fixture(str(fixture), str(database), str(tmp_path / "raw"))
    second = run_fixture(str(fixture), str(database), str(tmp_path / "raw"))
    assert first["processed"] == 4
    assert first["accepted"] == 3
    assert first["rejected"] == 1
    assert second["changed"] == 0
    assert second["records"] == 3
    assert second["batches"] == 2
    assert second["raw_manifests"] == 1


def test_http_source_runs_end_to_end_and_lands_raw(tmp_path):
    fixture = Path(__file__).parents[1] / "fixtures" / "sample.json"
    Handler.body = fixture.read_bytes()
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        result = run_url(f"http://127.0.0.1:{server.server_port}/records", str(tmp_path / "http.sqlite3"), str(tmp_path / "raw"))
        assert result["accepted"] == 3
        assert len(list((tmp_path / "raw").glob("*.json"))) == 1
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_quality_rejects_invalid_domain_value():
    fixture = json.loads((Path(__file__).parents[1] / "fixtures" / "sample.json").read_text())
    invalid = fixture[-1]
    try:
        record = normalize(invalid, invalid["source"])
    except ValueError:
        return
    assert validate(record)
