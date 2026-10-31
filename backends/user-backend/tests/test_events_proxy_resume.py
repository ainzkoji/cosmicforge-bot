"""Section K3 -- the portal's event-stream proxy forwards Last-Event-ID so the engine can replay."""
from app.api.events_proxy import upstream_headers


def test_the_header_is_forwarded_and_the_query_is_the_fallback():
    assert upstream_headers("t", "evt_1", None) == {"Authorization": "Bearer t", "Last-Event-ID": "evt_1"}
    assert upstream_headers("t", None, "evt_2") == {"Authorization": "Bearer t", "Last-Event-ID": "evt_2"}
    assert upstream_headers("t", "evt_1", "evt_2")["Last-Event-ID"] == "evt_1"


def test_without_a_resume_point_or_with_an_oversized_one_nothing_is_forwarded():
    assert upstream_headers("t", None, None) == {"Authorization": "Bearer t"}
    assert "Last-Event-ID" not in upstream_headers("t", "x" * 129, None)
