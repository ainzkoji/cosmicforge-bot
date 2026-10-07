"""Outbound destination guard (SSRF protection) for caller-supplied broker addresses.

Pure logic: IP literals and an injected resolver only -- no test here touches
the network or the system resolver.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from shared_lib.core.security.url_guard import (
    OutboundPolicy,
    UnsafeDestinationError,
    parse_allowed_hosts,
    validate_outbound_host_port,
    validate_outbound_url,
)


def _resolver(mapping):
    def resolve(host, port):
        return mapping[host]

    return resolve


def _no_dns(host, port):  # pragma: no cover - only runs if a test regresses
    raise AssertionError(f"unexpected DNS lookup for {host!r}")


def _reason(call, *args, **kwargs) -> str:
    with pytest.raises(UnsafeDestinationError) as exc_info:
        call(*args, **kwargs)
    return exc_info.value.reason


# ── always refused, in every environment, allow-list or not ────────────────

@pytest.mark.parametrize("production", [True, False])
@pytest.mark.parametrize(
    "url",
    [
        "http://169.254.169.254/latest/meta-data/",   # cloud metadata (link-local)
        "http://169.254.0.1/",
        "http://[fe80::1]/",                           # IPv6 link-local
        "http://[::ffff:169.254.169.254]/",            # IPv4-mapped metadata
        "http://[fd00:ec2::254]/",                     # AWS IMDS over IPv6
        "http://100.100.100.200/",                     # Alibaba metadata
        "http://224.0.0.1/",                           # multicast
        "http://[ff02::1]/",
        "http://0.0.0.0/",                             # unspecified
        "http://[::]/",
        "http://240.0.0.1/",                           # reserved
        "http://255.255.255.255/",                     # broadcast
    ],
)
def test_metadata_link_local_multicast_unspecified_reserved_always_blocked(url, production):
    assert _reason(validate_outbound_url, url, production=production, resolver=_no_dns) == "DESTINATION_BLOCKED"
    # The allow-list can never open these.
    host = url.split("//", 1)[1].split("/", 1)[0]
    assert (
        _reason(validate_outbound_url, url, production=production, allowed_hosts=host, resolver=_no_dns)
        == "DESTINATION_BLOCKED"
    )


@pytest.mark.parametrize("production", [True, False])
def test_hostname_resolving_to_metadata_is_blocked(production):
    resolver = _resolver({"metadata.attacker.example": ["169.254.169.254"]})
    assert (
        _reason(validate_outbound_url, "http://metadata.attacker.example/", production=production, resolver=resolver)
        == "DESTINATION_BLOCKED"
    )
    assert (
        _reason(validate_outbound_host_port, "metadata.attacker.example", 80, production=production, resolver=resolver)
        == "DESTINATION_BLOCKED"
    )


# ── loopback / private: production needs the allow-list ─────────────────────

INTERNAL_URLS = [
    "https://127.0.0.1:5000/v1/api",
    "http://[::1]:5000/",
    "http://10.0.0.5/",
    "http://172.16.3.4/",
    "http://192.168.1.10:8443/",
    "http://100.64.0.1/",          # CGNAT
    "http://[fc00::1]/",           # IPv6 unique-local
    "http://[::ffff:127.0.0.1]/",  # IPv4-mapped loopback
]


@pytest.mark.parametrize("url", INTERNAL_URLS)
def test_internal_addresses_refused_in_production(url):
    assert _reason(validate_outbound_url, url, production=True, resolver=_no_dns) == "DESTINATION_NOT_ALLOWED"


@pytest.mark.parametrize("url", INTERNAL_URLS)
def test_internal_addresses_allowed_outside_production(url):
    result = validate_outbound_url(url, production=False, resolver=_no_dns)
    assert result.addresses


def test_environment_defaults_to_production_when_unspecified():
    assert _reason(validate_outbound_url, "http://127.0.0.1/", resolver=_no_dns) == "DESTINATION_NOT_ALLOWED"
    assert _reason(validate_outbound_host_port, "127.0.0.1", 4001, resolver=_no_dns) == "DESTINATION_NOT_ALLOWED"
    # A settings object with no ``production`` attribute fails closed too.
    assert OutboundPolicy.from_settings(SimpleNamespace()).production is True


def test_production_allow_list_matches_host_and_port():
    ok = validate_outbound_host_port("127.0.0.1", 4001, production=True, allowed_hosts="127.0.0.1:4001", resolver=_no_dns)
    assert (ok.connect_host, ok.port) == ("127.0.0.1", 4001)
    # Same host, different port: not listed.
    assert (
        _reason(validate_outbound_host_port, "127.0.0.1", 4002, production=True,
                allowed_hosts="127.0.0.1:4001", resolver=_no_dns)
        == "DESTINATION_NOT_ALLOWED"
    )
    # A bare host entry allows any port.
    assert validate_outbound_url("http://10.0.0.9:8443/x", production=True, allowed_hosts="10.0.0.9", resolver=_no_dns)
    # Hostname entry: the name is allow-listed, the private address it resolves to is accepted.
    resolver = _resolver({"gw.internal": ["10.0.0.9"]})
    named = validate_outbound_host_port("GW.Internal", 4001, production=True, allowed_hosts="gw.internal:4001",
                                        resolver=resolver)
    assert named.connect_host == "10.0.0.9"
    # IPv6 with a port uses brackets.
    assert validate_outbound_host_port("::1", 4001, production=True, allowed_hosts="[::1]:4001", resolver=_no_dns)


def test_public_host_with_one_private_address_is_refused_in_production():
    """DNS answers mixing a public and an internal address must not slip through."""
    resolver = _resolver({"mixed.example": ["93.184.216.34", "10.0.0.5"]})
    assert (
        _reason(validate_outbound_url, "https://mixed.example/", production=True, resolver=resolver)
        == "DESTINATION_NOT_ALLOWED"
    )


def test_public_destination_allowed_and_resolved_addresses_returned():
    resolver = _resolver({"vps.example.com": ["93.184.216.34"]})
    result = validate_outbound_url("https://vps.example.com:8443/base", production=True, resolver=resolver)
    assert (result.scheme, result.host, result.port) == ("https", "vps.example.com", 8443)
    assert result.addresses == ("93.184.216.34",)
    assert result.connect_host == "93.184.216.34"   # connect to what was validated
    assert validate_outbound_url("http://93.184.216.34/", production=True, resolver=_no_dns).port == 80
    assert validate_outbound_url("https://93.184.216.34/", production=True, resolver=_no_dns).port == 443


def test_numeric_host_tricks_are_classified_by_what_they_resolve_to():
    # "2130706433" and "0x7f.1" are 127.0.0.1 to getaddrinfo/inet_aton.
    resolver = _resolver({"2130706433": ["127.0.0.1"], "0x7f.1": ["127.0.0.1"]})
    for host in ("2130706433", "0x7f.1"):
        assert (
            _reason(validate_outbound_host_port, host, 80, production=True, resolver=resolver)
            == "DESTINATION_NOT_ALLOWED"
        )


# ── URL shape ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("production", [True, False])
@pytest.mark.parametrize(
    "url,reason",
    [
        ("ftp://93.184.216.34/", "SCHEME_NOT_ALLOWED"),
        ("file:///etc/passwd", "SCHEME_NOT_ALLOWED"),
        ("gopher://93.184.216.34:70/_x", "SCHEME_NOT_ALLOWED"),
        ("93.184.216.34:8443", "SCHEME_NOT_ALLOWED"),
        ("//93.184.216.34/", "SCHEME_NOT_ALLOWED"),
        ("http://user:pw@93.184.216.34/", "URL_CREDENTIALS_NOT_ALLOWED"),
        ("http://user@93.184.216.34/", "URL_CREDENTIALS_NOT_ALLOWED"),
        ("http://@93.184.216.34/", "URL_CREDENTIALS_NOT_ALLOWED"),
        ("https://93.184.216.34:0/", "PORT_INVALID"),
        ("https://93.184.216.34:65536/", "PORT_INVALID"),
        ("https://93.184.216.34:99999/", "PORT_INVALID"),
        ("http://93.184.216.34\\@127.0.0.1/", "URL_INVALID"),
        ("http://93.184.216.34/\r\nHost: 127.0.0.1", "URL_INVALID"),
        ("http:///nohost", "HOST_REQUIRED"),
        ("", "URL_REQUIRED"),
        (None, "URL_REQUIRED"),
        (12345, "URL_REQUIRED"),
    ],
)
def test_bad_urls_are_rejected(url, reason, production):
    assert _reason(validate_outbound_url, url, production=production, resolver=_no_dns) == reason


@pytest.mark.parametrize("port", [0, -1, 65536, 99999, "abc", None, True, 1.5e9])
def test_port_must_be_1_to_65535(port):
    assert _reason(validate_outbound_host_port, "93.184.216.34", port, production=False, resolver=_no_dns) == "PORT_INVALID"


@pytest.mark.parametrize("port", [1, 4001, 7497, 65535, "4001"])
def test_valid_ports_accepted(port):
    assert validate_outbound_host_port("93.184.216.34", port, production=True, resolver=_no_dns).port == int(port)


@pytest.mark.parametrize("host", ["", "   ", "a b", "evil.example/path", "user@evil.example", "evil.example#x", 1234, None])
def test_bad_hosts_are_rejected(host):
    assert _reason(validate_outbound_host_port, host, 80, production=False, resolver=_no_dns) in {
        "HOST_REQUIRED", "HOST_INVALID",
    }


def test_unresolvable_host_is_rejected():
    def failing(host, port):
        raise OSError("no such host")

    assert _reason(validate_outbound_url, "https://nope.invalid/", production=False, resolver=failing) == "HOST_UNRESOLVABLE"
    assert (
        _reason(validate_outbound_url, "https://empty.invalid/", production=False, resolver=lambda h, p: [])
        == "HOST_UNRESOLVABLE"
    )


def test_error_message_never_echoes_url_credentials():
    with pytest.raises(UnsafeDestinationError) as exc_info:
        validate_outbound_url("http://alice:hunter2secret@93.184.216.34/", production=False, resolver=_no_dns)
    assert "hunter2secret" not in str(exc_info.value)
    assert "alice" not in str(exc_info.value)


def test_parse_allowed_hosts():
    parsed = parse_allowed_hosts(" 127.0.0.1:4001, [::1]:4001 ,GW.Internal,, ")
    assert parsed == frozenset({("127.0.0.1", 4001), ("::1", 4001), ("gw.internal", None)})
    assert parse_allowed_hosts("") == frozenset()
    assert parse_allowed_hosts(None) == frozenset()


def test_policy_from_settings_reads_environment_and_allow_list():
    policy = OutboundPolicy.from_settings(
        SimpleNamespace(production=True, BROKER_GATEWAY_ALLOWED_HOSTS="127.0.0.1:4001")
    )
    assert validate_outbound_host_port("127.0.0.1", 4001, policy=policy, resolver=_no_dns).port == 4001
    assert _reason(validate_outbound_host_port, "127.0.0.1", 7496, policy=policy, resolver=_no_dns) == "DESTINATION_NOT_ALLOWED"
    dev = OutboundPolicy.from_settings(SimpleNamespace(production=False))
    assert validate_outbound_host_port("127.0.0.1", 7496, policy=dev, resolver=_no_dns).connect_host == "127.0.0.1"
