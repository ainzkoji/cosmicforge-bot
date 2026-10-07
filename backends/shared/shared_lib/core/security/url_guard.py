"""Outbound destination guard (SSRF protection) for caller-supplied addresses.

Some broker integrations connect to an address the *user* supplies: a
self-hosted MT4/MT5 bridge URL, an IBKR Client Portal gateway URL, or a
TWS / IB Gateway ``host:port``. Without a guard, that is a server-side request
forgery primitive: any caller could make the engine talk to the cloud metadata
service, to loopback-only admin ports, or to other machines on the private
network, and read the answer back out of the error message.

Policy (one place, used by every route that connects to a caller address):

* URL schemes: ``http`` / ``https`` only. Credentials in the URL are refused.
* Ports: 1-65535.
* The hostname is resolved and EVERY resolved address is classified:

  - ALWAYS refused, in every environment and regardless of the allow-list:
    link-local (169.254.0.0/16, fe80::/10 -- cloud metadata), multicast,
    unspecified, reserved, broadcast, 0.0.0.0/8 and the well-known metadata
    addresses that live outside link-local space.
  - Internal (loopback, RFC1918, fc00::/7, CGNAT 100.64.0.0/10 and anything
    else that is not globally routable): refused in production unless the
    ``host`` or ``host:port`` is explicitly allow-listed; allowed outside
    production so a local IBKR gateway and the test-suite keep working.

DNS rebinding: validation resolves the name once and returns the addresses it
approved (:attr:`ValidatedDestination.addresses`). Raw TCP callers (TWS / IB
Gateway) should connect to :attr:`ValidatedDestination.connect_host`, which is
the validated IP, so the name is never resolved a second time. HTTP clients
resolve the hostname again themselves (pinning the IP there would break TLS
SNI / certificate verification), so for URLs a hostile DNS server can still
answer differently on the second lookup. With TLS verification on, the
rebound target would also need a valid certificate for the hostname; with
plain ``http`` or verification off this residual risk remains and should be
closed at the network layer (egress firewall). HTTP redirects are followed by
the HTTP client, not by this guard: callers that must not follow a redirect to
an internal address have to disable redirects on their client.

Broker gateway / bridge base URLs go through :func:`validate_gateway_url`, the
stricter variant: no query string or fragment (they would let the caller choose
the path the server requests), and ``https`` only in production unless the host
is allow-listed. To narrow the rebinding window, HTTP callers additionally call
:func:`revalidate_destination` immediately before every request: it resolves
the name again and applies the same address policy. It narrows the window to
the gap between that lookup and the HTTP client's own; it does not close it.

Allow-listed (loopback / private) destinations are operator infrastructure.
Routes that serve ordinary users validate with
:meth:`OutboundPolicy.public_only`, which drops the allow-list, so only an
admin can reach an allow-listed address.

Stdlib only; no network access unless a hostname has to be resolved.
"""
from __future__ import annotations

import ipaddress
import socket
from dataclasses import dataclass
from typing import Callable, Iterable, Optional, Sequence, Tuple, Union
from urllib.parse import urlsplit

__all__ = [
    "OutboundPolicy",
    "UnsafeDestinationError",
    "ValidatedDestination",
    "parse_allowed_hosts",
    "revalidate_destination",
    "validate_gateway_url",
    "validate_outbound_host_port",
    "validate_outbound_url",
]

IPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]
#: ``resolver(host, port) -> iterable of IP strings``. Injectable for tests.
Resolver = Callable[[str, int], Iterable[str]]

ALLOWED_SCHEMES: Tuple[str, ...] = ("http", "https")
_DEFAULT_PORTS = {"http": 80, "https": 443}

# Refused in every environment, allow-list or not.
_ALWAYS_BLOCKED_NETWORKS = tuple(
    ipaddress.ip_network(net)
    for net in (
        "0.0.0.0/8",            # "this network"; 0.0.0.0 reaches loopback on Linux
        "169.254.0.0/16",       # IPv4 link-local (AWS/GCP/Azure/OCI metadata)
        "fe80::/10",            # IPv6 link-local
        "224.0.0.0/4",          # IPv4 multicast
        "ff00::/8",             # IPv6 multicast
        "240.0.0.0/4",          # reserved + 255.255.255.255 broadcast
        "100.100.100.200/32",   # Alibaba Cloud metadata (inside CGNAT space)
        "192.0.0.192/32",       # Oracle Cloud (classic) metadata
        "fd00:ec2::254/128",    # AWS IMDS over IPv6 (inside ULA space)
    )
)

# "Internal": allowed outside production, allow-list only in production.
# ``not is_global`` already covers these on current Pythons; they are listed
# explicitly so the policy does not depend on the interpreter's tables.
_INTERNAL_NETWORKS = tuple(
    ipaddress.ip_network(net)
    for net in (
        "127.0.0.0/8", "::1/128",                          # loopback
        "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",   # RFC1918
        "fc00::/7",                                        # IPv6 unique-local
        "100.64.0.0/10",                                   # CGNAT
    )
)


_NAT64_NETWORK = ipaddress.ip_network("64:ff9b::/96")


class UnsafeDestinationError(ValueError):
    """A caller-supplied destination failed the outbound policy.

    ``reason`` is a stable machine-readable code. The message never contains
    the URL's credentials or the caller's full input.
    """

    def __init__(self, reason: str, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason}: {detail}" if detail else reason)


@dataclass(frozen=True)
class ValidatedDestination:
    """A destination that passed the policy, with the addresses it resolved to."""

    host: str
    port: int
    addresses: Tuple[str, ...]
    scheme: Optional[str] = None
    url: Optional[str] = None

    @property
    def connect_host(self) -> str:
        """The validated IP to connect to (avoids a second DNS lookup)."""
        return self.addresses[0]


@dataclass(frozen=True)
class OutboundPolicy:
    """Environment-bound policy: is this production, and what is allow-listed."""

    production: bool = True
    allowed_hosts: frozenset = frozenset()

    @classmethod
    def from_settings(cls, settings: object) -> "OutboundPolicy":
        """Build from an application settings object.

        Fails closed: a settings object without a ``production`` attribute is
        treated as production.
        """
        return cls(
            production=bool(getattr(settings, "production", True)),
            allowed_hosts=parse_allowed_hosts(getattr(settings, "BROKER_GATEWAY_ALLOWED_HOSTS", "")),
        )

    def public_only(self) -> "OutboundPolicy":
        """The same environment without the allow-list.

        For callers who are not operators: in production every loopback /
        private address is refused, allow-listed or not. Outside production
        the policy is unchanged (internal addresses are allowed there anyway).
        """
        return OutboundPolicy(production=self.production, allowed_hosts=frozenset())


def _normalize_host(host: str) -> str:
    host = str(host or "").strip().lower().rstrip(".")
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    return host


def _split_host_port_entry(entry: str) -> Tuple[str, Optional[int]]:
    """Parse one allow-list entry: ``host``, ``host:port``, ``[v6]``, ``[v6]:port``."""
    entry = entry.strip().lower()
    if entry.startswith("["):
        host, _, rest = entry[1:].partition("]")
        port_s = rest[1:] if rest.startswith(":") else ""
    elif entry.count(":") == 1:
        host, _, port_s = entry.partition(":")
    else:  # bare hostname / IPv4, or an un-bracketed IPv6 literal (no port)
        host, port_s = entry, ""
    port: Optional[int] = None
    if port_s:
        try:
            port = int(port_s)
        except ValueError:
            port = -1  # malformed entry can never match
    return _normalize_host(host), port


def parse_allowed_hosts(raw: Union[str, Iterable[str], None]) -> frozenset:
    """Parse ``BROKER_GATEWAY_ALLOWED_HOSTS`` (comma-separated) into entries.

    Each entry is ``host`` (any port) or ``host:port``. IPv6 literals with a
    port use brackets: ``[::1]:4001``.
    """
    if raw is None:
        return frozenset()
    items = raw.split(",") if isinstance(raw, str) else list(raw)
    out = set()
    for item in items:
        item = str(item or "").strip()
        if not item:
            continue
        host, port = _split_host_port_entry(item)
        if host:
            out.add((host, port))
    return frozenset(out)


def _is_allow_listed(names: Iterable[str], port: int, allowed: frozenset) -> bool:
    for name in names:
        name = _normalize_host(name)
        if (name, None) in allowed or (name, port) in allowed:
            return True
    return False


def _embedded_ipv4(ip: IPAddress) -> Optional[ipaddress.IPv4Address]:
    """IPv4 address tunnelled inside an IPv6 one (mapped, 6to4, NAT64, Teredo)."""
    if not isinstance(ip, ipaddress.IPv6Address):
        return None
    if ip.ipv4_mapped is not None:
        return ip.ipv4_mapped
    if ip.sixtofour is not None:
        return ip.sixtofour
    if ip in _NAT64_NETWORK:
        return ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF)
    if ip.teredo is not None:
        return ip.teredo[1]
    return None


def _classify(ip: IPAddress) -> str:
    """``blocked`` | ``internal`` | ``public`` for one address."""
    inner = _embedded_ipv4(ip)
    if inner is not None:
        inner_class = _classify(inner)
        if inner_class != "public":
            return inner_class
        # IPv4-mapped and NAT64 (DNS64 networks) are just the IPv4 address.
        # 6to4 / Teredo wrappers fall through and are judged as IPv6.
        if ip.ipv4_mapped is not None or ip in _NAT64_NETWORK:  # type: ignore[union-attr]
            return "public"
    same_family = lambda nets: (net for net in nets if net.version == ip.version)  # noqa: E731
    if any(ip in net for net in same_family(_ALWAYS_BLOCKED_NETWORKS)):
        return "blocked"
    if ip.is_link_local or ip.is_multicast or ip.is_unspecified:
        return "blocked"
    # Before ``is_reserved``: Python reports ::1 (inside ::/8) as reserved.
    if any(ip in net for net in same_family(_INTERNAL_NETWORKS)):
        return "internal"
    if ip.is_reserved:
        return "blocked"
    if ip.is_loopback or ip.is_private or not ip.is_global:
        return "internal"
    return "public"


def _system_resolver(host: str, port: int) -> Iterable[str]:
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return [info[4][0] for info in infos]


def _check_host_syntax(host: str) -> None:
    if not host:
        raise UnsafeDestinationError("HOST_REQUIRED")
    if len(host) > 253:
        raise UnsafeDestinationError("HOST_INVALID", "hostname too long")
    for ch in host:
        if ch.isspace() or ord(ch) < 0x21 or ord(ch) == 0x7F or ch in "@/\\?#%":
            raise UnsafeDestinationError("HOST_INVALID", "illegal character in host")


def _coerce_port(port: object) -> int:
    if isinstance(port, bool):
        raise UnsafeDestinationError("PORT_INVALID")
    try:
        value = int(port)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        raise UnsafeDestinationError("PORT_INVALID") from None
    if not 1 <= value <= 65535:
        raise UnsafeDestinationError("PORT_INVALID", "port must be 1-65535")
    return value


def _resolve_and_check(
    host: str,
    port: int,
    *,
    production: bool,
    allowed_hosts: frozenset,
    resolver: Optional[Resolver],
) -> Tuple[str, ...]:
    """Resolve ``host`` and apply the address policy to every result."""
    literal: Optional[IPAddress]
    try:
        literal = ipaddress.ip_address(host.split("%", 1)[0])
    except ValueError:
        literal = None

    if literal is not None:
        candidates = [str(literal)]
    else:
        try:
            candidates = [str(a) for a in (resolver or _system_resolver)(host, port)]
        except UnsafeDestinationError:
            raise
        except Exception:
            raise UnsafeDestinationError("HOST_UNRESOLVABLE", "hostname did not resolve") from None
        if not candidates:
            raise UnsafeDestinationError("HOST_UNRESOLVABLE", "hostname did not resolve")

    addresses = []
    for raw in candidates:
        try:
            ip = ipaddress.ip_address(str(raw).split("%", 1)[0])
        except ValueError:
            raise UnsafeDestinationError("HOST_UNRESOLVABLE", "resolver returned a non-IP value") from None
        kind = _classify(ip)
        if kind == "blocked":
            raise UnsafeDestinationError(
                "DESTINATION_BLOCKED",
                "link-local, metadata, multicast, unspecified and reserved addresses are never allowed",
            )
        if kind == "internal" and production and not _is_allow_listed((host, str(ip)), port, allowed_hosts):
            raise UnsafeDestinationError(
                "DESTINATION_NOT_ALLOWED",
                "loopback/private addresses must be listed in BROKER_GATEWAY_ALLOWED_HOSTS in production",
            )
        text = str(ip)
        if text not in addresses:
            addresses.append(text)
    return tuple(addresses)


def _unpack_policy(
    policy: Optional[OutboundPolicy], production: Optional[bool], allowed_hosts: object
) -> Tuple[bool, frozenset]:
    if policy is not None:
        return bool(policy.production), frozenset(policy.allowed_hosts)
    allowed = allowed_hosts if isinstance(allowed_hosts, frozenset) else parse_allowed_hosts(allowed_hosts)  # type: ignore[arg-type]
    # Fail closed: an unspecified environment is production.
    return (True if production is None else bool(production)), allowed


def validate_outbound_host_port(
    host: object,
    port: object,
    *,
    policy: Optional[OutboundPolicy] = None,
    production: Optional[bool] = None,
    allowed_hosts: Union[str, Iterable[str], frozenset, None] = None,
    resolver: Optional[Resolver] = None,
) -> ValidatedDestination:
    """Validate a raw ``host:port`` destination (e.g. TWS / IB Gateway).

    Raises :class:`UnsafeDestinationError`. Connect to
    ``result.connect_host`` rather than ``host`` to avoid a second DNS lookup.
    """
    is_production, allowed = _unpack_policy(policy, production, allowed_hosts)
    if not isinstance(host, str):
        raise UnsafeDestinationError("HOST_INVALID", "host must be a string")
    normalized = _normalize_host(host)
    _check_host_syntax(normalized)
    port_value = _coerce_port(port)
    addresses = _resolve_and_check(
        normalized, port_value, production=is_production, allowed_hosts=allowed, resolver=resolver
    )
    return ValidatedDestination(host=normalized, port=port_value, addresses=addresses)


def validate_outbound_url(
    url: object,
    *,
    policy: Optional[OutboundPolicy] = None,
    production: Optional[bool] = None,
    allowed_hosts: Union[str, Iterable[str], frozenset, None] = None,
    resolver: Optional[Resolver] = None,
    allowed_schemes: Sequence[str] = ALLOWED_SCHEMES,
) -> ValidatedDestination:
    """Validate a caller-supplied ``http(s)`` URL before the server requests it.

    Raises :class:`UnsafeDestinationError`.
    """
    is_production, allowed = _unpack_policy(policy, production, allowed_hosts)
    if not isinstance(url, str) or not url.strip():
        raise UnsafeDestinationError("URL_REQUIRED")
    text = url.strip()
    if len(text) > 2048:
        raise UnsafeDestinationError("URL_INVALID", "URL too long")
    # Characters on which URL parsers disagree (parser-differential SSRF).
    if any(ch.isspace() or ord(ch) < 0x20 or ord(ch) == 0x7F or ch == "\\" for ch in text):
        raise UnsafeDestinationError("URL_INVALID", "illegal character in URL")
    try:
        parts = urlsplit(text)
    except ValueError:
        raise UnsafeDestinationError("URL_INVALID") from None
    scheme = (parts.scheme or "").lower()
    if scheme not in {s.lower() for s in allowed_schemes}:
        raise UnsafeDestinationError("SCHEME_NOT_ALLOWED", "only http and https are allowed")
    if "@" in parts.netloc or parts.username is not None or parts.password is not None:
        raise UnsafeDestinationError("URL_CREDENTIALS_NOT_ALLOWED", "credentials in the URL are not allowed")
    try:
        raw_port = parts.port
    except ValueError:
        raise UnsafeDestinationError("PORT_INVALID", "port must be 1-65535") from None
    host = _normalize_host(parts.hostname or "")
    _check_host_syntax(host)
    port_value = _coerce_port(raw_port if raw_port is not None else _DEFAULT_PORTS[scheme])
    addresses = _resolve_and_check(
        host, port_value, production=is_production, allowed_hosts=allowed, resolver=resolver
    )
    return ValidatedDestination(host=host, port=port_value, addresses=addresses, scheme=scheme, url=text)


def validate_gateway_url(
    url: object,
    *,
    policy: Optional[OutboundPolicy] = None,
    production: Optional[bool] = None,
    allowed_hosts: Union[str, Iterable[str], frozenset, None] = None,
    resolver: Optional[Resolver] = None,
) -> ValidatedDestination:
    """Validate a caller-supplied broker gateway / bridge BASE URL.

    Everything :func:`validate_outbound_url` checks, plus:

    * no query string and no fragment, in every environment: the server
      appends its own endpoint path to this URL, and a trailing ``?x=`` or
      ``#`` would turn that path into a query / fragment and leave the request
      path to the caller;
    * in production the scheme must be ``https`` unless the host (or
      ``host:port``) is allow-listed -- plain ``http`` carries the bearer token
      in clear text and has no certificate check against DNS rebinding.

    Raises :class:`UnsafeDestinationError`.
    """
    is_production, allowed = _unpack_policy(policy, production, allowed_hosts)
    if isinstance(url, str) and ("?" in url or "#" in url):
        raise UnsafeDestinationError(
            "URL_QUERY_NOT_ALLOWED", "a gateway URL must not contain a query string or a fragment"
        )
    destination = validate_outbound_url(
        url, production=is_production, allowed_hosts=allowed, resolver=resolver
    )
    if (
        is_production
        and destination.scheme != "https"
        and not _is_allow_listed((destination.host, *destination.addresses), destination.port, allowed)
    ):
        raise UnsafeDestinationError(
            "HTTPS_REQUIRED", "https is required in production unless the host is allow-listed"
        )
    return destination


def revalidate_destination(
    destination: ValidatedDestination,
    *,
    policy: Optional[OutboundPolicy] = None,
    production: Optional[bool] = None,
    allowed_hosts: Union[str, Iterable[str], frozenset, None] = None,
    resolver: Optional[Resolver] = None,
) -> Tuple[str, ...]:
    """Resolve an already validated destination again and re-apply the address policy.

    Call it immediately before each request an HTTP client makes to
    ``destination``: a name that answered with a public address at validation
    time and with an internal one now (DNS rebinding) is refused here. Returns
    the addresses the name resolves to now. Raises :class:`UnsafeDestinationError`.
    """
    is_production, allowed = _unpack_policy(policy, production, allowed_hosts)
    return _resolve_and_check(
        destination.host, destination.port, production=is_production, allowed_hosts=allowed, resolver=resolver
    )
