"""CI failure drill only: a "release" that starts, answers HTTP on :8000 as the trading role, and is never ready.

Built on top of the real bot-backend image by unready-release.Dockerfile, so its migration job and schema range are
the real ones; only the served process differs. Every GET answers 503 with a body that says the runtime is still
starting -- what a release whose trading runtime never comes up looks like to the release gate. SIGTERM ends it at
once (as PID 1 it would otherwise ignore the signal and wait out the stop grace).
"""
import http.server
import json
import signal
import sys

BODY = json.dumps({"status": "ok", "process_role": "trading",
                   "runtime": {"state": "STARTING", "faults": ["DRILL_RELEASE_NEVER_READY"]}}).encode()


class NeverReady(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 -- http.server's naming
        self.send_response(503)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(BODY)))
        self.end_headers()
        self.wfile.write(BODY)

    def log_message(self, *args):
        pass


signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
http.server.ThreadingHTTPServer(("0.0.0.0", 8000), NeverReady).serve_forever()
