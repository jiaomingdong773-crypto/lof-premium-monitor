"""
本地预览服务器：一键运行 python3 server.py 即可在 http://localhost:8000 打开 LOF 实时溢价看板
"""

from __future__ import annotations

import json
import os
import urllib.parse
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from api.lof import compute_all_lof_data

BASE_DIR = Path(__file__).resolve().parent
PUBLIC_DIR = BASE_DIR / "public"


class LocalAppHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(PUBLIC_DIR), **kwargs)

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/lof":
            qs = urllib.parse.parse_qs(parsed.query)
            force = qs.get("refresh", ["0"])[0] in ("1", "true", "yes")
            try:
                data = compute_all_lof_data(force_refresh=force)
                body = json.dumps(data, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)
            except Exception as e:
                err = json.dumps({"error": str(e)}, ensure_ascii=False).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(err)
            return

        if parsed.path == "/":
            self.path = "/index.html"
        super().do_GET()

    def log_message(self, format: str, *args) -> None:
        print(f"[server] {self.address_string()} - {format % args}")


def main() -> None:
    port = int(os.environ.get("PORT", "8000"))
    server = ThreadingHTTPServer(("127.0.0.1", port), LocalAppHandler)
    print(f"LOF 实时溢价监控看板已启动: http://localhost:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n服务已停止。")
        server.server_close()


if __name__ == "__main__":
    main()
