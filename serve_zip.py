import http.server, os, pathlib

FILE = pathlib.Path("/home/runner/workspace/attached_assets/migrate-fixed.zip")

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/download"):
            data = FILE.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Disposition", 'attachment; filename="migrate-fixed.zip"')
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            self.send_response(404)
            self.end_headers()
    def log_message(self, *a): pass

http.server.HTTPServer(("0.0.0.0", 5000), Handler).serve_forever()
