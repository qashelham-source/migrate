import http.server, pathlib, base64

FILE = pathlib.Path("/home/runner/workspace/attached_assets/migrate-fixed.zip")

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/dl":
            data = FILE.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Disposition", 'attachment; filename="migrate-fixed.zip"')
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(data)
        else:
            data = FILE.read_bytes()
            b64 = base64.b64encode(data).decode()
            html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Download</title>
<style>body{{font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#1a1a2e}}
.box{{background:#fff;padding:40px;border-radius:16px;text-align:center}}
a{{display:inline-block;background:#0070f3;color:#fff;padding:16px 32px;border-radius:8px;text-decoration:none;font-size:18px;font-weight:bold}}</style>
</head><body>
<div class="box">
  <p style="font-size:14px;color:#666">migrate-fixed.zip &nbsp;·&nbsp; {len(data)//1024} KB</p>
  <a href="data:application/zip;base64,{b64}" download="migrate-fixed.zip">⬇ Download ZIP</a>
</div>
</body></html>"""
            encoded = html.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
    def log_message(self, *a): pass

http.server.HTTPServer(("0.0.0.0", 5000), Handler).serve_forever()
