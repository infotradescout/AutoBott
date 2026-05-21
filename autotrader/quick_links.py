"""Dashboard quick-link helper for AutoBott operator URLs."""

from __future__ import annotations

from flask import request, render_template_string

QUICK_LINKS = [
    ("Dashboard", "/"),
    ("Truth", "/api/truth"),
    ("Decision Journal", "/api/decision-journal"),
    ("Outcomes 15m", "/api/decision-outcomes?horizon=15"),
    ("Outcomes 30m", "/api/decision-outcomes?horizon=30"),
    ("Outcomes 60m", "/api/decision-outcomes?horizon=60"),
    ("Learning", "/decision-learning"),
    ("Health", "/healthz"),
]

STYLE = """
<style data-autobott-quicklinks-style>
.autobott-quicklinks{position:sticky;top:0;z-index:9999;background:rgba(4,9,19,.96);backdrop-filter:blur(10px);border-bottom:1px solid #27384f;padding:10px 18px;display:flex;gap:8px;align-items:center;flex-wrap:wrap;font:13px/1.2 system-ui,Segoe UI,Arial,sans-serif}.autobott-quicklinks-title{color:#8ea1b8;font-weight:900;text-transform:uppercase;letter-spacing:.08em;margin-right:4px}.autobott-quicklinks a{border:1px solid #27384f;background:#13243a;color:#eef5ff;border-radius:999px;padding:8px 11px;text-decoration:none;font-weight:800}.autobott-quicklinks a:hover{background:#1b3352}.autobott-quicklinks .hot{background:#ffd166;color:#1a1400;border-color:#ffd166}
</style>
"""

BAR = STYLE + """
<div class="autobott-quicklinks" data-autobott-quicklinks>
  <span class="autobott-quicklinks-title">AutoBott URLs</span>
  <a class="hot" href="/">Dashboard</a>
  <a href="/api/truth" target="_blank" rel="noopener">Truth</a>
  <a href="/api/decision-journal" target="_blank" rel="noopener">Journal</a>
  <a href="/api/decision-outcomes?horizon=15" target="_blank" rel="noopener">Outcomes 15m</a>
  <a href="/api/decision-outcomes?horizon=30" target="_blank" rel="noopener">30m</a>
  <a href="/api/decision-outcomes?horizon=60" target="_blank" rel="noopener">60m</a>
  <a href="/decision-learning" target="_blank" rel="noopener">Learning</a>
  <a href="/healthz" target="_blank" rel="noopener">Health</a>
  <a href="/quick-links" target="_blank" rel="noopener">All Links</a>
</div>
"""


def register_quick_links(app):
    @app.after_request
    def inject_quick_links(response):
        try:
            if request.path == "/" and str(response.content_type or "").startswith("text/html"):
                html = response.get_data(as_text=True)
                if "data-autobott-quicklinks" not in html:
                    html = html.replace("<body>", "<body>" + BAR, 1)
                    response.set_data(html)
                    response.headers["Content-Length"] = str(len(response.get_data()))
        except Exception as exc:  # noqa: BLE001
            print(f"[quicklinks] injection failed: {exc}")
        return response

    @app.get("/quick-links")
    def quick_links_page():
        return render_template_string(
            """
<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>AutoBott Quick Links</title>{{ style|safe }}</head><body style="margin:0;background:#07111f;color:#eef5ff;font:14px system-ui,Segoe UI,Arial,sans-serif"><div style="max-width:980px;margin:0 auto;padding:24px"><h1 style="margin-top:0">AutoBott Quick Links</h1><p style="color:#8ea1b8">Keep this page pinned. These links are operator tools, not strategy changes.</p><div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px">{% for label, url in links %}<a href="{{ url }}" target="_blank" rel="noopener" style="display:block;border:1px solid #27384f;background:#13243a;color:#eef5ff;border-radius:16px;padding:16px;text-decoration:none;font-weight:900">{{ label }}<div style="color:#8ea1b8;font-size:12px;margin-top:5px">{{ url }}</div></a>{% endfor %}</div></div></body></html>
            """,
            links=QUICK_LINKS,
            style=STYLE,
        )
