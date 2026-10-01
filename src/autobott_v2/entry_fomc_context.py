"""Observed, listed FOMC events from the Board's monthly calendar.

No inferred meeting times, emergency-event guarantee, or historical-as-of claim.
"""
from datetime import UTC, date, datetime, time
from html.parser import HTMLParser
import re
import urllib.request

from .entry_schedule_context import MAX_BYTES, NY, _NoRedirect, digest

MONTHS = ("", "january", "february", "march", "april", "may", "june", "july",
          "august", "september", "october", "november", "december")
TITLES = ("FOMC Press Conference", "FOMC Meeting", "FOMC Minutes")
SCOPE = "listed_fomc_meetings_press_conferences_minutes_only"


def fomc_url(day: date) -> str:
    return f"https://www.federalreserve.gov/newsevents/{day.year}-{MONTHS[day.month]}.htm"


def fetch_fomc_text(day: date) -> str:
    request = urllib.request.Request(fomc_url(day), headers={
        "User-Agent": "AutoBott calendar reader", "Accept": "text/html"})
    with urllib.request.build_opener(_NoRedirect).open(request, timeout=5) as response:
        raw = response.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError("schedule_fomc_source_size_limit")
    return raw.decode("utf-8-sig")


class _Calendar(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.root = {"attrs": {}, "parts": []}
        self.stack = [self.root]
        self.nodes = []

    def handle_starttag(self, tag, attrs):
        if tag != "div":
            return
        if len(self.stack) >= 64 or len(self.nodes) >= 3000:
            raise ValueError("schedule_fomc_html_bounds")
        node = {"attrs": dict(attrs), "parts": []}
        self.stack[-1]["parts"].append(node)
        self.stack.append(node)
        self.nodes.append(node)

    def handle_endtag(self, tag):
        if tag == "div":
            if len(self.stack) == 1:
                raise ValueError("schedule_fomc_html_structure")
            self.stack.pop()

    def handle_data(self, data):
        self.stack[-1]["parts"].append(data)


def _text(node):
    return " ".join(" ".join(_text(p) if isinstance(p, dict) else p
                             for p in node["parts"]).split())


def _nodes(node):
    yield node
    for part in node["parts"]:
        if isinstance(part, dict):
            yield from _nodes(part)


def _class(node, name):
    return name in node["attrs"].get("class", "").split()


def parse_fomc_calendar(text: str, day: date) -> dict:
    if not isinstance(text, str) or len(text.encode()) > MAX_BYTES:
        raise ValueError("schedule_fomc_source_size_limit")
    parser = _Calendar()
    parser.feed(text)
    parser.close()
    articles = [n for n in parser.nodes if n["attrs"].get("id") == "article"]
    if len(parser.stack) != 1 or len(articles) != 1:
        raise ValueError("schedule_fomc_calendar_structure")
    nodes = list(_nodes(articles[0]))
    headings = [_text(n) for n in nodes if _class(n, "row-title")]
    expected = f"{MONTHS[day.month].title()} {day.year}"
    if headings != [expected]:
        raise ValueError("schedule_fomc_month_mismatch")
    panels = [n for n in nodes if _class(n, "panel-body")]
    if not panels:
        raise ValueError("schedule_fomc_calendar_empty")
    # A heading and arbitrary panel/error message are not a calendar. Require
    # at least one recognizable published row even when this month lists no
    # FOMC events. Some non-FOMC panels group multiple rows under one time.
    recognizable = False
    for panel in panels:
        parts = list(_nodes(panel))
        clocks = [_text(n) for n in parts if _class(n, "col-xs-2")]
        labels = [_text(n) for n in parts if _class(n, "col-xs-7")]
        dates = [_text(n) for n in parts if _class(n, "col-xs-3")]
        if (any(re.fullmatch(r"(?:[1-9]|1[0-2]):[0-5]\d (?:a|p)\.m\.", t) for t in clocks)
                and any(labels) and any(re.fullmatch(r"\d{1,2}(?:,\s*\d{1,2})*", d) for d in dates)):
            recognizable = True
    if not recognizable:
        raise ValueError("schedule_fomc_calendar_rows_unrecognized")
    events = {}
    for panel in panels:
        if "FOMC" not in _text(panel):
            continue
        cells = [[n for n in _nodes(panel) if _class(n, f"col-xs-{size}")]
                 for size in (2, 7, 3)]
        if any(len(c) != 1 for c in cells):
            raise ValueError("schedule_fomc_event_structure")
        stamp, label, days = (_text(c[0]) for c in cells)
        titles = [t for t in TITLES if label == t or label.startswith(t + " ")]
        match = re.fullmatch(r"(\d{1,2}):(\d{2}) (a|p)\.m\.", stamp)
        if len(titles) != 1 or not match or not re.fullmatch(r"\d{1,2}(?:,\s*\d{1,2})*", days):
            raise ValueError("schedule_fomc_event_time_or_title_unknown")
        hour, minute = int(match[1]), int(match[2])
        if not 1 <= hour <= 12 or not 0 <= minute < 60:
            raise ValueError("schedule_fomc_event_time_invalid")
        hour = hour % 12 + (12 if match[3] == "p" else 0)
        for value in days.split(","):
            local = datetime.combine(date(day.year, day.month, int(value)), time(hour, minute), NY)
            if local.replace(fold=0).utcoffset() != local.replace(fold=1).utcoffset():
                raise ValueError("schedule_fomc_event_dst_ambiguous")
            event = {"id": f"fed:{local.date().isoformat()}:{titles[0]}", "title": titles[0],
                     "at": local.astimezone(UTC).isoformat(), "status": "CONFIRMED"}
            if event["id"] in events and events[event["id"]] != event:
                raise ValueError("schedule_fomc_conflicting_event")
            events[event["id"]] = event
    rows = sorted(events.values(), key=lambda row: (row["at"], row["id"]))
    if len(rows) > 100:
        raise ValueError("schedule_fomc_event_count_limit")
    return {"source_url": fomc_url(day), "scope": SCOPE, "month": day.strftime("%Y-%m"),
            "events": rows, "dataset_hash": digest(rows), "source_hash": digest(text),
            "historical_asof_verified": False}
