#!/usr/bin/env python3
"""
The MIA's Paper — periódico local de noticias de IA que lee feeds RSS/Atom en vivo.

Uso:
    python3 radar.py            # abre http://localhost:8765 en tu navegador
    python3 radar.py --port 9000 --no-browser

Solo usa la biblioteca estándar de Python (3.9+). Para agregar o quitar
fuentes, edita la lista FEEDS de abajo.

Cómo se ordena la portada: las notas que hablan de lo mismo se agrupan en
una "historia". Una historia que cubren más medios va más arriba; a igual
cobertura, gana la más reciente.
"""
import argparse
import html
import json
import math
import re
import ssl
import threading
import time
import urllib.request
import webbrowser
import xml.etree.ElementTree as ET
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
# ai_only=True: el feed es de tecnología general, así que solo se conservan
# las notas que mencionan IA.
GN = "https://news.google.com/rss/search?hl=en-US&gl=US&ceid=US:en&q="
FEEDS = [
    {"name": "TechCrunch",      "url": "https://techcrunch.com/category/artificial-intelligence/feed/", "site": "https://techcrunch.com/category/artificial-intelligence/"},
    {"name": "The Verge",       "url": "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml", "site": "https://www.theverge.com/ai-artificial-intelligence"},
    {"name": "MIT Tech Review", "url": "https://www.technologyreview.com/topic/artificial-intelligence/feed", "site": "https://www.technologyreview.com/topic/artificial-intelligence/"},
    {"name": "NYT",             "url": "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml", "site": "https://www.nytimes.com/section/technology", "ai_only": True, "pay": True},
    {"name": "Financial Times", "url": "https://www.ft.com/artificial-intelligence?format=rss", "site": "https://www.ft.com/artificial-intelligence", "pay": True},
    {"name": "Reuters",         "url": GN + "artificial+intelligence+site:reuters.com+when:3d", "site": "https://www.reuters.com/technology/artificial-intelligence/", "via": "Google News"},
    {"name": "Bloomberg",       "url": GN + "AI+site:bloomberg.com+when:3d", "site": "https://www.bloomberg.com/technology", "via": "Google News", "pay": True},
    {"name": "The Decoder",     "url": "https://the-decoder.com/feed/", "site": "https://the-decoder.com/"},
    {"name": "Wired",           "url": "https://www.wired.com/feed/tag/ai/latest/rss", "site": "https://www.wired.com/tag/artificial-intelligence/"},
    {"name": "Xataka",          "url": "https://www.xataka.com/feedburner.xml", "site": "https://www.xataka.com/categoria/inteligencia-artificial", "ai_only": True, "lang": "es"},
]

CACHE_SECONDS = 10 * 60      # cada cuánto se vuelven a leer los feeds
MAX_PER_FEED = 25            # notas máximas por fuente
TIMEOUT = 12                 # segundos por feed
CLUSTER_THRESHOLD = 0.20     # más bajo = agrupa más notas en una misma historia

AI_WORDS = re.compile(
    r"\b(ai|a\.i\.|artificial intelligence|inteligencia artificial|ia|llm|chatgpt|openai|anthropic|claude|gemini|"
    r"deepmind|copilot|machine learning|aprendizaje autom[aá]tico|neural|generative|generativa|nvidia|agent(e|s)?|"
    r"mistral|llama|grok|xai|perplexity|chatbot)\b", re.I)

TOPICS = [  # (clave, palabras); gana la primera que coincida
    ("seg", r"secur|cyber|hack|vulnerab|exploit|malware|safety|seguridad|ciber|deepfake|scam|fraud|breach|spy|espionage"),
    ("reg", r"regulat|law\b|lawsuit|sued|court|judge|congress|senat|\beu\b|european|ai act|policy|ban\b|copyright|antitrust|"
            r"government|white house|probe|investigat|scandal|affair|ley|regulaci|demanda|tribunal|gobierno|pol[ií]tica|derechos de autor"),
    ("neg", r"\bipo\b|i\.p\.o|funding|raises|valuation|revenue|earnings|stock|shares|investor|acqui|billion|trillion|deal|startup|"
            r"layoff|loss|inversi|valuaci|ingresos|acciones|adquie|millones|despid|bolsa"),
    ("inf", r"data cent|datacent|chip|gpu|tpu|compute|semiconductor|energy|power grid|electric|turbine|"
            r"centro de datos|energ[ií]a|infraestructura"),
    ("mod", r"model|gpt|gemini|claude|llama|launch|releas|open.?weight|benchmark|feature|app\b|assistant|agent|robot|"
            r"modelo|lanza|presenta|asistente|agente"),
]
TOPICS = [(k, re.compile(p, re.I)) for k, p in TOPICS]

# ---------------------------------------------------------------------------
# Lectura y parseo de feeds
# ---------------------------------------------------------------------------
def _ssl_context():
    try:
        import certifi  # opcional; ayuda en macOS si Python no trae certificados
        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return ssl.create_default_context()

SSL_CTX = _ssl_context()
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) RadarIA/2.0 (+local RSS reader)"


def _local(tag):
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _child(el, *names):
    for c in el:
        if _local(c.tag) in names:
            return c
    return None


def _text(el, *names):
    c = _child(el, *names)
    return (c.text or "").strip() if c is not None and c.text else ""


def _clean(s, limit=280):
    s = re.sub(r"<[^>]+>", " ", s or "")
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s if len(s) <= limit else s[:limit].rsplit(" ", 1)[0] + "…"


def _date(s):
    if not s:
        return None
    try:
        d = parsedate_to_datetime(s)
    except Exception:
        try:
            d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)


def topic_of(text):
    for key, rx in TOPICS:
        if rx.search(text):
            return key
    return "otr"


def parse_feed(xml_bytes, feed):
    root = ET.fromstring(xml_bytes)
    items = []
    if _local(root.tag) == "feed":  # Atom
        entries = [e for e in root if _local(e.tag) == "entry"]
    else:                           # RSS 2.0 / RDF
        channel = _child(root, "channel")
        entries = [e for e in (channel if channel is not None else root) if _local(e.tag) == "item"]
        if not entries:
            entries = [e for e in root if _local(e.tag) == "item"]
    for e in entries:
        title = _clean(_text(e, "title"), 300)
        link = _text(e, "link")
        if not link:
            l = _child(e, "link")
            if l is not None:
                link = l.get("href", "")
        summary = _clean(_text(e, "description", "summary", "content", "encoded"))
        date = _date(_text(e, "pubDate", "published", "updated", "date"))
        if feed.get("via") == "Google News":   # Google News pone " - Medio" al final
            title = re.sub(r"\s+-\s+[^-]+$", "", title)
            summary = ""
        if not title or not link:
            continue
        if feed.get("ai_only") and not AI_WORDS.search(title + " " + summary):
            continue
        items.append({
            "title": title,
            "link": link,
            "summary": summary if summary.lower() != title.lower() else "",
            "date": date.isoformat() if date else None,
            "source": feed["name"],
            "pay": bool(feed.get("pay")),
            "topic": topic_of(title + " " + summary),
        })
        if len(items) >= MAX_PER_FEED:
            break
    return items


def fetch_feed(feed):
    started = time.time()
    try:
        req = urllib.request.Request(feed["url"], headers={"User-Agent": UA, "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*"})
        with urllib.request.urlopen(req, timeout=TIMEOUT, context=SSL_CTX) as r:
            data = r.read()
        items = parse_feed(data, feed)
        return items, {"name": feed["name"], "ok": True, "count": len(items), "ms": int((time.time() - started) * 1000)}
    except Exception as ex:
        msg = str(ex)
        if "CERTIFICATE_VERIFY_FAILED" in msg:
            msg = "Certificados SSL faltantes (ver README)"
        return [], {"name": feed["name"], "ok": False, "error": msg[:160]}

# ---------------------------------------------------------------------------
# Agrupar notas que cuentan la misma historia
# ---------------------------------------------------------------------------
STOP = set("""a an the and or of to in on for with from by at as is are was were be been has have had will would could should can may
its it this that these those their his her our your who what why how when where than then into over after before about up out new
says said say ahead just more most much make makes making gets get takes take now year years day days week amid via
ai a.i artificial intelligence la el los las de del en y que para por con una un se su al lo es""".split())
ALIAS = {"i.p.o": "ipo", "tn": "trillion", "billion": "bn", "chips": "chip", "centre": "center", "centres": "center", "centers": "center"}


def _tokens(title):
    t = title.lower().replace("’", "'")
    t = re.sub(r"\$?(\d+(?:\.\d+)?)(tn|bn|m)\b", r"\1 \2", t)
    out = set()
    for w in re.findall(r"[a-záéíóúñ0-9][a-záéíóúñ0-9.\-']*", t):
        w = w.strip(".'-")
        if w.endswith("'s"):
            w = w[:-2]
        w = ALIAS.get(w, w)
        if len(w) < 2 or w in STOP:
            continue
        if w.endswith("s") and len(w) > 4 and not w.endswith("ss"):
            w = w[:-1]
        out.add(w)
    return out


def build_stories(items):
    """Agrupamiento jerárquico (enlace promedio) sobre la similitud de títulos,
    ponderada por lo poco comunes que son las palabras compartidas."""
    n = len(items)
    if not n:
        return []
    toks = [_tokens(i["title"]) for i in items]
    df = Counter(t for ts in toks for t in ts)
    idf = {t: math.log((n + 1) / (c + 0.5)) for t, c in df.items()}
    w = [sum(idf[t] for t in ts) or 1.0 for ts in toks]

    def sim(a, b):
        sh = toks[a] & toks[b]
        if len(sh) < 2:
            return 0.0
        return sum(idf[t] for t in sh) / math.sqrt(w[a] * w[b])

    S = [[0.0] * n for _ in range(n)]
    for a in range(n):
        for b in range(a + 1, n):
            S[a][b] = S[b][a] = sim(a, b)

    members = {i: [i] for i in range(n)}
    pair_sum = {i: {j: S[i][j] for j in range(n) if j != i and S[i][j] > 0} for i in range(n)}
    while True:
        best, pair = CLUSTER_THRESHOLD, None
        for x, row in pair_sum.items():
            for y, s in row.items():
                if x < y:
                    avg = s / (len(members[x]) * len(members[y]))
                    if avg > best:
                        best, pair = avg, (x, y)
        if not pair:
            break
        x, y = pair
        members[x] += members.pop(y)
        row_y = pair_sum.pop(y)
        row_y.pop(x, None)
        pair_sum[x].pop(y, None)
        for z, s in row_y.items():
            pair_sum[x][z] = pair_sum[x].get(z, 0.0) + s
            pair_sum[z].pop(y, None)
            pair_sum[z][x] = pair_sum[x][z]

    now = datetime.now(timezone.utc)
    stories = []
    for idxs in members.values():
        group = [items[i] for i in idxs]
        # Nota representativa: la más parecida al resto; a igualdad, con resumen y más reciente.
        def central(i):
            return (sum(S[i][j] for j in idxs if j != i), bool(items[i]["summary"]), items[i]["date"] or "")
        rep = items[max(idxs, key=central)]
        summary = rep["summary"] or next((g["summary"] for g in group if g["summary"]), "")
        sources = []
        for g in sorted(group, key=lambda g: g["date"] or "", reverse=True):
            if g["source"] not in [s["source"] for s in sources]:
                sources.append({"source": g["source"], "title": g["title"], "link": g["link"], "pay": g["pay"]})
        latest = max((g["date"] for g in group if g["date"]), default=None)
        hours = (now - datetime.fromisoformat(latest)).total_seconds() / 3600 if latest else 72
        n_src = len(sources)
        score = n_src * 10 + min(len(group), 8) * 2 + max(0.0, 36 - hours) / 4
        topic = Counter(g["topic"] for g in group).most_common(1)[0][0]
        stories.append({
            "id": re.sub(r"\W+", "", rep["link"])[-24:] or str(len(stories)),
            "title": rep["title"],
            "link": rep["link"],
            "summary": summary,
            "source": rep["source"],
            "pay": rep["pay"],
            "date": latest,
            "topic": topic,
            "n_sources": n_src,
            "n_items": len(group),
            "score": round(score, 2),
            "coverage": sources,
        })
    stories.sort(key=lambda s: (s["score"], s["date"] or ""), reverse=True)
    return stories

# ---------------------------------------------------------------------------
# Caché
# ---------------------------------------------------------------------------
_cache = {"at": 0, "payload": None}
_lock = threading.Lock()


def get_news(force=False):
    with _lock:
        if not force and _cache["payload"] and time.time() - _cache["at"] < CACHE_SECONDS:
            return _cache["payload"]
        with ThreadPoolExecutor(max_workers=len(FEEDS)) as pool:
            results = list(pool.map(fetch_feed, FEEDS))
        items, status, seen = [], [], set()
        for its, st in results:
            status.append(st)
            for it in its:
                key = re.sub(r"\W+", "", it["title"].lower())[:80]
                if key in seen:
                    continue
                seen.add(key)
                items.append(it)
        items.sort(key=lambda x: x["date"] or "", reverse=True)
        payload = {
            "updated": datetime.now(timezone.utc).isoformat(),
            "n_items": len(items),
            "stories": build_stories(items),
            "status": status,
            "sources": [{k: f.get(k) for k in ("name", "site", "pay", "via", "lang")} for f in FEEDS],
        }
        _cache.update(at=time.time(), payload=payload)
        return payload

# ---------------------------------------------------------------------------
# Página
# ---------------------------------------------------------------------------
PAGE = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>The MIA's Paper</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Gloock&family=IM+Fell+Double+Pica:ital@0;1&family=IM+Fell+English+SC&family=Instrument+Serif:ital@0;1&family=Abril+Fatface&family=Alfa+Slab+One&family=Special+Elite&family=Old+Standard+TT:ital,wght@0,400;0,700;1,400&family=Playfair+Display+SC:wght@700;900&family=Bodoni+Moda:ital,opsz,wght@0,6..96,800;1,6..96,800&family=Libre+Franklin:ital,wght@0,400;0,700;0,800;1,900&family=Oswald:wght@300;500&family=Bebas+Neue&family=Archivo:wdth,wght@62..125,300..900&display=swap" rel="stylesheet">
<style>
/* Plan: hoja de periódico envejecido (bordes quemados, manchas, grano y orilla rasgada) sobre fondo verde oscuro.
   Cabecera estilo "The Telegraph"; "Última hora" enmarcada con punteado; secciones con letras recortadas;
   titulares en familias distintas (serif tintada, Clarendon, didona, gótica, máquina de escribir). */
:root{
  --table:#26332d; --paper:#ebe0c8; --paper-2:#e2d4b6; --ink:#2b211c; --muted:#6a5a4c; --chip:#d8c59e;
  --mast:"Gloock", "Playfair Display", Georgia, serif;
  --fell:"IM Fell Double Pica", "Old Standard TT", Georgia, serif;
  --body:"Old Standard TT", Georgia, "Times New Roman", serif;
  --grot:"Libre Franklin", "Franklin Gothic Medium", Arial, sans-serif;
  color-scheme:light;
}
*{box-sizing:border-box}
html,body{margin:0}
body{
  background-color:var(--table);
  background-image:
    radial-gradient(ellipse at 50% 0%, rgba(255,255,255,.06), transparent 60%),
    url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='200' height='200'><filter id='n'><feTurbulence type='fractalNoise' baseFrequency='.9' numOctaves='2' stitchTiles='stitch'/><feColorMatrix values='0 0 0 0 0  0 0 0 0 0  0 0 0 0 0  0 0 0 .25 0'/></filter><rect width='100%' height='100%' filter='url(%23n)'/></svg>");
  color:var(--ink);font-family:var(--body);font-size:16px;line-height:1.5;padding:40px 18px 60px;
}
a{color:inherit;text-decoration:none}
a:hover{text-decoration:underline;text-decoration-thickness:1px;text-underline-offset:3px}
a:focus-visible,button:focus-visible,input:focus-visible,select:focus-visible{outline:2px solid var(--ink);outline-offset:3px}

/* ---------- Hoja envejecida ---------- */
.stage{max-width:1200px;margin:0 auto;filter:drop-shadow(0 22px 34px rgba(0,0,0,.55)) drop-shadow(0 2px 3px rgba(0,0,0,.4))}
.sheet{position:relative;padding:54px 56px 48px;background-color:var(--paper);
  background-image:
    radial-gradient(ellipse 75% 70% at 50% 45%, transparent 52%, rgba(120,82,40,.22) 82%, rgba(70,42,18,.55) 100%),
    radial-gradient(circle at 9% 14%, rgba(150,105,55,.20), transparent 16%),
    radial-gradient(circle at 93% 22%, rgba(130,90,45,.14), transparent 14%),
    radial-gradient(circle at 78% 82%, rgba(150,105,55,.18), transparent 20%),
    radial-gradient(circle at 18% 88%, rgba(110,75,35,.20), transparent 18%),
    radial-gradient(ellipse at 40% 55%, rgba(255,250,235,.35), transparent 45%),
    url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='260' height='260'><filter id='g'><feTurbulence type='fractalNoise' baseFrequency='.75' numOctaves='3' stitchTiles='stitch'/><feColorMatrix values='0 0 0 0 .32  0 0 0 0 .22  0 0 0 0 .12  0 0 0 .16 0'/></filter><rect width='100%' height='100%' filter='url(%23g)'/></svg>"),
    url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='600' height='600'><filter id='m'><feTurbulence type='fractalNoise' baseFrequency='.012' numOctaves='3' seed='7'/><feColorMatrix values='0 0 0 0 .45  0 0 0 0 .32  0 0 0 0 .18  0 0 0 .22 -.04'/></filter><rect width='100%' height='100%' filter='url(%23m)'/></svg>");
}
.sheet::before{content:"";position:absolute;inset:20px;border:2px solid var(--ink);pointer-events:none;opacity:.85}
.sheet::after{content:"";position:absolute;inset:26px;border:1px solid var(--ink);pointer-events:none;opacity:.6}
@media (max-width:700px){.sheet{padding:40px 26px 34px}.sheet::before{inset:12px}.sheet::after{inset:17px}}

/* ---------- Cabecera ---------- */
.proudly{display:flex;justify-content:space-between;align-items:center;gap:10px;font:800 11px var(--grot);text-transform:uppercase;letter-spacing:.22em;padding-bottom:10px}
.proudly .orn{font-family:var(--fell);font-size:22px;letter-spacing:0;line-height:0}
.mast{text-align:center;margin:4px 0 6px;font-family:var(--mast);font-weight:400;line-height:.82;color:var(--ink)}
.mast .the{display:block;font-size:clamp(28px,4.4vw,56px);letter-spacing:.32em;padding-left:.32em;margin-bottom:.12em}
.mast .name{display:block;font-size:clamp(66px,12.5vw,168px);letter-spacing:-.015em;white-space:nowrap}
.mast .name i{font-family:"Instrument Serif", serif;font-style:italic;font-weight:400;letter-spacing:0}
.subline{display:grid;grid-template-columns:1fr auto 1fr;align-items:center;gap:18px;margin:10px 0 0}
.subline .bar{border-top:3px solid var(--ink);border-bottom:1px solid var(--ink);height:6px}
.subline span{font:italic 400 17px var(--fell)}

.dateline{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px 18px;
  font:800 12px var(--grot);text-transform:uppercase;letter-spacing:.16em;padding:12px 0;border-bottom:1.5px solid var(--ink)}
.dateline .star{font-family:var(--fell);font-size:20px;line-height:0}
@media (max-width:700px){.dateline .star{display:none}.dateline{justify-content:center}}

/* Última hora, estilo "Introducing / The Telegraph" */
.breaking{display:grid;grid-template-columns:28px 1fr 28px;margin:18px 0 6px;border-top:2px dotted var(--ink);border-bottom:2px dotted var(--ink)}
.breaking .side{writing-mode:vertical-rl;transform:rotate(180deg);font:italic 400 12px var(--fell);text-align:center;border-left:2px dotted var(--ink);padding:0 4px;white-space:nowrap;overflow:hidden}
.breaking .side.r{transform:none;border-left:0;border-right:2px dotted var(--ink)}
.breaking .core{text-align:center;padding:12px 10px 16px}
.breaking .intro{font:400 13px var(--grot);text-transform:uppercase;letter-spacing:.6em;padding-left:.6em}
.breaking h2{font-family:var(--mast);font-weight:400;font-size:clamp(46px,7.2vw,92px);line-height:.95;margin:6px 0 0;letter-spacing:-.01em}
.breaking h2 em{font-family:"Instrument Serif", serif;font-weight:400}

/* ---------- Controles ---------- */
.controls{display:flex;flex-wrap:wrap;align-items:center;gap:8px 18px;padding:12px 0 14px;border-bottom:3px double var(--ink)}
.tabs{display:flex;flex-wrap:wrap;gap:4px 16px}
.tab{font:700 12px var(--grot);text-transform:uppercase;letter-spacing:.14em;background:none;border:0;color:var(--muted);padding:4px 0;cursor:pointer;border-bottom:2px solid transparent}
.tab:hover{color:var(--ink)}
.tab[aria-pressed="true"]{color:var(--ink);border-bottom-color:var(--ink)}
.spacer{flex:1 1 auto}
.field{font:italic 400 16px var(--fell);color:var(--ink);background:transparent;border:0;border-bottom:1.5px solid var(--ink);padding:4px 2px;border-radius:0}
select.field{font-style:normal;font-family:var(--grot);font-weight:700;font-size:12px;text-transform:uppercase;letter-spacing:.08em;cursor:pointer}
#q{width:230px;max-width:100%}
.btn{font:800 11px var(--grot);text-transform:uppercase;letter-spacing:.16em;background:var(--ink);color:var(--paper);border:0;padding:8px 12px;cursor:pointer}
.btn[disabled]{opacity:.55;cursor:wait}

/* ---------- Portada ---------- */
.front{display:grid;grid-template-columns:minmax(0,5fr) minmax(0,7fr);gap:0 34px;padding:24px 0 26px}
@media (max-width:900px){.front{grid-template-columns:1fr;gap:28px}}
.kicker{font:800 11px var(--grot);text-transform:uppercase;letter-spacing:.16em;display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:10px}
.kicker .count{background:var(--ink);color:var(--paper);font:italic 400 14px var(--fell);letter-spacing:.02em;text-transform:none;padding:2px 9px 1px}
.lead h2{font-family:var(--fell);font-weight:400;font-size:clamp(34px,3.6vw,50px);line-height:1.02;margin:0 0 16px;text-wrap:balance;letter-spacing:-.005em}
.txt{text-align:justify;hyphens:auto;-webkit-hyphens:auto;margin:0 0 16px;font-size:16px;line-height:1.5}
.lead .txt::first-letter{font-family:"Alfa Slab One", serif;font-size:3.7em;float:left;line-height:.84;padding:5px 8px 0 0}
.cover{margin:0 0 20px;padding:0;list-style:none;border-top:1.5px solid var(--ink)}
.cover li{display:grid;grid-template-columns:116px 1fr;gap:10px;padding:7px 0;border-bottom:1px dotted var(--ink);font-size:14.5px;line-height:1.35}
.cover b{font:800 10.5px var(--grot);text-transform:uppercase;letter-spacing:.12em;padding-top:3px}
.quote{margin:0;border:2px solid var(--ink);outline:1px solid var(--ink);outline-offset:-6px;padding:18px 22px;text-align:center;font:400 clamp(18px,1.8vw,22px)/1.25 "Playfair Display SC", serif}
.quote cite{display:block;margin-top:8px;font:italic 400 14px var(--fell)}

/* Cartel tipográfico en lugar de foto */
.plate{border:2px solid var(--ink);outline:1px solid var(--ink);outline-offset:5px;margin:6px 6px 22px;background:rgba(214,196,160,.45);
  padding:clamp(18px,3vw,36px);display:flex;flex-direction:column;justify-content:center;min-height:330px;text-align:center}
.plate .kicker{justify-content:center}
.plate h2{font-family:"Bodoni Moda", serif;font-weight:800;font-size:clamp(36px,4.8vw,66px);line-height:.98;margin:6px 0 14px;text-wrap:balance;letter-spacing:-.01em}
.plate h2 .alt{display:block;font-family:"Instrument Serif", serif;font-style:italic;font-weight:400;font-size:.62em;letter-spacing:0;margin-top:.15em}
.plate .src{font:400 12px var(--grot);text-transform:uppercase;letter-spacing:.24em}
.meta{font:700 10.5px var(--grot);text-transform:uppercase;letter-spacing:.12em;color:var(--muted);display:flex;flex-wrap:wrap;gap:4px 12px}
.meta .pay{color:var(--ink)}

/* ---------- Bandas con letras recortadas ---------- */
.band{display:flex;align-items:center;gap:16px;margin:10px 0 0}
.band .lines{flex:1;border-top:3px solid var(--ink);border-bottom:1px solid var(--ink);height:7px}
.ransom{display:inline-flex;flex-wrap:nowrap;align-items:center;gap:3px;margin:0;font-weight:400;line-height:1}
.ransom span{display:inline-block;background:var(--chip);color:var(--ink);padding:.12em .16em .08em;box-shadow:1px 1px 0 rgba(43,33,28,.25)}
.ransom .gap{background:none;box-shadow:none;width:.35em;padding:0}

/* ---------- Rejilla ---------- */
.grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));margin:14px 0 26px;border-top:2px solid var(--ink)}
.story{padding:18px 22px 20px;border-right:1px solid var(--ink);border-bottom:1px solid var(--ink)}
.story:nth-child(3n){border-right:0;padding-right:0}
.story:nth-child(3n+1){padding-left:0}
@media (max-width:900px){.grid{grid-template-columns:1fr 1fr}.story:nth-child(n){padding:16px 16px 18px;border-right:1px solid var(--ink)}.story:nth-child(2n){border-right:0}}
@media (max-width:560px){.grid{grid-template-columns:1fr}.story:nth-child(n){padding:16px 0;border-right:0}}
.story h4{margin:0 0 10px;line-height:1.06;text-wrap:balance;overflow-wrap:break-word}
.story p{font-size:14px;line-height:1.45;margin:0 0 10px;text-align:justify;hyphens:auto;-webkit-hyphens:auto}

/* ---------- Breves ---------- */
.briefs{columns:3;column-gap:34px;column-rule:1px solid var(--ink);margin:14px 0 26px}
@media (max-width:900px){.briefs{columns:2}}
@media (max-width:560px){.briefs{columns:1}}
.brief{break-inside:avoid;padding:0 0 10px;margin-bottom:10px;border-bottom:1px dotted var(--ink);font-size:15px;line-height:1.32}
.brief .when{display:block;font:italic 400 13px var(--fell);color:var(--muted);margin-bottom:2px}

.empty{font:italic 400 22px var(--fell);text-align:center;padding:40px 0}

/* ---------- Pie ---------- */
.wire{display:flex;flex-wrap:wrap;justify-content:center;gap:6px 18px;padding:14px 0;font:800 11px var(--grot);text-transform:uppercase;letter-spacing:.12em;border-top:3px double var(--ink);border-bottom:3px double var(--ink)}
.wire a.bad{text-decoration:line-through;color:var(--muted)}
.wire .n{font-weight:400;color:var(--muted)}
.foot{display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px 20px;padding:12px 0 0;font:italic 400 14px var(--fell);color:var(--muted)}
</style>
</head>
<body>
<div class="stage"><div class="sheet" id="sheet">
  <div class="proudly"><span>Orgullosamente presenta</span><span class="orn" aria-hidden="true">❦</span><span id="vol">Vol. I · No. —</span></div>
  <h1 class="mast"><span class="the">THE</span><span class="name">MIA's <i>Paper</i></span></h1>
  <div class="subline"><div class="bar"></div><span>Noticias de inteligencia artificial, recién salidas de la imprenta</span><div class="bar"></div></div>
  <div class="dateline">
    <span id="dateLong">—</span><span class="star" aria-hidden="true">✦</span>
    <span id="edition">Edición local</span><span class="star" aria-hidden="true">✦</span>
    <span id="counts">— medios</span>
  </div>

  <section class="breaking" aria-label="Última hora">
    <div class="side" id="sideL">Edición del día</div>
    <div class="core"><div class="intro">Presentamos</div><h2>Última <em>hora</em></h2></div>
    <div class="side r">Directo de los cables</div>
  </section>

  <div class="controls">
    <div class="tabs" role="group" aria-label="Secciones" id="tabs">
      <button class="tab" data-f="all" aria-pressed="true" id="f-all">Portada</button>
      <button class="tab" data-f="mod" aria-pressed="false" id="f-mod">Modelos</button>
      <button class="tab" data-f="neg" aria-pressed="false" id="f-neg">Negocios</button>
      <button class="tab" data-f="inf" aria-pressed="false" id="f-inf">Infraestructura</button>
      <button class="tab" data-f="reg" aria-pressed="false" id="f-reg">Regulación</button>
      <button class="tab" data-f="seg" aria-pressed="false" id="f-seg">Seguridad</button>
    </div>
    <span class="spacer"></span>
    <select class="field" id="src" aria-label="Filtrar por medio"><option value="">Todos los medios</option></select>
    <input class="field" id="q" type="search" placeholder="Buscar en la edición…" aria-label="Buscar">
    <button class="btn" id="refresh" type="button">Actualizar</button>
  </div>

  <div id="paper"><p class="empty">Imprimiendo la edición…</p></div>

  <div class="wire" id="wire"></div>
  <div class="foot">
    <span>La portada se ordena por cuántos medios cubren cada historia; a igual cobertura, gana la más reciente.</span>
    <span id="totals"></span>
  </div>
</div></div>

<script>
/* Familias de titulares (rotan en este orden en "También hoy") */
const FACES = [
  {ff:'"IM Fell Double Pica", serif',   w:400, up:false, it:false, k:1.10},   // tipo antiguo con tinta corrida
  {ff:'"Libre Franklin", sans-serif',   w:900, up:false, it:true,  k:1.00},   // gótica extra-bold itálica
  {ff:'"Gloock", serif',                w:400, up:false, it:false, k:1.02},   // estilo The Telegraph
  {ff:'"Alfa Slab One", serif',         w:400, up:false, it:false, k:0.86},   // Clarendon de diario
  {ff:'"Oswald", sans-serif',           w:500, up:true,  it:false, k:1.00},   // condensada
  {ff:'"Instrument Serif", serif',      w:400, up:false, it:true,  k:1.25},   // serif condensada itálica
  {ff:'"Special Elite", monospace',     w:400, up:false, it:false, k:0.86},   // máquina de escribir
  {ff:'"Bodoni Moda", serif',           w:800, up:false, it:true,  k:0.98},   // didona itálica
  {ff:'"Bebas Neue", sans-serif',       w:400, up:true,  it:false, k:1.30},
  {ff:'"Playfair Display SC", serif',   w:700, up:false, it:false, k:0.92},   // versalitas
  {ff:'"Abril Fatface", serif',         w:400, up:false, it:false, k:1.00},
  {ff:'"Archivo", sans-serif',          w:800, up:true,  it:false, k:1.00, st:62},  // ultra condensada
];
/* Letras recortadas para las bandas */
const RANSOM = [
  ['"Abril Fatface", serif', 400, false], ['"Bebas Neue", sans-serif', 400, false], ['"Special Elite", monospace', 400, false],
  ['"Gloock", serif', 400, false], ['"Instrument Serif", serif', 400, true], ['"Alfa Slab One", serif', 400, false],
  ['"Oswald", sans-serif', 500, false], ['"IM Fell Double Pica", serif', 400, true], ['"Libre Franklin", sans-serif', 900, false],
  ['"Bodoni Moda", serif', 800, true]
];
const CHIPS = ["#d8c59e", "#e4d6b8", "#cdb88c", "#efe4cc", "#d2bd93"];

const LABEL = {mod:"Modelos", neg:"Negocios", inf:"Infraestructura", reg:"Regulación", seg:"Seguridad", otr:"General"};
let DATA = null, topic = "all";
const $ = id => document.getElementById(id);
const esc = s => String(s ?? "").replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const rtf = new Intl.RelativeTimeFormat("es-MX", {numeric:"auto"});
function ago(iso){
  if (!iso) return "";
  const m = (Date.parse(iso) - Date.now()) / 60000;
  if (m > -60) return rtf.format(Math.round(m), "minute");
  if (m > -60*24) return rtf.format(Math.round(m/60), "hour");
  return new Date(iso).toLocaleDateString("es-MX", {day:"numeric", month:"short"});
}
function rng(seed){ let s = 7; for (const c of String(seed)) s = (s*31 + c.charCodeAt(0)) >>> 0; return () => { s = (s*1664525 + 1013904223) >>> 0; return s/4294967296; }; }
const medios = n => n === 1 ? "1 medio" : `${n} medios`;
const A = (s, txt) => `<a href="${esc(s.link)}" target="_blank" rel="noopener">${esc(txt ?? s.title)}</a>`;
const clip = (t, n) => t.length > n ? t.slice(0, n).replace(/\s\S*$/, "") + "…" : t;

function ransom(text, size){
  const r = rng(text);
  return `<h3 class="ransom" aria-label="${esc(text)}" style="font-size:${size}px">${[...text].map((ch, i) => {
    if (ch === " ") return '<span class="gap" aria-hidden="true"></span>';
    const [ff, w, it] = RANSOM[Math.floor(r() * RANSOM.length)], up = r() < .6;
    const rot = (r() * 8 - 4).toFixed(1), sz = (0.82 + r() * 0.36).toFixed(2), bg = CHIPS[Math.floor(r() * CHIPS.length)];
    return `<span aria-hidden="true" style="font-family:${ff};font-weight:${w};font-style:${it ? "italic" : "normal"};font-size:${sz}em;transform:rotate(${rot}deg);background:${bg}">${esc(up ? ch.toUpperCase() : ch.toLowerCase())}</span>`;
  }).join("")}</h3>`;
}
function kicker(s, extra){
  return `<div class="kicker">${extra ? `<span>${extra}</span>` : ""}<span class="count">${s.n_sources > 1 ? "En " + medios(s.n_sources) : "Solo en " + esc(s.source)}</span><span>${LABEL[s.topic]}</span></div>`;
}
function meta(s){
  return `<div class="meta"><span>${esc(s.source)}${s.n_sources > 1 ? ` + ${s.n_sources - 1}` : ""}</span><span>${esc(ago(s.date))}</span>${s.pay ? '<span class="pay">Suscripción</span>' : ""}</div>`;
}
function plateTitle(s){
  const words = s.title.split(/\s+/), cut = Math.min(words.length, Math.max(2, Math.ceil(words.length * 0.5)));
  const a = words.slice(0, cut).join(" "), b = words.slice(cut).join(" ");
  return `<a href="${esc(s.link)}" target="_blank" rel="noopener">${esc(a)}${b ? `<span class="alt">${esc(b)}</span>` : ""}</a>`;
}
function headStyle(face, size){
  return `font-family:${face.ff};font-weight:${face.w};font-style:${face.it ? "italic" : "normal"};text-transform:${face.up ? "uppercase" : "none"};font-stretch:${face.st || 100}%;font-size:${(size * face.k).toFixed(1)}px`;
}

function filtered(){
  const q = $("q").value.trim().toLowerCase(), src = $("src").value;
  return DATA.stories.filter(s =>
    (topic === "all" || s.topic === topic) &&
    (!src || s.coverage.some(c => c.source === src)) &&
    (!q || (s.title + " " + s.summary + " " + s.coverage.map(c => c.title).join(" ")).toLowerCase().includes(q)));
}

function render(){
  if (!DATA) return;
  const st = filtered();
  if (!st.length){ $("paper").innerHTML = '<p class="empty">No hay noticias en esta sección. Prueba otro filtro.</p>'; tear(); return; }
  const [lead, second, ...rest] = st, grid = rest.slice(0, 12), briefs = rest.slice(12);
  const alt = lead.coverage.find(c => c.title !== lead.title);
  let h = `<section class="front">
    <article class="lead">
      ${kicker(lead, "Principal")}
      <h2>${A(lead)}</h2>
      ${lead.summary ? `<p class="txt">${esc(lead.summary)}</p>` : ""}
      ${lead.coverage.length > 1 ? `<ul class="cover">${lead.coverage.map(c => `<li><b>${esc(c.source)}</b>${A(c)}</li>`).join("")}</ul>` : meta(lead)}
      ${alt ? `<blockquote class="quote">“${A(alt)}”<cite>— ${esc(alt.source)}</cite></blockquote>` : ""}
    </article>`;
  if (second){
    h += `<article class="second">
      <div class="plate">${kicker(second)}<h2>${plateTitle(second)}</h2><div class="src">${esc(second.coverage.map(c => c.source).join(" · "))}</div></div>
      ${second.summary ? `<p class="txt">${esc(second.summary)}</p>` : ""}
      ${meta(second)}
    </article>`;
  }
  h += `</section>`;
  if (grid.length){
    const top = grid[0].score, bottom = grid[grid.length - 1].score || 1;
    h += `<div class="band">${ransom("También hoy", 30)}<div class="lines"></div></div><section class="grid">${grid.map((s, i) => {
      const t = top === bottom ? 1 : (s.score - bottom) / (top - bottom);
      return `<article class="story">${kicker(s)}<h4 style='${headStyle(FACES[i % FACES.length], 22 + 14 * t)}'>${A(s)}</h4>${s.summary ? `<p>${esc(clip(s.summary, 190))}</p>` : ""}${meta(s)}</article>`;
    }).join("")}</section>`;
  }
  if (briefs.length){
    h += `<div class="band">${ransom("Breves", 30)}<div class="lines"></div></div><section class="briefs">${briefs.map(s => `
      <div class="brief"><span class="when">${esc(s.source)}${s.n_sources > 1 ? ` + ${s.n_sources - 1}` : ""} · ${esc(ago(s.date))}</span>${A(s)}</div>`).join("")}</section>`;
  }
  $("paper").innerHTML = h;
  tear();
}

/* Orilla rasgada: polígono con pequeñas mordidas irregulares en los cuatro lados */
function tear(){
  const el = $("sheet"), r = rng("mia"), pts = [], step = 1.6, j = () => (r() * 5).toFixed(1);
  for (let x = 0; x <= 100; x += step) pts.push(`${x.toFixed(1)}% ${j()}px`);
  for (let y = 0; y <= 100; y += step * .6) pts.push(`calc(100% - ${j()}px) ${y.toFixed(1)}%`);
  for (let x = 100; x >= 0; x -= step) pts.push(`${x.toFixed(1)}% calc(100% - ${j()}px)`);
  for (let y = 100; y >= 0; y -= step * .6) pts.push(`${j()}px ${y.toFixed(1)}%`);
  el.style.clipPath = `polygon(${pts.join(",")})`;
}

function renderChrome(){
  const st = Object.fromEntries(DATA.status.map(x => [x.name, x]));
  $("wire").innerHTML = "<span>Corresponsales:</span>" + DATA.sources.map(f => {
    const x = st[f.name] || {};
    return `<a href="${esc(f.site)}" target="_blank" rel="noopener" class="${x.ok ? "" : "bad"}" title="${esc(x.ok ? (f.via ? "vía " + f.via : "") : x.error)}">${esc(f.name)} <span class="n">${x.ok ? x.count : "sin señal"}</span></a>`;
  }).join("");
  const sel = $("src"), cur = sel.value;
  sel.innerHTML = '<option value="">Todos los medios</option>' + DATA.sources.map(f => `<option${f.name === cur ? " selected" : ""}>${esc(f.name)}</option>`).join("");
  const up = new Date(DATA.updated);
  const doy = Math.floor((up - new Date(up.getFullYear(), 0, 0)) / 864e5);
  const ok = DATA.status.filter(x => x.ok).length;
  $("vol").textContent = `Vol. I · No. ${doy}`;
  $("dateLong").textContent = up.toLocaleDateString("es-MX", {weekday:"long", day:"numeric", month:"long", year:"numeric"});
  $("edition").textContent = `Edición de las ${up.toLocaleTimeString("es-MX", {hour:"2-digit", minute:"2-digit"})}`;
  $("counts").textContent = `${ok} medios · ${DATA.stories.length} historias`;
  $("sideL").textContent = up.toLocaleDateString("es-MX", {day:"numeric", month:"long"});
  const multi = DATA.stories.filter(s => s.n_sources > 1).length;
  $("totals").textContent = `${DATA.n_items} notas · ${DATA.stories.length} historias · ${multi} en varios medios`;
}

async function load(force){
  if (window.__DATA__){                       // versión publicada (GitHub Pages)
    if (force) { location.reload(); return; }
    DATA = window.__DATA__; renderChrome(); render(); return;
  }
  $("refresh").disabled = true;
  if (force) $("edition").textContent = "Imprimiendo…";
  try{
    const r = await fetch("/api/news" + (force ? "?refresh=1" : ""));
    if (!r.ok) throw new Error("HTTP " + r.status);
    DATA = await r.json();
    renderChrome(); render();
  }catch(e){
    $("edition").textContent = "Sin conexión con la imprenta";
    $("paper").innerHTML = `<p class="empty">No pude leer las noticias (${esc(e.message)}). Revisa que la Terminal con radar.py siga abierta.</p>`;
    tear();
  }finally{ $("refresh").disabled = false; }
}
$("tabs").addEventListener("click", e => {
  const b = e.target.closest(".tab"); if (!b) return;
  topic = b.dataset.f;
  document.querySelectorAll("#tabs .tab").forEach(x => x.setAttribute("aria-pressed", x === b ? "true" : "false"));
  render();
});
$("src").addEventListener("change", render);
$("q").addEventListener("input", render);
$("refresh").addEventListener("click", () => load(true));
setInterval(() => window.__DATA__ ? location.reload() : load(false), 15 * 60 * 1000);
tear();
load(false);
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype):
        data = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path in ("/", "/index.html"):
            self._send(200, PAGE, "text/html; charset=utf-8")
        elif u.path == "/api/news":
            force = parse_qs(u.query).get("refresh", ["0"])[0] == "1"
            self._send(200, json.dumps(get_news(force), ensure_ascii=False), "application/json; charset=utf-8")
        else:
            self._send(404, "No encontrado", "text/plain; charset=utf-8")

    def log_message(self, fmt, *args):
        pass  # terminal limpia


def build_static(path):
    """Escribe una sola página HTML con las noticias ya incrustadas (para GitHub Pages o el iPad)."""
    import os
    data = json.dumps(get_news(force=True), ensure_ascii=False).replace("</", "<\\/")
    page = PAGE.replace("<script>", f"<script>window.__DATA__ = {data};</script>\n<script>", 1)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(page)
    ok = sum(1 for s in json.loads(data.replace("<\\/", "</"))["status"] if s["ok"])
    print(f"Página estática escrita en {path} ({ok}/{len(FEEDS)} medios respondieron)")


def main():
    ap = argparse.ArgumentParser(description="The MIA's Paper (local)")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--no-browser", action="store_true", help="no abrir el navegador automáticamente")
    ap.add_argument("--static", metavar="ARCHIVO", help="en vez de servidor, genera una página HTML con las noticias incrustadas")
    args = ap.parse_args()
    if args.static:
        build_static(args.static)
        return

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    url = f"http://localhost:{args.port}"
    print(f"The MIA's Paper corriendo en {url}  (Ctrl+C para detener)")
    threading.Thread(target=get_news, daemon=True).start()  # precarga los feeds
    if not args.no_browser:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nDetenido.")


if __name__ == "__main__":
    main()
