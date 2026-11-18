#!/usr/bin/env python3
"""pxboard — дашборд pixel.mk69.dev: поле, дельты, кланы, авторы, возраст, поток.

  python pxboard.py                 # http://127.0.0.1:8808/
  python pxboard.py --token <T>     # + официальные цифры кланов из /api/top
  python pxboard.py --db px.db      # авторский слой

Сервер тянет /live/canvas.bin, /api/state, опрашивает /live/head.bin раз в секунду,
читает px.db. Всё остальное — статистика — считается в браузере.
"""

import argparse
import base64
import collections
import io
import json
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# локальный dns иногда отваливается: если имя не разрешилось, берём известный адрес
PIN = {"pixel.mk69.dev": "109.120.137.32"}
_gai = socket.getaddrinfo


def _getaddrinfo(host, port, *a, **kw):
    try:
        return _gai(host, port, *a, **kw)
    except socket.gaierror:
        ip = PIN.get(host)
        if ip:
            return _gai(ip, port, *a, **kw)
        raise


socket.getaddrinfo = _getaddrinfo

TARGET = "https://pixel.mk69.dev"
UA = "pixelboard"
UA_FALLBACK = ("Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) "
               "Chrome/120.0.0.0 Mobile Safari/537.36")
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
PBKDF2_ITER = 240000
PASS_FAILS = collections.deque(maxlen=20)
CACHE_TTL = 2.0
_lock = threading.Lock()
_cache = {"t": 0.0, "payload": None}
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "px.db")
TOKEN = ""

# ---------------------------------------------------------------- внешние данные


def fetch(path, timeout=20):
    req = urllib.request.Request(TARGET + path, headers={
        "User-Agent": "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        "Accept": "application/json",
        "Origin": TARGET,
        "Referer": TARGET + "/",
    })
    if TOKEN:
        req.add_header("Authorization", "Bearer " + TOKEN)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def get_snapshot():
    with _lock:
        if _cache["payload"] and time.time() - _cache["t"] < CACHE_TTL:
            return _cache["payload"]
    raw = fetch("/live/canvas.bin")
    if len(raw) < 24 or raw[:4] != b"PXC1":
        raise ValueError("bad snapshot")
    seg = int.from_bytes(raw[4:8], "little")
    ver = int.from_bytes(raw[8:12], "little")
    w = int.from_bytes(raw[12:14], "little")
    h = int.from_bytes(raw[14:16], "little")
    seq = int.from_bytes(raw[16:24], "little")
    colors = raw[24:24 + w * h]
    painted = sum(1 for b in colors if b)
    try:
        state = json.loads(fetch("/api/state", timeout=12))
    except Exception:
        state = {}
    payload = {
        "w": w, "h": h, "seg": seg, "ver": ver, "seq": seq,
        "palette": state.get("palette") or [],
        "online": state.get("online"), "phase": state.get("phase"),
        "charges": state.get("max_charges"), "cooldown": state.get("cooldown_ms"),        "serverNow": state.get("now"), "bot": state.get("bot") or "",
        "serverStart": state.get("starts_at"), "serverEnd": state.get("ends_at"),
        "colors": base64.b64encode(colors).decode("ascii"),
        "painted": painted,
        "fetchedAt": int(time.time() * 1000),
    }
    with _lock:
        _cache.update(t=time.time(), payload=payload)
    return payload


# ---------------------------------------------------------------- поток событий

EVENTS = collections.deque(maxlen=4000)
FEED = {"last_seq": 0, "seg": 0, "polled": 0, "err": ""}

TOKEN_META = {}
TOK = {"state": "none", "code": None, "checked": 0, "me": None, "err": ""}
TOK_LOCK = threading.Lock()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def load_config():
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_config(cfg):
    tmp = CONFIG_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=1)
    os.replace(tmp, CONFIG_PATH)
    try:
        os.chmod(CONFIG_PATH, 0o600)
    except Exception:
        pass


def hash_pass(phrase, salt_hex, iterations=PBKDF2_ITER):
    import hashlib
    salt = bytes.fromhex(salt_hex)
    return hashlib.pbkdf2_hmac("sha256", phrase.encode("utf-8"), salt, iterations).hex()


def pass_configured():
    cfg = load_config()
    return bool(cfg.get("pass_hash") and cfg.get("pass_salt"))


def pass_ok(candidate):
    """сравнение с хешем из конфига; сама фраза никуда не пишется."""
    import hmac
    if not candidate:
        return False
    cfg = load_config()
    if not (cfg.get("pass_hash") and cfg.get("pass_salt")):
        return False
    calc = hash_pass(candidate, cfg["pass_salt"], cfg.get("iterations", PBKDF2_ITER))
    return hmac.compare_digest(calc, cfg["pass_hash"])


def throttled():
    now = time.time()
    while PASS_FAILS and now - PASS_FAILS[0] > 300:
        PASS_FAILS.popleft()
    return len(PASS_FAILS) >= 5


def token_path():
    return os.path.join(BASE_DIR, "token.txt")


def read_token_file():
    try:
        with open(token_path(), encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""


def read_meta_file():
    try:
        with open(os.path.join(BASE_DIR, "token.json"), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_token(tok, expires=None, checking=True):
    """записать токен и подхватить его без перезапуска сервера."""
    global TOKEN, TOKEN_META
    with TOK_LOCK:
        TOKEN = tok
        TOKEN_META = {"token": tok, "expires_at": expires, "updated": int(time.time() * 1000)}
    with open(token_path(), "w", encoding="utf-8") as f:
        f.write(tok)
    with open(os.path.join(BASE_DIR, "token.json"), "w", encoding="utf-8") as f:
        json.dump(TOKEN_META, f)
    if checking:
        TOK.update(state="checking", code=None, me=None, err="")


def exchange_initdata(init):
    """обмен init_data на токен. сервер отбивает не-браузерный user-agent
    кодом 403 client, поэтому на такой ответ идёт один повтор с браузерным ua."""
    data = json.dumps({"init_data": init}).encode("utf-8")
    last = ("", None, "unknown")
    for ua in (UA, UA_FALLBACK):
        req = urllib.request.Request(TARGET + "/api/auth", data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")
        req.add_header("User-Agent", ua)
        req.add_header("Origin", TARGET)
        req.add_header("Referer", TARGET + "/")
        try:
            with urllib.request.urlopen(req, timeout=25) as r:
                j = json.loads(r.read())
            return j.get("token", ""), j.get("expires_at"), None
        except urllib.error.HTTPError as e:
            try:
                err = json.loads(e.read()).get("err", "http_%d" % e.code)
            except Exception:
                err = "http_%d" % e.code
            last = ("", None, err)
            if err != "client":
                return last
        except Exception as e:
            return "", None, type(e).__name__
    return last


def check_token():
    if not TOKEN:
        TOK.update(state="none", code=None, checked=int(time.time() * 1000), me=None, err="")
        return
    try:
        raw = fetch("/api/me", timeout=15)
        TOK.update(state="ok", code=200, checked=int(time.time() * 1000),
                   me=json.loads(raw), err="")
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", "replace")
        except Exception:
            pass
        if e.code == 403 and "banned" in body:
            state = "banned"
        elif e.code == 401:
            state = "dead"
        else:
            state = "error"
        TOK.update(state=state, code=e.code, checked=int(time.time() * 1000), me=None, err=body[:120])
    except Exception as e:
        TOK.update(state="net", code=None, checked=int(time.time() * 1000),
                   me=TOK.get("me"), err=type(e).__name__)


def token_loop():
    """следит за token.txt и раз в минуту проверяет /api/me."""
    global TOKEN, TOKEN_META
    mtime = 0.0
    last_check = 0.0
    while True:
        try:
            st = os.stat(token_path())
            if st.st_mtime != mtime:
                mtime = st.st_mtime
                from_file = read_token_file()
                if from_file and from_file != TOKEN:
                    with TOK_LOCK:
                        TOKEN = from_file
                        TOKEN_META = read_meta_file()
                    TOK.update(state="checking", code=None, me=None, err="")
                    last_check = 0.0
        except FileNotFoundError:
            mtime = 0.0

        if time.time() - last_check >= 60 or TOK.get("state") == "checking":
            check_token()
            last_check = time.time()
        time.sleep(5)


def parse_segment(b):
    if not b or len(b) < 40 or b[:4] != b"PXS1":
        return None
    n = int.from_bytes(b[4:8], "little")
    first = int.from_bytes(b[16:24], "little")
    last = int.from_bytes(b[24:32], "little")
    flags = b[32]
    count = int.from_bytes(b[36:40], "little")
    if len(b) != 40 + count * 5:
        return None
    ents = [(int.from_bytes(b[40 + i * 5:42 + i * 5], "little"),
             int.from_bytes(b[42 + i * 5:44 + i * 5], "little"),
             b[44 + i * 5]) for i in range(count)]
    return {"n": n, "first": first, "last": last, "flags": flags,
            "count": count, "entries": ents}


def feed_loop():
    floor = None
    while True:
        try:
            seg = parse_segment(fetch("/live/head.bin", timeout=12))
            if seg:
                if floor is None:
                    floor = seg["last"]
                if seg["count"]:
                    for i, (x, y, c) in enumerate(seg["entries"]):
                        seq = seg["first"] + i
                        if seq > floor:
                            EVENTS.append({"seq": seq, "sec": seg["n"], "x": x, "y": y, "c": c})
                    floor = max(floor, seg["last"])
                FEED.update(seg=seg["n"], last_seq=seg["last"], polled=int(time.time() * 1000), err="")
        except Exception as e:
            FEED["err"] = str(e)[:120]
        time.sleep(1.0)


# ---------------------------------------------------------------- кланы и топ


def clan_stats():
    """кланы берутся только из /api/top, локальной базы больше нет."""
    data = top_data()
    if not data.get("ok"):
        return {"ok": False, "err": data.get("err") or "no_token", "clans": [], "token": bool(TOKEN)}
    clans = []
    for c in data.get("clans", []):
        if not isinstance(c, dict):
            continue
        clans.append({"tag": c.get("tag"), "cells": c.get("cells"), "pixels": c.get("pixels"),
                      "members": c.get("members"), "rank": c.get("rank"), "name": c.get("name")})
    return {"ok": True, "clans": clans, "token": bool(TOKEN),
            "updated_at": data.get("updated_at")}


_CACHE = {"top": (0.0, None), "clans": (0.0, None)}


def cached_fetch(kind, path, ttl):
    """общий кеш для /api/top и /api/clans, чтобы не дёргать сервер лишний раз."""
    ts, data = _CACHE.get(kind, (0.0, None))
    if data is not None and time.time() - ts < ttl:
        return data
    if TOK.get("state") != "ok":
        return data if data is not None else {"ok": False, "err": TOK.get("state") or "no_token"}
    try:
        payload = json.loads(fetch(path, timeout=20))
        _CACHE[kind] = (time.time(), payload)
        return payload
    except urllib.error.HTTPError as e:
        return {"ok": False, "err": "http_%d" % e.code}
    except Exception as e:
        return {"ok": False, "err": type(e).__name__}


def top_data():
    d = cached_fetch("top", "/api/top", 30)
    if isinstance(d, dict) and "players" in d:
        return {"ok": True, "players": d.get("players", []), "clans": d.get("clans", []),
                "me": d.get("me"), "my_clan": d.get("my_clan"), "updated_at": d.get("updated_at")}
    return {"ok": False, "err": (d or {}).get("err", "unknown") if isinstance(d, dict) else "unknown"}


def clan_list():
    d = cached_fetch("clans", "/api/clans", 60)
    if isinstance(d, dict) and isinstance(d.get("clans"), list):
        return {"ok": True, "clans": d["clans"]}
    return {"ok": False, "err": (d or {}).get("err", "unknown") if isinstance(d, dict) else "unknown"}


# ---------------------------------------------------------------- HTML

HTML = r"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<title>pxboard — pixel.mk69.dev</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
:root{--bg:#111;--panel:#1a1a1a;--line:#2a2a2a;--fg:#e9e9e9;--dim:#8a8a8a;--acc:#5FB8FF;--warn:#FF7A1A;--bad:#F0353B;--ok:#86E04C}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:12.5px/1.45 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
header{display:flex;align-items:center;gap:12px;padding:9px 14px;border-bottom:1px solid var(--line);background:#181818;position:sticky;top:0;z-index:5}
h1{font-size:12px;font-weight:600;margin:0;letter-spacing:.06em}
h1 .by{font-size:9.5px;font-weight:400;color:var(--dim);letter-spacing:.02em;margin-left:6px}
.badge{padding:2px 7px;border:1px solid var(--line);border-radius:4px;color:var(--dim);white-space:nowrap}
.badge.ok{color:var(--ok);border-color:#2E6B1E}
.badge.bad{color:var(--bad);border-color:#5C0F1F}
.badge.wait{color:var(--warn);border-color:#6b4226}
main{display:grid;grid-template-columns:minmax(340px,1fr) 430px;gap:12px;padding:12px;align-items:start}
@media(max-width:1060px){main{grid-template-columns:1fr}}
.stage{position:relative;background:#0b0b0b;border:1px solid var(--line);border-radius:6px;overflow:hidden;touch-action:none}
canvas#field{display:block;width:100%;height:auto;image-rendering:pixelated;cursor:crosshair}
canvas#overlay{position:absolute;inset:0;width:100%;height:100%;image-rendering:pixelated;pointer-events:none;opacity:.88}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:6px;padding:9px 11px;margin-bottom:10px}
.panel h2{font-size:10px;letter-spacing:.09em;text-transform:uppercase;color:var(--dim);margin:0 0 7px;display:flex;justify-content:space-between}
.kv{display:flex;justify-content:space-between;gap:8px;padding:1.5px 0}
.kv span:first-child{color:var(--dim)}
.bar{height:5px;background:#000;border-radius:3px;overflow:hidden;margin-top:4px}
.bar i{display:block;height:100%;background:linear-gradient(90deg,#2B74E0,#5FB8FF)}
.row{display:flex;gap:6px;flex-wrap:wrap}
button{background:#232323;color:var(--fg);border:1px solid var(--line);border-radius:4px;padding:3px 8px;font:inherit;cursor:pointer}
button.on{border-color:var(--acc);color:var(--acc)}
button:hover{border-color:#4a4a4a}
table{width:100%;border-collapse:collapse;font-size:11.5px}
th{text-align:left;color:var(--dim);font-weight:400;font-size:9.5px;letter-spacing:.06em;text-transform:uppercase;padding:0 4px 3px 0}
td{padding:2.5px 4px 2.5px 0;border-top:1px solid #232323}
td.num,th.num{text-align:right;font-variant-numeric:tabular-nums}
tr.click{cursor:pointer}tr.click:hover td{background:#202020}
.mini{height:4px;border-radius:2px;background:#2B74E0}
.legend{display:flex;flex-wrap:wrap;gap:6px;margin-top:6px;font-size:10.5px;color:var(--dim)}
.legend i{display:inline-block;width:8px;height:8px;border-radius:2px;margin-right:3px}
#spark{width:100%;height:40px;display:block}
#stack{width:100%;height:34px;display:block}
#ivals{width:100%;height:34px;display:block}
#minimap{width:132px;height:132px;image-rendering:pixelated;border:1px solid var(--line);border-radius:4px;display:block;float:right}
#cellinfo,#claninfo,#feed{color:var(--dim);font-size:11.5px}
#feed div{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#feed b{color:var(--fg)}
#toasts{position:fixed;right:14px;bottom:14px;display:flex;flex-direction:column;gap:6px;z-index:50}
.toast{background:#241a10;border:1px solid #6b4226;color:#ffd9a8;padding:6px 10px;border-radius:5px;font-size:11.5px;max-width:320px}
#err{color:var(--warn)}
.swatches{display:grid;grid-template-columns:repeat(16,1fr);gap:3px}
.sw{height:13px;border-radius:2px;border:1px solid #0006}
.q{display:inline-block;width:13px;height:13px;line-height:12px;text-align:center;border:1px solid #3a3a3a;border-radius:50%;color:#8a8a8a;font-size:9px;margin-left:5px;cursor:help;position:relative;font-style:normal;vertical-align:1px;user-select:none}
.q:hover{border-color:var(--acc);color:var(--acc)}
.src{display:inline-block;width:5px;height:5px;border-radius:50%;margin-left:6px;vertical-align:2px;cursor:help}
.src.local{background:#5FB8FF}
.src.sample{background:#FF7A1A}
.src-tag{font-size:9.5px;font-weight:400;letter-spacing:.03em;text-transform:none;margin-left:6px;white-space:nowrap}
.src-tag.local{color:#5FB8FF}
.src-tag.sample{color:#FF7A1A}
.q:hover::after{content:attr(data-tip);position:absolute;left:-8px;top:17px;width:252px;padding:7px 9px;background:#0c0c0c;border:1px solid var(--acc);border-radius:5px;color:#dcdcdc;font-size:11px;line-height:1.45;white-space:normal;text-align:left;z-index:999;box-shadow:0 8px 24px #000c;pointer-events:none}
</style></head><body>
<header>
  <h1>PIXELBOARD<span class="by">by cubzx</span></h1>
  <span class="badge" id="conn">connecting</span>
  <span class="badge" id="phase">—</span>
  <span class="badge" id="online">—</span>
  <span class="badge" id="timer">15s</span>
  <span class="badge" title="считается локально по canvas.bin, в api этого поля нет"><i class="src local"></i> локально</span>
  <span id="err"></span>
</header>
<main>
  <div>
    <div class="stage" id="stage">
      <canvas id="field" width="512" height="512"></canvas>
      <canvas id="overlay" width="512" height="512"></canvas>
    </div>
    <div class="panel" style="margin-top:10px">
      <h2>Вид</h2>
      <div class="row">
        <button id="zIn">+</button><button id="zOut">−</button><button id="zReset">сброс</button>
        <button id="expPng">PNG</button><button id="expCsv">CSV</button>
        <button id="deep">открыть клетку в Telegram</button>
      </div>
      <div style="margin-top:8px;overflow:hidden">
        <canvas id="minimap" width="64" height="64"></canvas>
        <div id="cellinfo" style="min-height:40px">клик по полю — автор, время, клан</div>
      </div>
    </div>
    <div class="panel">
      <h2>Лента постановок <span id="feedState" style="color:var(--dim)"></span></h2>
      <div id="feed"></div>
    </div>
  </div>
  <div>
    <div class="panel">
      <h2>Обновление</h2>
      <div class="row"><button id="btnAuto" class="on">авто 15s</button><button id="btnNow">сейчас</button></div>
      <div class="kv"><span>снимок</span><span id="snapAge">—</span></div>
      <div class="kv"><span>сервер</span><span id="srvTime">—</span></div>
    </div>
    <div class="panel">
      <h2>Токен<button id="tkSet" style="padding:1px 6px;font-size:10px">заменить</button></h2>
      <div class="kv"><span>состояние</span><span id="tkState">—</span></div>
      <div class="kv"><span>истекает</span><span id="tkExp">—</span></div>
      <div class="kv"><span>аккаунт</span><span id="tkAcc">—</span></div>
      <div class="kv"><span>статус</span><span id="tkStatus">—</span></div>
      <div class="kv"><span>клан и роль</span><span id="tkClan">—</span></div>
      <div class="kv"><span>заряды</span><span id="tkCharges">—</span></div>
      <div class="kv"><span>дневной лимит</span><span id="tkCap">—</span></div>
      <div class="kv"><span>поставил сам</span><span id="tkPixels">—</span></div>
      <div class="kv"><span>проверка</span><span id="tkChecked">—</span></div>
      <div class="kv"><span>след. проверка</span><span id="tkNext">—</span></div>
    </div>
    <div class="panel">
      <h2>Поле</h2>
      <div class="kv"><span>заполнено</span><span id="fill">—</span></div>
      <div class="bar"><i id="fillBar" style="width:0"></i></div>
      <div class="kv" style="margin-top:6px"><span>клетки</span><span id="cells">—</span></div>
      <div class="kv"><span>цветов</span><span id="ncolors">—</span></div>
      <div class="kv"><span>энтропия</span><span id="entropy">—</span></div>
      <div class="kv"><span>границы</span><span id="edges">—</span></div>
      <div class="kv"><span>событий</span><span id="seq">—</span></div>
      <div class="kv"><span>прирост</span><span id="rate">—</span></div>
      <div class="kv"><span>прогноз заполнения</span><span id="eta">—</span></div>
      <div class="kv"><span>z-score окна</span><span id="zscore">—</span></div>
    </div>
    <div class="panel">
      <h2>За окно</h2>
      <div class="kv"><span>новых событий</span><span id="winEv">—</span></div>
      <div class="kv"><span>изменено клеток</span><span id="winCells">—</span></div>
      <div class="kv"><span>откатов цвета</span><span id="reverts">—</span></div>
      <div class="kv"><span>турбулентность</span><span id="turb">—</span></div>
      <canvas id="spark" width="640" height="80"></canvas>
      <canvas id="stack" width="640" height="68"></canvas>
    </div>
    <div class="panel">
      <h2>Структура</h2>
      <div class="kv"><span>рисунков</span><span id="regions">—</span></div>
      <div class="kv"><span>крупнейший</span><span id="biggest">—</span></div>
      <div class="kv"><span>свободный блок</span><span id="free">—</span></div>
      <div class="kv"><span>дублей тайлов</span><span id="dups">—</span></div>
      <div class="kv"><span>кольца ц/с/к</span><span id="rings">—</span></div>
    </div>
    <div class="panel">
      <h2>Слои</h2>
      <div class="row">
        <button id="lDiff" class="on">дельты</button>
        <button id="lFill">тайлы</button>
        <button id="lEdge">границы</button>
        <button id="lHot">горячие</button>
        <button id="lGrid">сетка</button>
      </div>
    </div>
    <div class="panel">
      <h2>Кланы</h2>
      <table><thead><tr><th>клан</th><th class="num">пикселей</th><th class="num">на поле</th><th class="num">люди</th></tr></thead>
      <tbody id="clanBody"><tr><td colspan="4" style="color:var(--dim)">нужен рабочий токен</td></tr></tbody></table>
      <div id="claninfo"></div>
    </div>
    <div class="panel">
      <h2>Топ игроков</h2>
      <table><thead><tr><th>#</th><th>имя</th><th class="num">пикселей</th><th class="num">клан</th></tr></thead>
      <tbody id="topBody"><tr><td colspan="4" style="color:var(--dim)">нужен рабочий токен</td></tr></tbody></table>
    </div>
    <div class="panel">
      <h2>Интервалы постановок</h2>
      <canvas id="ivals" width="640" height="68"></canvas>
    </div>
    <div class="panel">
      <h2>Палитра</h2>
      <div class="swatches" id="pal"></div>
    </div>
  </div>
</main>
<div id="toasts"></div>
<script>
const $ = id => document.getElementById(id);
const F = $('field'), O = $('overlay'), fc = F.getContext('2d'), oc = O.getContext('2d');
const fimg = fc.createImageData(512, 512);

const S = {
  w: 512, h: 512, pal: [], colors: null, prev: null, prev2: null,
  last: null, nextAt: 0, auto: true, timer: null,
  fillHist: [], fillT: [], seqHist: [], changeHist: [],
  tileFill: new Float32Array(32 * 32), tileEdge: new Float32Array(32 * 32), tileHot: new Float32Array(32 * 32),
  revert: 0, dups: 0,
  clanMap: null, clanNames: [], authorMap: null, authorNames: [], ageMap: null, ageHist: null, ageBuckets: [],
  layer: {diff: true, fill: false, edge: false, hot: false, grid: false},
  view: {scale: 1, x: 0, y: 0},
  feed: [], feedSeen: 0, lastFeedAt: 0, prevSec: 0,
};

function rgb(hex){ const n = parseInt(hex.slice(1),16); return [n>>16&255,n>>8&255,n&255]; }
function hue(seed){ let h=0; for (let i=0;i<seed.length;i++) h=(h*31+seed.charCodeAt(i))%360; return h; }
function hsl(seed, s=68, l=55){ return `hsl(${hue(seed)} ${s}% ${l}%)`; }
function hslToRgb(h,s,l){ const f=n=>{const k=(n+h*12)%12; const a=s*Math.min(l,1-l); return l-a*Math.max(-1,Math.min(k-3,9-k,1));}; return [f(0),f(8),f(4)].map(v=>Math.round(v*255)); }

/* ---------------- подсказки ---------------- */
const TIPS = {
  "энтропия": "неопределённость по цветам: сумма −p·log2(p). чем выше, тем пестрее поле.",
  "границы": "число соседних пар клеток разного цвета. мера детализации рисунка.",
  "z-score окна": "насколько текущее окно выбивается из прошлых в сигмах. выше 3 — всплеск.",
  "турбулентность": "изменённые клетки в процентах от всего поля за окно.",
  "рисунков": "связные однотонные области от 16 клеток. каждая — отдельный рисунок.",
  "крупнейший": "самая большая однотонная область: площадь и левый верхний угол.",
  "дублей тайлов": "блоки 16×16 с одинаковым хешем цвета: копии одного изображения.",
  "кольца ц/с/к": "заполнение в центре, средней зоне и по краям поля в процентах.",
};

const TIP_KEYS = Object.keys(TIPS).sort((a,b)=>b.length-a.length);

/* какие цифры считаются локально, а какие приходят прямо из api */
const SRC_LOCAL = ["заполнено", "клетки", "цветов", "энтропия", "границы", "прирост",
  "прогноз заполнения", "z-score окна", "новых событий", "изменено клеток",
  "откатов цвета", "турбулентность", "рисунков", "крупнейший", "свободный блок",
  "дублей тайлов", "кольца ц/с/к", "лента постановок"];

function decorateSrc(){
  for (const el of document.querySelectorAll('.kv > span:first-child, .panel h2')){
    if (el.querySelector('.src') || el.querySelector('.src-tag')) continue;
    const raw = el.textContent.trim().toLowerCase();
    let kind = null;
    if (SRC_LOCAL.some(k => raw.startsWith(k))) kind = 'local';
    if (!kind) continue;

    if (el.tagName === 'H2'){
      const tag = document.createElement('span');
      tag.className = 'src-tag ' + kind;
      tag.textContent = '(локально)';
      tag.title = 'считается локально по canvas.bin, в api такой цифры нет';
      if (el.childNodes.length > 1) el.insertBefore(tag, el.childNodes[1]);
      else el.appendChild(tag);
    } else {
      const i = document.createElement('i');
      i.className = 'src ' + kind;
      i.title = 'считается локально: поле из canvas.bin, в api такой цифры нет';
      el.appendChild(i);
    }
  }
}

function decorate(){
  const nodes = document.querySelectorAll('.kv > span:first-child, th, .panel h2');
  for (const el of nodes){
    if (el.querySelector('.q')) continue;
    const raw = el.textContent.trim().toLowerCase();
    const key = TIP_KEYS.find(k => raw === k || raw.startsWith(k));
    if (!key) continue;
    const i = document.createElement('i');
    i.className = 'q';
    i.dataset.tip = TIPS[key];
    i.textContent = '?';
    if (el.tagName === 'H2' && el.childNodes.length > 1) el.insertBefore(i, el.childNodes[1]);
    else el.appendChild(i);
  }
  decorateSrc();
}

/* ---------------- рисование поля ---------------- */
function paint(colors){
  const d = fimg.data;
  for (let i=0;i<colors.length;i++){
    const v = rgb(S.pal[colors[i]] || '#000000'), j=i*4;
    d[j]=v[0]; d[j+1]=v[1]; d[j+2]=v[2]; d[j+3]=255;
  }
  fc.putImageData(fimg,0,0);
}

/* ---------------- статистика ---------------- */
function computeStats(c){
  const cnt = new Array(32).fill(0);
  let fill=0, edge=0;
  for (let i=0;i<c.length;i++){ if(c[i]) fill++; cnt[c[i]]++; }
  for (let y=0;y<S.h;y++) for (let x=0;x<S.w;x++){
    const v = c[y*S.w+x];
    if (x+1<S.w && c[y*S.w+x+1]!==v) edge++;
    if (y+1<S.h && c[(y+1)*S.w+x]!==v) edge++;
  }
  let ent=0; for (const n of cnt){ if(!n) continue; const p=n/c.length; ent-=p*Math.log2(p); }
  return {cnt, fill, edge, ent};
}

function countDiff(prev, cur){
  let n = 0;
  for (let i=0;i<cur.length;i++) if (prev[i] !== cur[i]) n++;
  return n;
}

/* медианный наклон по всем парам точек: один выброс не сдвигает прогноз */
function theilSen(ts, vs){
  const n = ts.length, slopes = [];
  for (let i=0;i<n;i++) for (let j=i+1;j<n;j++){
    const dt = ts[j]-ts[i];
    if (dt > 3000) slopes.push((vs[j]-vs[i])/dt);
  }
  if (!slopes.length) return 0;
  slopes.sort((a,b)=>a-b);
  return slopes[Math.floor(slopes.length/2)];
}

function tileStats(c){
  const T=16, N=32, fill=new Float32Array(1024), edge=new Float32Array(1024);
  for (let ty=0;ty<N;ty++) for (let tx=0;tx<N;tx++){
    let f=0,e=0;
    for (let y=ty*T;y<ty*T+T;y++) for (let x=tx*T;x<tx*T+T;x++){
      const v=c[y*S.w+x]; if(v) f++;
      if (x+1<S.w && c[y*S.w+x+1]!==v) e++;
      if (y+1<S.h && c[(y+1)*S.w+x]!==v) e++;
    }
    const k=ty*N+tx; fill[k]=f/(T*T); edge[k]=e/(2*T*T);
  }
  return {fill, edge};
}

function regions(c, limit=5){
  const W=S.w, H=S.h, seen=new Uint8Array(W*H), out=[];
  const stack=new Int32Array(W*H);
  for (let i=0;i<W*H;i++){
    if (seen[i] || !c[i]) continue;
    const col=c[i]; let sp=0; stack[sp++]=i; seen[i]=1;
    let area=0, x0=W, y0=H, x1=0, y1=0;
    while (sp){
      const p=stack[--sp]; area++;
      const x=p%W, y=(p-x)/W;
      if (x<x0)x0=x; if (y<y0)y0=y; if (x>x1)x1=x; if (y>y1)y1=y;
      if (x>0){ const q=p-1; if(!seen[q]&&c[q]===col){seen[q]=1;stack[sp++]=q;} }
      if (x<W-1){ const q=p+1; if(!seen[q]&&c[q]===col){seen[q]=1;stack[sp++]=q;} }
      if (y>0){ const q=p-W; if(!seen[q]&&c[q]===col){seen[q]=1;stack[sp++]=q;} }
      if (y<H-1){ const q=p+W; if(!seen[q]&&c[q]===col){seen[q]=1;stack[sp++]=q;} }
    }
    if (area >= 16) out.push({area, color:col, x0, y0, x1, y1});
  }
  out.sort((a,b)=>b.area-a.area);
  return {count: out.length, top: out.slice(0, limit)};
}

function freeRect(c){
  const W=S.w, H=S.h, h=new Int32Array(W);
  let best={area:0,x:0,y:0,w:0,h:0};
  for (let y=0;y<H;y++){
    for (let x=0;x<W;x++) h[x] = c[y*W+x] ? 0 : h[x]+1;
    const st=[];
    for (let x=0;x<=W;x++){
      const cur = x<W ? h[x] : 0;
      while (st.length && h[st[st.length-1]] >= cur){
        const top=st.pop();
        const height=h[top];
        const left = st.length ? st[st.length-1]+1 : 0;
        const width = x-left;
        const area = height*width;
        if (area>best.area) best={area,x:left,y:y-height+1,w:width,h:height};
      }
      st.push(x);
    }
  }
  return best;
}

function tileDupes(c){
  const T=16, N=32, seen=new Map();
  let dupes=0;
  for (let ty=0;ty<N;ty++) for (let tx=0;tx<N;tx++){
    let hsh=2166136261>>>0;
    for (let y=ty*T;y<ty*T+T;y++) for (let x=tx*T;x<tx*T+T;x++){
      hsh = (hsh ^ c[y*S.w+x]) >>> 0; hsh = (hsh * 16777619) >>> 0;
    }
    const k=hsh + ':' + ty + ':' + tx;
    if (seen.has(hsh)) dupes++; else seen.set(hsh, k);
  }
  return dupes;
}

function rings(c){
  const W=S.w, H=S.h, cx=(W-1)/2, cy=(H-1)/2, maxd=Math.hypot(cx,cy);
  const t=[0,0,0], n=[0,0,0];
  for (let y=0;y<H;y++) for (let x=0;x<W;x++){
    const d=Math.hypot(x-cx,y-cy)/maxd;
    const b = d<0.4?0:(d<0.75?1:2);
    n[b]++; if (c[y*W+x]) t[b]++;
  }
  return t.map((v,i)=> (v/n[i]*100).toFixed(1));
}

/* ---------------- оверлеи ---------------- */
function clearOverlay(){ oc.clearRect(0,0,S.w,S.h); }

function drawDiff(){
  if (!S.prev || !S.colors) return;
  const img=oc.createImageData(S.w,S.h), d=img.data;
  for (let i=0;i<S.colors.length;i++){
    if (S.prev[i]!==S.colors[i]){ const j=i*4; d[j]=255; d[j+1]=122; d[j+2]=26; d[j+3]=215; }
  }
  oc.putImageData(img,0,0);
}

function drawTilesData(kind){
  clearOverlay();
  const T=16, N=32;
  for (let ty=0;ty<N;ty++) for (let tx=0;tx<N;tx++){
    const k=ty*N+tx;
    const v = kind==='fill' ? S.tileFill[k] : kind==='edge' ? S.tileEdge[k]*1.6 : S.tileHot[k];
    if (v<=0.01) continue;
    oc.fillStyle = kind==='edge' ? `rgba(255,122,26,${Math.min(1,v)})`
                 : kind==='hot' ? `rgba(240,53,59,${Math.min(1,v)})`
                 : `rgba(95,184,255,${Math.min(1,v)})`;
    oc.fillRect(tx*T,ty*T,T,T);
  }
}

function drawGrid(){
  clearOverlay();
  oc.strokeStyle='rgba(255,255,255,.13)'; oc.lineWidth=1;
  for (let x=0;x<=S.w;x+=32){ oc.beginPath(); oc.moveTo(x+.5,0); oc.lineTo(x+.5,S.h); oc.stroke(); }
  for (let y=0;y<=S.h;y+=32){ oc.beginPath(); oc.moveTo(0,y+.5); oc.lineTo(S.w,y+.5); oc.stroke(); }
}

function drawByteMap(map, names, seedFn){
  clearOverlay();
}

function repaintLayer(){
  if (S.layer.hot) return drawTilesData('hot');
  if (S.layer.edge) return drawTilesData('edge');
  if (S.layer.fill) return drawTilesData('fill');
  if (S.layer.grid) return drawGrid();
  if (S.layer.diff) return drawDiff();
  clearOverlay();
}

/* ---------------- вид: зум и пан ---------------- */
function applyView(){
  const tr = `translate(${S.view.x}px, ${S.view.y}px) scale(${S.view.scale})`;
  F.style.transform = tr; O.style.transform = tr;
  F.style.transformOrigin = O.style.transformOrigin = '0 0';
}
function stagePoint(ev){
  const rect = F.getBoundingClientRect();
  const x = (ev.clientX - rect.left) / rect.width * S.w;
  const y = (ev.clientY - rect.top) / rect.height * S.h;
  return {x: Math.floor(x), y: Math.floor(y)};
}
$('stage').addEventListener('wheel', ev => {
  ev.preventDefault();
  const rect = F.getBoundingClientRect();
  const px = (ev.clientX - rect.left)/rect.width, py = (ev.clientY - rect.top)/rect.height;
  const before = S.view.scale;
  S.view.scale = Math.max(1, Math.min(8, S.view.scale * (ev.deltaY < 0 ? 1.25 : 0.8)));
  const k = S.view.scale/before;
  const st = $('stage');
  S.view.x = (S.view.x - px*st.clientWidth) * k + px*st.clientWidth;
  S.view.y = (S.view.y - py*st.clientHeight) * k + py*st.clientHeight;
  applyView();
}, {passive:false});
let drag = null;
$('stage').addEventListener('pointerdown', ev => { drag = {x:ev.clientX, y:ev.clientY}; });
window.addEventListener('pointerup', () => { drag = null; });
window.addEventListener('pointermove', ev => {
  if (!drag) return;
  S.view.x += ev.clientX - drag.x; S.view.y += ev.clientY - drag.y;
  drag = {x:ev.clientX, y:ev.clientY};
  applyView();
});
$('zIn').onclick = () => { S.view.scale=Math.min(8,S.view.scale*1.3); applyView(); };
$('zOut').onclick = () => { S.view.scale=Math.max(1,S.view.scale/1.3); applyView(); };
$('zReset').onclick = () => { S.view={scale:1,x:0,y:0}; applyView(); };

/* ---------------- миникарта ---------------- */
function drawMinimap(c){
  const m=$('minimap'), mc=m.getContext('2d');
  const img=mc.createImageData(64,64);
  for (let y=0;y<64;y++) for (let x=0;x<64;x++){
    const v=c[(y*8)*S.w + x*8];
    const rgbv=rgb(S.pal[v]||'#000'), j=(y*64+x)*4;
    img.data[j]=rgbv[0]; img.data[j+1]=rgbv[1]; img.data[j+2]=rgbv[2]; img.data[j+3]=255;
  }
  mc.putImageData(img,0,0);
  mc.strokeStyle='#5FB8FF'; mc.lineWidth=1;
  mc.strokeRect(.5,.5,63,63);
}
$('minimap').onclick = ev => {
  const r=$('minimap').getBoundingClientRect();
  const x=(ev.clientX-r.left)/r.width, y=(ev.clientY-r.top)/r.height;
  const st=$('stage');
  S.view.x = -x*st.clientWidth*S.view.scale + st.clientWidth/2;
  S.view.y = -y*st.clientHeight*S.view.scale + st.clientHeight/2;
  applyView();
};

/* ---------------- графики ---------------- */
function spark(){
  const s=$('spark'), c=s.getContext('2d');
  c.clearRect(0,0,s.width,s.height);
  const h=S.seqHist, max=Math.max(1,...h), bw=s.width/Math.max(h.length,1);
  h.forEach((v,i)=>{ const bh=(v/max)*(s.height-8); c.fillStyle=i===h.length-1?'#5FB8FF':'#2B74E0'; c.fillRect(i*bw+1,s.height-bh-2,Math.max(1,bw-2),bh); });
  c.fillStyle='#8a8a8a'; c.font='9px monospace'; c.fillText('max '+max,2,9);
}

function stackChart(){
  const s=$('stack'), c=s.getContext('2d');
  c.clearRect(0,0,s.width,s.height);
  if (!S.stackHist || !S.stackHist.length) return;
  const n=S.stackHist.length, bw=s.width/n;
  for (let i=0;i<n;i++){
    const row=S.stackHist[i]; let acc=0;
    const total=row.reduce((a,b)=>a+b,0)||1;
    for (let k=0;k<row.length;k++){
      if (!row[k]) continue;
      const hh=(row[k]/total)*(s.height-4);
      c.fillStyle=S.pal[k]||'#888';
      c.fillRect(i*bw, s.height-2-acc-hh, Math.max(1,bw-1), hh);
      acc+=hh;
    }
  }
}

function ivals(){
  const s=$('ivals'), c=s.getContext('2d');
  c.clearRect(0,0,s.width,s.height);
  const per=S.secCounts||{};
  const vals=Object.values(per);
  if (!vals.length) return;
  const max=Math.max(...vals,1);
  const bins=new Array(16).fill(0);
  for (const v of vals) bins[Math.min(15, v)]++;
  const m=Math.max(...bins,1), bw=s.width/16;
  bins.forEach((v,i)=>{ const hh=(v/m)*(s.height-4); c.fillStyle=i===max?'#F0353B':'#2B74E0'; c.fillRect(i*bw+1,s.height-hh-2,bw-2,hh); });
  c.fillStyle='#8a8a8a'; c.font='9px monospace';
  c.fillText('0/с',2,9); c.fillText(max+'/с', s.width-34, 9);
}

/* ---------------- панели ---------------- */
function fmtT(ms){ return new Date(ms).toLocaleTimeString('ru-RU',{hour12:false}); }

function toast(msg){
  const d=document.createElement('div'); d.className='toast'; d.textContent=msg;
  $('toasts').appendChild(d);
  setTimeout(()=>d.remove(), 9000);
}

function setText(id, v){ const el=$(id); if (el) el.textContent = v; }

async function load(){
  $('conn').textContent='fetching'; $('conn').className='badge wait';
  try{
    const j = await (await fetch('/snapshot',{cache:'no-store'})).json();
    S.w=j.w; S.h=j.h; S.pal=j.palette.length?j.palette:S.pal;
    const bin=atob(j.colors), colors=new Uint8Array(bin.length);
    for (let i=0;i<bin.length;i++) colors[i]=bin.charCodeAt(i);

    const st = computeStats(colors);
    const tl = tileStats(colors);
    const rg = regions(colors);
    const fr = freeRect(colors);
    const dups = tileDupes(colors);
    const rn = rings(colors);

    let changed = 0, revert = 0;
    if (S.colors){
      for (let i=0;i<colors.length;i++){
        if (S.colors[i]!==colors[i]) changed++;
        if (S.prev && S.colors[i]!==colors[i] && S.prev[i]===colors[i]) revert++;
      }
    }
    const evDelta = (S.last && j.seq>=S.last.seq) ? j.seq-S.last.seq : null;

    S.fillHist.push(st.fill); if (S.fillHist.length>200) S.fillHist.shift();
    S.fillT.push(j.fetchedAt || Date.now()); if (S.fillT.length>200) S.fillT.shift();

    // наклон по медиане пар точек за последние 30 минут, потом сглаживание
    if (S.fillT.length >= 6){
      const win = Math.min(S.fillT.length, 120);
      const ts = S.fillT.slice(-win), vs = S.fillHist.slice(-win);
      const slope = theilSen(ts, vs);            // клеток в миллисекунду
      S.rateRaw = slope * 60000;                 // клеток в минуту
      S.rateWin = Math.round((ts[ts.length-1] - ts[0]) / 60000);
      S.rate = (S.rate === undefined || S.rate === null)
        ? S.rateRaw
        : 0.7 * S.rate + 0.3 * S.rateRaw;
    }
    if (evDelta!==null){ S.seqHist.push(evDelta); if(S.seqHist.length>40) S.seqHist.shift(); }

    // z-score
    let z = '—';
    if (S.seqHist.length>4){
      const arr=S.seqHist.slice(0,-1), mean=arr.reduce((a,b)=>a+b,0)/arr.length;
      const sd=Math.sqrt(arr.reduce((a,b)=>a+(b-mean)**2,0)/arr.length)||1;
      z = ((S.seqHist[S.seqHist.length-1]-mean)/sd).toFixed(1);
    }

    // накопление горячих тайлов
    if (S.colors){
      for (let i=0;i<colors.length;i++) if (S.colors[i]!==colors[i]){
        const x=i%S.w, y=(i-x)/S.w;
        S.tileHot[Math.floor(y/16)*32+Math.floor(x/16)] += 0.12;
      }
    }

    paint(colors);
    S.prev2 = S.prev; S.prev = S.colors; S.colors = colors;
    S.tileFill=tl.fill; S.tileEdge=tl.edge;
    S.dups=dups; S.revert=revert;
    repaintLayer();
    drawMinimap(colors);

    setText('fill', (st.fill/(S.w*S.h)*100).toFixed(2)+'%');
    $('fillBar').style.width=(st.fill/(S.w*S.h)*100).toFixed(2)+'%';
    setText('cells', st.fill.toLocaleString('ru-RU')+' / '+(S.w*S.h).toLocaleString('ru-RU'));
    setText('ncolors', st.cnt.filter(c=>c).length+' / '+st.cnt.length);
    setText('entropy', st.ent.toFixed(2)+' бит');
    setText('edges', st.edge.toLocaleString('ru-RU'));
    setText('seq', j.seq.toLocaleString('ru-RU'));
    setText('rate', (S.rate===undefined||S.rate===null) ? '—'
      : S.rate.toFixed(1)+' кл/мин'+(S.rateWin ? ' / '+S.rateWin+' мин' : ''));
    const left=(S.w*S.h)-st.fill;
    let etaText='мало данных';
    if (S.rate !== undefined && S.rate !== null){
      if (S.rate <= 0.5) etaText='не растёт';
      else {
        const hours = left/(S.rate*60);
        etaText = hours < 1 ? Math.max(1, Math.round(hours*60))+' мин'
                : hours < 48 ? hours.toFixed(1)+' ч'
                : hours < 24*45 ? (hours/24).toFixed(1)+' сут'
                : 'больше месяца';
      }
    }
    setText('eta', etaText);
    setText('zscore', z);
    setText('winEv', evDelta===null?'—':evDelta);
    setText('winCells', changed?changed.toLocaleString('ru-RU'):'0');
    setText('reverts', revert.toLocaleString('ru-RU'));
    setText('turb', (changed/(S.w*S.h)*100).toFixed(3)+'%');
    setText('regions', rg.count.toLocaleString('ru-RU'));
    setText('biggest', rg.top[0] ? rg.top[0].area+' кл @ '+rg.top[0].x0+','+rg.top[0].y0 : '—');
    setText('free', fr.area ? fr.w+'×'+fr.h+' @ '+fr.x+','+fr.y : '—');
    setText('dups', dups);
    setText('rings', rn.join(' / ')+'%');
    setText('snapAge', fmtT(j.fetchedAt));
    setText('srvTime', fmtT(j.serverNow||j.fetchedAt));
    setText('phase', j.phase||'—');
    setText('online', 'online '+(j.online??'—'));
    S.bot = j.bot || S.bot;

    // стек по цветам (раз в окно)
    S.stackHist = S.stackHist || [];
    S.stackHist.push(Array.from(st.cnt)); if (S.stackHist.length>40) S.stackHist.shift();
    stackChart(); spark(); ivals();

    // палитра
    const pal=$('pal'); pal.innerHTML='';
    S.pal.forEach((c,i)=>{
      const n=st.cnt[i]||0, el=document.createElement('div');
      el.className='sw'; el.style.background=c; el.title=i+': '+c+' — '+n;
      pal.appendChild(el);
    });

    if (z!=='—' && Number(z)>3) toast('всплеск активности: z='+z);
    if (st.fill/(S.w*S.h)*100>40 && !S.warned40){ S.warned40=true; toast('заполнение поля больше 40%'); }

    S.last=j; S.nextAt=Date.now()+15000;
    $('conn').textContent='live'; $('conn').className='badge ok'; setText('err','');
    decorate();
    loadDerived();
  }catch(e){
    $('conn').textContent='offline'; $('conn').className='badge bad'; setText('err', e.message);
  }
}

/* ---------------- кланы и топ из api ---------------- */
async function loadDerived(){
  let hasOfficial = false, err = "";
  try{
    const c = await (await fetch('/clans',{cache:'no-store'})).json();
    hasOfficial = c.ok && c.clans.length > 0;
    err = c.ok ? "" : (c.err || "");
    const body=$('clanBody'); body.innerHTML='';
    if (!c.ok){
      body.innerHTML='<tr><td colspan="4" style="color:var(--dim)">нужен токен: '+err+'</td></tr>';
    } else if (!c.clans.length){
      body.innerHTML='<tr><td colspan="4" style="color:var(--dim)">кланов нет</td></tr>';
    } else {
      for (const k of c.clans.slice(0,12)){
        const tr=document.createElement('tr');
        tr.innerHTML='<td style="color:'+hsl(k.tag||'?')+'">'+(k.tag||'—')+'</td>'
          +'<td class="num">'+(k.pixels!=null?k.pixels.toLocaleString('ru-RU'):'—')+'</td>'
          +'<td class="num">'+(k.cells!=null?k.cells.toLocaleString('ru-RU'):'—')+'</td>'
          +'<td class="num">'+(k.members!=null?k.members:'—')+'</td>';
        const info = S.clanInfo && S.clanInfo[k.tag];
        if (info) tr.title = (info.name || k.tag)
          + (info.owner ? ' · владелец ' + info.owner.name : '')
          + (info.description ? '\n' + info.description : '');
        body.appendChild(tr);
      }
    }
    setText('claninfo', hasOfficial
      ? 'данные /api/top'
      : (c.token ? 'токен не принят: ' + err : 'токен не задан'));
  }catch(e){
    setText('claninfo', 'ошибка: ' + e.message);
  }
  decorate();
}

/* ---------------- лента событий ---------------- */
async function pollFeed(){
  try{
    fetch('/events?since='+S.feedSeen,{cache:'no-store'}).then(r=>r.json()).then(j=>{
      setText('feedState', j.err ? 'ошибка: '+j.err : ('seq '+j.last_seq));
      if (j.events && j.events.length){
        S.secCounts = S.secCounts || {};
        for (const e of j.events){
          S.feed.unshift(e);
          S.secCounts[e.sec] = (S.secCounts[e.sec]||0)+1;
        }
        S.feed = S.feed.slice(0,120);
        S.feedSeen = Math.max(S.feedSeen, j.events[j.events.length-1].seq);
        const keys=Object.keys(S.secCounts).map(Number).sort((a,b)=>a-b);
        for (const k of keys.slice(0, keys.length-600)) delete S.secCounts[k];
        renderFeed(); ivals();
      }
    }).catch(()=>setText('feedState','offline'));
  }catch(e){ setText('feedState','offline'); }
}

function renderFeed(){
  $('feed').innerHTML = S.feed.slice(0,14).map(e=>
    '<div>'+(e.sec?new Date(e.sec*1000).toLocaleTimeString('ru-RU',{hour12:false}):'')
    +' <b>('+e.x+','+e.y+')</b> c'+e.c+' <span style="color:'+S.pal[e.c]+'">■</span></div>'
  ).join('');
}

/* ---------------- клик по клетке ---------------- */
$('field').addEventListener('click', async ev => {
  const p = stagePoint(ev);
  if (p.x<0||p.y<0||p.x>=S.w||p.y>=S.h) return;
  const info=$('cellinfo');
  S.picked = p;
  info.textContent='('+p.x+','+p.y+') — запрос…';
  try{
    const j=await (await fetch('/cell?x='+p.x+'&y='+p.y)).json();
    if (!j.by){ info.innerHTML='<b>('+p.x+','+p.y+')</b> — никто не ставил'; return; }
    const t=j.ts?new Date(j.ts).toLocaleString('ru-RU',{hour12:false}):'—';
    info.innerHTML='<b>('+p.x+','+p.y+')</b> · color '+j.color+' · <b style="color:'+hsl(j.by.pid)+'">'+j.by.name+'</b>'
      +(j.by.clan_tag?' ['+j.by.clan_tag+']':'')+' · pid '+j.by.pid+'<br>'+t;
  }catch(e){ info.textContent='ошибка: '+e.message; }
});

$('deep').onclick = () => {
  const p = S.picked || {x:256,y:256};
  const param = '#x='+p.x+'&y='+p.y+'&z=1';
  const bot = S.bot || 'exterapixelbattle_bot';
  window.open('https://t.me/'+bot+'?start='+encodeURIComponent(param), '_blank');
};

/* ---------------- экспорт ---------------- */
$('expPng').onclick = () => {
  const a=document.createElement('a');
  a.href=F.toDataURL('image/png');
  a.download='pixel-'+Math.floor(Date.now()/1000)+'.png';
  a.click();
};
$('expCsv').onclick = () => {
  const rows=[['metric','value']];
  for (const el of document.querySelectorAll('.panel')) {
    for (const kv of el.querySelectorAll('.kv')){
      rows.push([kv.children[0].textContent, kv.children[1].textContent]);
    }
  }
  const blob=new Blob([rows.map(r=>r.join(';')).join('\n')],{type:'text/csv'});
  const a=document.createElement('a');
  a.href=URL.createObjectURL(blob); a.download='pxboard-'+Math.floor(Date.now()/1000)+'.csv'; a.click();
};

/* ---------------- кнопки слоёв ---------------- */
function layerBtn(id, key){
  $(id).onclick = e => {
    for (const k in S.layer) S.layer[k]=false;
    S.layer[key]=true;
    for (const [bid,bk] of [['lDiff','diff'],['lFill','fill'],['lEdge','edge'],['lHot','hot'],
                            ['lGrid','grid']]){
      $(bid).classList.toggle('on', bk===key);
    }
    repaintLayer();
  };
}
layerBtn('lDiff','diff'); layerBtn('lFill','fill'); layerBtn('lEdge','edge'); layerBtn('lHot','hot');
layerBtn('lGrid','grid');

$('btnNow').onclick = load;
$('btnAuto').onclick = () => {
  S.auto=!S.auto; $('btnAuto').classList.toggle('on', S.auto);
  clearInterval(S.timer);
  if (S.auto){ S.timer=setInterval(load,15000); S.nextAt=Date.now()+15000; }
};

setInterval(()=>{
  if (!S.auto){ setText('timer','paused'); return; }
  setText('timer', Math.max(0,Math.ceil((S.nextAt-Date.now())/1000))+'s');
}, 500);

async function loadToken(){
  try{
    const t = await (await fetch('/token',{cache:'no-store'})).json();
    const el = $('tkState');
    const names = {ok:'жив', dead:'протух', none:'нет токена', net:'сеть недоступна',
                   error:'ошибка', banned:'аккаунт забанен', checking:'проверка'};
    el.textContent = names[t.state] || t.state;
    el.style.color = t.state === 'ok' ? 'var(--ok)'
      : (t.state === 'dead' || t.state === 'banned' || t.state === 'error') ? 'var(--bad)'
      : 'var(--dim)';
    if (t.code && t.state !== 'ok') el.textContent += ' (' + t.code + ')';

    if (t.expires_at){
      const ms = t.expires_at - Date.now();
      const h = ms/3600000;
      setText('tkExp', ms <= 0 ? 'истёк'
        : h >= 24 ? (h/24).toFixed(1)+' сут'
        : h >= 1 ? h.toFixed(1)+' ч'
        : Math.max(1, Math.round(h*60))+' мин');
    } else setText('tkExp', '—');

    const me = t.me;
    if (me){
      setText('tkAcc', me.name + (me.username ? ' @'+me.username : '') + ' · ' + (me.pid||'').slice(0,8));
      setText('tkStatus', (me.status||'—') + (me.number ? ' #'+me.number : '') + ' · ' + (me.tier||''));
      setText('tkClan', me.clan ? me.clan.tag+' · '+me.clan.role : 'без клана');
      setText('tkCharges', (me.max_charges!=null?me.max_charges:'—') + (me.slowed?' · замедлен':''));
      setText('tkCap', (me.day_used!=null?me.day_used:'—') + ' / ' + (me.day_cap!=null?me.day_cap:'—'));
      setText('tkPixels', (me.pixels!=null?me.pixels.toLocaleString('ru-RU'):'—'));
    } else {
      ['tkAcc','tkStatus','tkClan','tkCharges','tkCap','tkPixels'].forEach(id=>setText(id,'—'));
    }
    setText('tkChecked', t.checked ? new Date(t.checked).toLocaleTimeString('ru-RU',{hour12:false}) : '—');
    if (t.checked){
      const left = Math.max(0, 60 - Math.round((Date.now() - t.checked)/1000));
      setText('tkNext', left + ' с');
    } else setText('tkNext', '—');
  }catch(e){
    setText('tkState', 'нет ответа');
    $('tkState').style.color = 'var(--warn)';
  }
}

$('tkSet').onclick = async () => {
  const pass = prompt('парольная фраза');
  if (!pass) return;
  const v = prompt('вставь initData целиком или готовый bearer');
  if (!v) return;
  const body = v.indexOf('query_id=') === 0 || v.indexOf('auth_date=') > 0
    ? {pass: pass, initdata: v.trim()} : {pass: pass, token: v.trim()};
  const r = await fetch('/token', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
  const j = await r.json();
  if (!j.ok){
    const why = {bad_pass:'неверная фраза', pass_not_set:'фраза не задана: python pxboard.py --set-pass',
                 too_many_tries:'слишком много попыток, подожди 5 минут',
                 origin:'запрос не с localhost'}[j.err] || j.err || j.code;
    alert('не принято: ' + why);
  }
  setTimeout(loadToken, 800);
};

async function loadTop(){
  try{
    const t = await (await fetch('/top',{cache:'no-store'})).json();
    const body = $('topBody');
    if (!t.ok){
      body.innerHTML = '<tr><td colspan="4" style="color:var(--dim)">нет данных: '+ (t.err||'токен') +'</td></tr>';
      return;
    }
    body.innerHTML = '';
    const me = (t.me && t.me.rank) ? t.me : null;
    for (const p of t.players.slice(0,15)){
      const tr = document.createElement('tr');
      const mine = me && p.rank === me.rank;
      tr.innerHTML = '<td style="color:var(--dim)">'+p.rank+'</td>'
        + '<td style="color:'+hsl(p.pid||p.name)+'">'+p.name+(mine?' ←':'')+'</td>'
        + '<td class="num">'+p.pixels.toLocaleString('ru-RU')+'</td>'
        + '<td class="num" style="color:var(--dim)">'+(p.clan_tag||'—')+'</td>';
      body.appendChild(tr);
    }
  }catch(e){
    $('topBody').innerHTML = '<tr><td colspan="4" style="color:var(--dim)">ошибка: '+e.message+'</td></tr>';
  }
}

async function loadClanList(){
  try{
    const t = await (await fetch('/clanlist',{cache:'no-store'})).json();
    if (!t.ok) return;
    S.clanInfo = {};
    for (const c of t.clans) S.clanInfo[c.tag] = c;
  }catch(e){}
}

loadTop();
loadClanList();
setInterval(loadTop, 60000);
setInterval(loadClanList, 120000);

loadToken();
setInterval(loadToken, 30000);

load();
S.timer = setInterval(load, 15000);
setInterval(pollFeed, 2000);
</script>
</body></html>
"""


# ---------------------------------------------------------------- HTTP


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _send(self, code, body, ctype):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, code=200):
        self._send(code, json.dumps(obj), "application/json")

    def do_POST(self):
        p = urllib.parse.urlparse(self.path)
        if p.path != "/token":
            return self._json({"ok": False, "err": "not_found"}, 404)

        # csrf: посторонние сайты не должны уметь менять токен
        origin = self.headers.get("Origin") or ""
        if origin and "127.0.0.1" not in origin and "localhost" not in origin:
            return self._json({"ok": False, "err": "origin"}, 403)

        if not pass_configured():
            return self._json({"ok": False, "err": "pass_not_set",
                               "hint": "запусти: python pxboard.py --set-pass"}, 403)
        if throttled():
            return self._json({"ok": False, "err": "too_many_tries"}, 429)

        try:
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n) or b"{}")
        except Exception as e:
            return self._json({"ok": False, "err": type(e).__name__}, 400)

        phrase = body.get("pass") or self.headers.get("X-Pass") or ""
        if not pass_ok(phrase):
            PASS_FAILS.append(time.time())
            return self._json({"ok": False, "err": "bad_pass"}, 403)

        init = (body.get("initdata") or body.get("init_data") or "").strip()
        tok = (body.get("token") or "").strip()

        if init:
            new, expires, err = exchange_initdata(init)
            if not new:
                return self._json({"ok": False, "err": err or "auth"}, 200)
            save_token(new, expires)
            return self._json({"ok": True, "state": "checking", "expires_at": expires})

        if tok:
            old_tok, old_meta = TOKEN, TOKEN_META
            save_token(tok, None)
            check_token()
            if TOK.get("state") != "ok":
                st, code = TOK.get("state"), TOK.get("code")
                save_token(old_tok, old_meta.get("expires_at"), checking=False)
                check_token()
                return self._json({"ok": False, "err": st, "code": code}, 200)
            return self._json({"ok": True, "state": "ok"})

        return self._json({"ok": False, "err": "empty"}, 400)

    def do_GET(self):
        p = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(p.query)
        try:
            if p.path in ("/", "/index.html"):
                return self._send(200, HTML, "text/html; charset=utf-8")
            if p.path == "/snapshot":
                return self._json(get_snapshot())
            if p.path == "/cell":
                x = int(q.get("x", ["0"])[0])
                y = int(q.get("y", ["0"])[0])
                return self._send(200, fetch("/api/cell?x=%d&y=%d" % (x, y)), "application/json")
            if p.path == "/clans":
                return self._json(clan_stats())
            if p.path == "/top":
                return self._json(top_data())
            if p.path == "/clanlist":
                return self._json(clan_list())
            if p.path == "/token":
                return self._json({
                    "has_token": bool(TOKEN),
                    "state": TOK.get("state") or "none",
                    "code": TOK.get("code"),
                    "checked": TOK.get("checked") or 0,
                    "me": TOK.get("me"),
                    "err": TOK.get("err") or "",
                    "expires_at": TOKEN_META.get("expires_at"),
                    "updated": TOKEN_META.get("updated"),
                })
            if p.path == "/events":
                since = int(q.get("since", ["0"])[0] or 0)
                ev = [e for e in list(EVENTS) if e["seq"] > since]
                return self._json({"events": ev[-200:], "last_seq": FEED["last_seq"],
                                   "seg": FEED["seg"], "err": FEED["err"]})
        except Exception as e:
            return self._json({"err": str(e)}, 500)
        if p.path == "/favicon.ico":
            return self._send(204, b"", "image/x-icon")
        return self._json({"err": "not_found"}, 404)


def main():
    global TOKEN, DB_PATH, TOKEN_META
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8808)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--token", default="", help="bearer из /api/auth, либо переменная окружения PX_TOKEN")
    ap.add_argument("--set-pass", action="store_true", help="задать парольную фразу для смены токена")
    a = ap.parse_args()

    if a.set_pass:
        import getpass
        import secrets
        if sys.stdin.isatty():
            p1 = getpass.getpass("парольная фраза: ")
            p2 = getpass.getpass("ещё раз: ")
        else:
            lines = sys.stdin.read().splitlines()
            p1 = lines[0].strip() if lines else ""
            p2 = lines[1].strip() if len(lines) > 1 else ""
        if not p1 or p1 != p2:
            print("не совпало или пусто")
            return 2
        cfg = load_config()
        cfg["pass_salt"] = secrets.token_hex(16)
        cfg["iterations"] = PBKDF2_ITER
        cfg["pass_hash"] = hash_pass(p1, cfg["pass_salt"], PBKDF2_ITER)
        cfg.setdefault("port", a.port)
        save_config(cfg)
        print("фраза записана как pbkdf2-хеш в", CONFIG_PATH)
        return 0

    here = os.path.dirname(os.path.abspath(__file__))
    TOKEN = a.token or os.environ.get("PX_TOKEN", "")
    if not TOKEN:
        TOKEN = read_token_file()
    TOKEN_META = read_meta_file()

    threading.Thread(target=feed_loop, daemon=True).start()
    threading.Thread(target=token_loop, daemon=True).start()
    print("pxboard: http://%s:%d/  token=%s  pass=%s"
          % (a.host, a.port, "есть" if TOKEN else "нет",
             "задан" if pass_configured() else "не задан"))
    return ThreadingHTTPServer((a.host, a.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
