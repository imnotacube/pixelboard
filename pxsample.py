#!/usr/bin/env python3
"""Выборка авторского слоя: N случайных закрашенных клеток через /api/cell.

  python pxsample.py --n 800          # 1 запрос раз в ~0.8 с
  python pxsample.py --n 400 --delay 1.2

Пишет в px.db (таблица cells), после этого pxboard.py показывает кланы.
"""

import argparse
import io
import json
import random
import sqlite3
import sys
import time
import urllib.error
import urllib.request

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

TARGET = "https://pixel.mk69.dev"
DB = "px.db"


def fetch(path, timeout=25):
    with urllib.request.urlopen(TARGET + path, timeout=timeout) as r:
        return r.read()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=800)
    ap.add_argument("--delay", type=float, default=0.8)
    ap.add_argument("--db", default=DB)
    a = ap.parse_args()

    raw = fetch("/live/canvas.bin")
    w = int.from_bytes(raw[12:14], "little")
    h = int.from_bytes(raw[14:16], "little")
    body = raw[24:24 + w * h]
    painted = [i for i, v in enumerate(body) if v]
    print("поле %dx%d, закрашено %d" % (w, h, len(painted)))

    c = sqlite3.connect(a.db, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript("""CREATE TABLE IF NOT EXISTS cells(idx INTEGER PRIMARY KEY, x INTEGER, y INTEGER,
      color INTEGER, ts INTEGER, pid TEXT, name TEXT, clan TEXT, avatar TEXT,
      checked INTEGER, pending INTEGER DEFAULT 1);""")
    done = {r[0] for r in c.execute("SELECT idx FROM cells WHERE pid IS NOT NULL")}
    pool = [i for i in painted if i not in done]
    random.shuffle(pool)
    todo = pool[:a.n]
    print("уже собрано %d, беру %d" % (len(done), len(todo)))

    ok = fail = 0
    t0 = time.time()
    for k, idx in enumerate(todo, 1):
        x, y = idx % w, idx // w
        for attempt in range(3):
            try:
                d = json.loads(fetch("/api/cell?x=%d&y=%d" % (x, y)))
                by = d.get("by") or {}
                c.execute("""INSERT INTO cells(idx,x,y,color,ts,pid,name,clan,avatar,checked,pending)
                             VALUES(?,?,?,?,?,?,?,?,?,?,0)
                             ON CONFLICT(idx) DO UPDATE SET color=?,ts=?,pid=?,name=?,clan=?,
                             avatar=?,checked=?,pending=0""",
                          (idx, x, y, d.get("color"), d.get("ts"), by.get("pid"),
                           by.get("name"), by.get("clan_tag"), by.get("avatar"),
                           int(time.time() * 1000),
                           d.get("color"), d.get("ts"), by.get("pid"), by.get("name"),
                           by.get("clan_tag"), by.get("avatar"), int(time.time() * 1000)))
                c.commit()
                ok += 1
                break
            except Exception as e:
                if attempt == 2:
                    fail += 1
                    print("  miss (%d,%d): %s" % (x, y, e))
                else:
                    time.sleep(3 * (attempt + 1))
        time.sleep(a.delay)
        if k % 25 == 0:
            el = time.time() - t0
            print("  %d/%d ok=%d fail=%d  %.1f/с  осталось ~%.0f мин"
                  % (k, len(todo), ok, fail, ok / el, (len(todo) - k) * a.delay / 60))
    c.close()
    print("готово: ok=%d fail=%d за %.0f c" % (ok, fail, time.time() - t0))


if __name__ == "__main__":
    main()
