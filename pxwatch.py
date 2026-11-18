#!/usr/bin/env python3
"""Живой сбор постановок с pixel.mk69.dev через /live/head.bin.

  python pxwatch.py watch            # опрос head.bin раз в секунду, бесконечно
  python pxwatch.py catchup --mins 10  # добить сегменты за последние N минут
  python pxwatch.py report           # агрегат по авторам

Формат PXS1 (40 байт шапки):
  0   magic "PXS1"
  4   u32  n            номер сегмента = unix-секунда
  8   u32  stateVer
  12  u16  w
  14  u16  h
  16  u64  first_seq
  24  u64  last_seq
  32  u8   flags        bit0 = требуется ресинк снапшотом
  36  u32  count
  40+ count * (u16 x, u16 y, u8 color)
"""

import argparse
import io
import queue
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

HOST = "pixel.mk69.dev"
BASE = "https://%s" % HOST
DB = "px.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS events(
  seq     INTEGER PRIMARY KEY,
  sec     INTEGER,
  x       INTEGER,
  y       INTEGER,
  color   INTEGER,
  pid     TEXT,
  name    TEXT,
  clan    TEXT,
  ts_ms   INTEGER,
  done    INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_events_done ON events(done);
CREATE INDEX IF NOT EXISTS idx_events_pid ON events(pid);
CREATE INDEX IF NOT EXISTS idx_events_sec ON events(sec);
"""


def db():
    c = sqlite3.connect(DB, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript(SCHEMA)
    return c


def fetch(path, tries=3, wait=2.0):
    for _ in range(tries):
        try:
            with urllib.request.urlopen(BASE + path, timeout=25) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            time.sleep(wait)
            wait = min(wait * 2, 60)
        except Exception:
            time.sleep(wait)
            wait = min(wait * 2, 60)
    return None


def parse(b):
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
    return {"n": n, "first": first, "last": last, "flags": flags, "count": count, "entries": ents}


class Store:
    def __init__(self):
        self.c = db()
        self.lock = threading.Lock()

    def add(self, rows):
        if not rows:
            return 0
        with self.lock:
            cur = self.c.executemany(
                "INSERT OR IGNORE INTO events(seq,sec,x,y,color) VALUES(?,?,?,?,?)", rows)
            self.c.commit()
            return cur.rowcount

    def pending(self, limit=50):
        with self.lock:
            return self.c.execute(
                "SELECT seq,x,y FROM events WHERE done=0 ORDER BY seq LIMIT ?", (limit,)).fetchall()

    def attribute(self, seq, x, y):
        b = fetch("/api/cell?x=%d&y=%d" % (x, y), tries=2)
        if b is None:
            return False
        import json
        try:
            d = json.loads(b)
        except Exception:
            return False
        by = d.get("by") or {}
        with self.lock:
            self.c.execute(
                "UPDATE events SET pid=?,name=?,clan=?,ts_ms=?,done=1 WHERE seq=?",
                (by.get("pid"), by.get("name"), by.get("clan_tag"), d.get("ts"), seq))
            self.c.commit()
        return True

    def seq_floor(self):
        with self.lock:
            r = self.c.execute("SELECT MAX(seq) FROM events").fetchone()
        return r[0]


def attribute_loop(st, stop, delay=0.0):
    """Один поток-атрибутор: 1 запрос /api/cell на событие."""
    while not stop.is_set():
        batch = st.pending(20)
        if not batch:
            time.sleep(0.3)
            continue
        for seq, x, y in batch:
            if stop.is_set():
                return
            st.attribute(seq, x, y)
            if delay:
                time.sleep(delay)


def ingest(st, seg, floor):
    if not seg:
        return 0
    rows = []
    for i, (x, y, c) in enumerate(seg["entries"]):
        seq = seg["first"] + i
        if floor is not None and seq <= floor:
            continue
        rows.append((seq, seg["n"], x, y, c))
    return st.add(rows)


def cmd_watch(a):
    st = Store()
    floor = st.seq_floor()
    if a.backfill:
        floor = None
    stop = threading.Event()
    th = threading.Thread(target=attribute_loop, args=(st, stop, a.delay), daemon=True)
    th.start()
    print("watch: старт, floor=%s, атрибуция %s" % (floor, "вкл" if not a.no_author else "выкл"))
    if a.no_author:
        stop.set()
    got = 0
    try:
        while True:
            b = fetch("/live/head.bin", tries=2)
            seg = parse(b)
            if seg is None:
                time.sleep(1)
                continue
            n = ingest(st, seg, floor)
            if seg["last"]:
                floor = seg["last"] if floor is None else max(floor, seg["last"])
            got += n
            if n:
                print("  +%d seq<=%d sec=%d total=%d" % (n, seg["last"], seg["n"], got))
            time.sleep(max(0.0, a.interval))
    except KeyboardInterrupt:
        stop.set()
        print("стоп, событий собрано %d" % got)


def cmd_catchup(a):
    st = Store()
    floor = st.seq_floor()
    now = int(time.time())
    have = 0
    for sec in range(now - a.mins * 60, now + 1):
        seg = parse(fetch("/live/%d.bin" % sec, tries=1))
        if seg:
            have += ingest(st, seg, a.force and None or None)
        time.sleep(a.delay)
    print("catchup: добавлено %d событий за %d мин" % (have, a.mins))
    if not a.no_author:
        stop = threading.Event()
        attribute_loop(st, stop, a.delay)


def cmd_report(a):
    c = db()
    rows = c.execute("""SELECT name, pid, count(*) n, datetime(min(sec),'unixepoch') f,
                               datetime(max(sec),'unixepoch') l
                        FROM events WHERE pid IS NOT NULL
                        GROUP BY pid ORDER BY n DESC LIMIT ?""", (a.top,)).fetchall()
    tot = c.execute("SELECT count(*), count(pid) FROM events").fetchone()
    print("событий всего %d, атрибутировано %d" % tot)
    for name, pid, n, f, l in rows:
        print("  %-22s %-17s %4d  %s .. %s" % (name, pid, n, f, l))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["watch", "catchup", "report"])
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--delay", type=float, default=0.0, help="пауза между /api/cell")
    p.add_argument("--mins", type=int, default=10)
    p.add_argument("--backfill", action="store_true")
    p.add_argument("--no-author", action="store_true")
    p.add_argument("--force", action="store_true")
    p.add_argument("--top", type=int, default=15)
    a = p.parse_args()
    {"watch": cmd_watch, "catchup": cmd_catchup, "report": cmd_report}[a.mode](a)


if __name__ == "__main__":
    sys.exit(main())
