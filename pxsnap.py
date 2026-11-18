#!/usr/bin/env python3
"""pixel.mk69.dev автор-снапшот.

Режимы:
  baseline  — обход всех 512x512 клеток через /api/cell, докачиваемый, пишет в sqlite
  delta     — только изменившиеся с прошлого прогона клетки (сравнение по /live/canvas.bin)
  dump      — выгрузка текущей базы в jsonl

Запуск:
  python pxsnap.py baseline --rps 2
  python pxsnap.py delta
  python pxsnap.py dump --out field.jsonl

Только stdlib. Одна база px.db рядом со скриптом.
"""

import argparse
import collections
import http.client
import io
import json
import sqlite3
import sys
import threading
import time
import urllib.request

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

HOST = "pixel.mk69.dev"
W = H = 512
DB = "px.db"
SNAP = "/live/canvas.bin"

SCHEMA = """
CREATE TABLE IF NOT EXISTS cells(
  idx     INTEGER PRIMARY KEY,
  x       INTEGER NOT NULL,
  y       INTEGER NOT NULL,
  color   INTEGER,
  ts      INTEGER,
  pid     TEXT,
  name    TEXT,
  clan    TEXT,
  avatar  TEXT,
  checked INTEGER,          -- ms, когда ответили
  pending INTEGER DEFAULT 1 -- 0 = клетка уже подтверждена сервером
);
CREATE INDEX IF NOT EXISTS idx_cells_pending ON cells(pending);
CREATE INDEX IF NOT EXISTS idx_cells_pid ON cells(pid);
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
"""


def db():
    c = sqlite3.connect(DB, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript(SCHEMA)
    return c


def seed(c):
    n = c.execute("SELECT COUNT(*) FROM cells").fetchone()[0]
    if n == 0:
        rows = ((i, i % W, i // W) for i in range(W * H))
        c.executemany("INSERT INTO cells(idx,x,y) VALUES(?,?,?)", rows)
        c.commit()
        print("seeded %d cells" % (W * H))


def fetch_snapshot():
    req = urllib.request.Request("https://%s%s" % (HOST, SNAP))
    with urllib.request.urlopen(req, timeout=30) as r:
        b = r.read()
    if len(b) != 24 + W * H or b[:4] != b"PXC1":
        raise ValueError("bad snapshot %d bytes" % len(b))
    ts = int.from_bytes(b[4:8], "little")
    seq = int.from_bytes(b[16:20], "little")
    return ts, seq, b[24:]


class Sweeper:
    """Пулы keep-alive соединений, адаптивный backoff на ошибках."""

    def __init__(self, workers, rps):
        self.workers = workers
        self.delay = 1.0 / rps if rps > 0 else 0.0
        self.stop = threading.Event()
        self.stats = collections.Counter()
        self.lock = threading.Lock()

    def hit(self, conn, x, y, retries=3):
        """Один запрос по живому keep-alive соединению; на ошибке соединение
        пересоздаётся, backoff растёт. Возвращает (json|None, conn)."""
        path = "/api/cell?x=%d&y=%d" % (x, y)
        backoff = 3.0
        for _ in range(retries):
            try:
                conn.request("GET", path, headers={"Accept": "application/json"})
                r = conn.getresponse()
                body = r.read()
                if r.status == 200:
                    return json.loads(body), conn
                with self.lock:
                    self.stats["http_%d" % r.status] += 1
                if r.status == 404:
                    return None, conn
            except Exception as e:
                with self.lock:
                    self.stats["err:%s" % type(e).__name__] += 1
            try:
                conn.close()
            except Exception:
                pass
            conn = http.client.HTTPSConnection(HOST, 443, timeout=30)
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)
        return None, conn

    def run(self, jobs):
        """jobs: список (idx,x,y). Возвращает число подтверждённых."""
        q = list(jobs)
        qlock = threading.Lock()
        ok = [0]

        def worker():
            local = []
            conn = http.client.HTTPSConnection(HOST, 443, timeout=30)
            while not self.stop.is_set():
                with qlock:
                    if not q:
                        break
                    idx, x, y = q.pop()
                d, conn = self.hit(conn, x, y)
                if d is not None:
                    by = d.get("by") or {}
                    local.append((d.get("color"), d.get("ts"), by.get("pid"),
                                  by.get("name"), by.get("clan_tag"), by.get("avatar"),
                                  int(time.time() * 1000), idx))
                    ok[0] += 1
                if self.delay:
                    time.sleep(self.delay)
                if len(local) >= 25:
                    self.flush(local)
                    local = []
            try:
                conn.close()
            except Exception:
                pass
            self.flush(local)

        def drain():
            ths = [threading.Thread(target=worker, daemon=True) for _ in range(self.workers)]
            for t in ths:
                t.start()
            for t in ths:
                t.join()
        drain()
        return ok[0]

    def flush(self, rows):
        if not rows:
            return
        c = db()
        try:
            c.executemany(
                "UPDATE cells SET color=?,ts=?,pid=?,name=?,clan=?,avatar=?,checked=?,pending=0 WHERE idx=?",
                rows)
            c.commit()
        finally:
            c.close()


def cmd_baseline(args):
    c = db()
    seed(c)
    todo = c.execute("SELECT idx,x,y FROM cells WHERE pending=1 ORDER BY idx").fetchall()
    c.close()
    if args.limit:
        todo = todo[:args.limit]
    total = len(todo)
    print("осталось клеток: %d, темп %s rps, потоков %d" % (total, args.rps, args.workers))
    if not total:
        print("всё уже собрано")
        return
    t0 = time.time()
    sw = Sweeper(args.workers, args.rps)
    done_before = 0
    step = 2000
    for i in range(0, total, step):
        chunk = todo[i:i + step]
        n = sw.run(chunk)
        done_before += n
        el = time.time() - t0
        rate = done_before / el if el else 0
        left = total - done_before
        eta = left / rate / 60 if rate else -1
        print("  %d/%d ok=%d  %.1f rps  ETA %.0f мин  %s"
              % (i + len(chunk), total, done_before, rate, eta, dict(sw.stats)))
    print("готово за %.0f мин" % ((time.time() - t0) / 60))


def cmd_delta(args):
    c = db()
    seed(c)
    ts, seq, body = fetch_snapshot()
    have = dict(c.execute("SELECT idx, color FROM cells").fetchall())
    jobs = []
    for i, col in enumerate(body):
        if have.get(i, -1) != col or i not in have:
            jobs.append((i, i % W, i // W))
    c.close()
    print("снапшот ts=%d seq=%d, изменённых/неизвестных клеток: %d" % (ts, seq, len(jobs)))
    if not jobs:
        return
    sw = Sweeper(args.workers, args.rps)
    t0 = time.time()
    ok = sw.run(jobs)
    print("подтверждено %d из %d за %.1f c %s" % (ok, len(jobs), time.time() - t0, dict(sw.stats)))


def cmd_dump(args):
    c = db()
    n = 0
    with open(args.out, "w", encoding="utf-8") as f:
        cur = c.execute("SELECT x,y,color,ts,pid,name,clan FROM cells WHERE pending=0 ORDER BY idx")
        for x, y, color, ts, pid, name, clan in cur:
            f.write(json.dumps({"x": x, "y": y, "color": color, "ts": ts,
                                "pid": pid, "name": name, "clan": clan},
                               ensure_ascii=False) + "\n")
            n += 1
    print("выгружено %d строк в %s" % (n, args.out))
    top = c.execute("""SELECT name, pid, COUNT(*) n, MIN(ts) first, MAX(ts) last
                       FROM cells WHERE pid IS NOT NULL GROUP BY pid
                       ORDER BY n DESC LIMIT 15""").fetchall()
    for name, pid, n2, fst, lst in top:
        print("  %-20s %-17s %5d" % (name, pid, n2))
    c.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["baseline", "delta", "dump"])
    p.add_argument("--rps", type=float, default=1.0, help="запросов в секунду на поток")
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--out", default="field.jsonl")
    a = p.parse_args()
    {"baseline": cmd_baseline, "delta": cmd_delta, "dump": cmd_dump}[a.mode](a)


if __name__ == "__main__":
    sys.exit(main())
