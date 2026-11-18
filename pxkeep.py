#!/usr/bin/env python3
"""держит pxboard живым и обновляет токен сам.

  python pxkeep.py

читает initdata.txt рядом со скриптом, обменивает на bearer, запускает pxboard
с токеном в окружении. раз в минуту проверяет /api/me: если 401 — обменивает
заново и перезапускает сервер. если initdata протухла — ждёт новую в файле.

положить новую строку: initdata.txt, одна строка, без кавычек.
"""

import json
import io
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

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

HERE = os.path.dirname(os.path.abspath(__file__))
INIT_FILE = os.path.join(HERE, "initdata.txt")
PORT = 8808
UA = ("Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/120.0.0.0 Mobile Safari/537.36")
TARGET = "https://pixel.mk69.dev"

token = ""
child = None
last_note = ""


def note(msg):
    global last_note
    if msg != last_note:
        print("%s %s" % (time.strftime("%H:%M:%S"), msg), flush=True)
        last_note = msg


def read_init():
    try:
        with open(INIT_FILE, encoding="utf-8") as f:
            s = f.read().strip()
        return s or None
    except FileNotFoundError:
        return None


def atomic_write(path, text):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


def auth(init):
    data = json.dumps({"init_data": init}).encode("utf-8")
    req = urllib.request.Request(TARGET + "/api/auth", data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", UA)
    req.add_header("Origin", TARGET)
    req.add_header("Referer", TARGET + "/")
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            j = json.loads(r.read())
        return j.get("token", ""), None
    except urllib.error.HTTPError as e:
        try:
            err = json.loads(e.read()).get("err", "http_%d" % e.code)
        except Exception:
            err = "http_%d" % e.code
        return "", err
    except Exception as e:
        return "", type(e).__name__


def alive(tok):
    req = urllib.request.Request(TARGET + "/api/me")
    req.add_header("Authorization", "Bearer " + tok)
    req.add_header("User-Agent", UA)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status == 200
    except Exception:
        return False


def port_busy():
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", PORT)) == 0


def start_board():
    global child
    env = dict(os.environ)
    env["PX_TOKEN"] = token
    env["PYTHONIOENCODING"] = "utf-8"
    child = subprocess.Popen([sys.executable, os.path.join(HERE, "pxboard.py"), "--port", str(PORT)],
                             cwd=HERE, env=env,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    note("pxboard запущен, pid %d, токен %s…" % (child.pid, token[:6]))


def stop_board():
    global child
    if child and child.poll() is None:
        child.terminate()
        try:
            child.wait(10)
        except Exception:
            child.kill()
    child = None


def main():
    global token
    note("supervisor старт, порт %d" % PORT)
    while True:
        init = read_init()
        if init and (not token or not alive(token)):
            new, err = auth(init)
            if new:
                token = new
                atomic_write(os.path.join(HERE, "token.txt"), token)
                note("токен обновлён, %s…" % token[:6])
                stop_board()
                time.sleep(1)
                start_board()
            elif err == "expired":
                note("initdata протухла — положи новую строку в initdata.txt")
            else:
                note("авторизация не прошла: %s" % err)
        elif not init:
            note("нет initdata.txt — только сервер без токена")

        if child is None or child.poll() is not None:
            if not port_busy():
                start_board()
        time.sleep(60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        stop_board()
