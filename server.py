from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
import json, os, hashlib, psycopg2, psycopg2.extras
from typing import Dict
from datetime import datetime

app = FastAPI()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Database (PostgreSQL) ─────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL")  # Render tự inject biến này

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id         SERIAL PRIMARY KEY,
            username   TEXT NOT NULL UNIQUE,
            password   TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id         SERIAL PRIMARY KEY,
            username   TEXT NOT NULL,
            text       TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()

def user_exists(username: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM users WHERE LOWER(username) = LOWER(%s)", (username,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row is not None

def register_user(username: str, password: str) -> bool:
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (username, password, created_at) VALUES (%s, %s, %s)",
            (username, hash_pw(password), datetime.now().isoformat())
        )
        conn.commit()
        cur.close(); conn.close()
        return True
    except psycopg2.IntegrityError:
        return False

def login_user(username: str, password: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM users WHERE LOWER(username) = LOWER(%s) AND password = %s",
        (username, hash_pw(password))
    )
    row = cur.fetchone()
    cur.close(); conn.close()
    return row is not None

def save_message(username: str, text: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO messages (username, text, created_at) VALUES (%s, %s, %s)",
        (username, text, datetime.now().strftime("%H:%M %d/%m/%Y"))
    )
    conn.commit()
    cur.close(); conn.close()

def get_recent_messages(limit: int = 50):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT username, text, created_at FROM messages ORDER BY id DESC LIMIT %s", (limit,)
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return list(reversed(rows))

init_db()

# ── REST API ──────────────────────────────────────────
class AuthBody(BaseModel):
    username: str
    password: str

RESET_SECRET = "privchat-reset-2024"

@app.get("/api/reset/{secret}")
def reset_db(secret: str):
    if secret != RESET_SECRET:
        raise HTTPException(403, "Sai mật khẩu")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM users")
    cur.execute("DELETE FROM messages")
    conn.commit()
    cur.close(); conn.close()
    return {"ok": True, "msg": "Đã xóa toàn bộ tài khoản và tin nhắn"}

@app.post("/api/register")
def api_register(body: AuthBody):
    name = body.username.strip()
    pw   = body.password.strip()
    if not name or not pw:
        raise HTTPException(400, "Thiếu thông tin")
    if len(name) < 2 or len(name) > 30:
        raise HTTPException(400, "Tên phải từ 2–30 ký tự")
    if len(pw) < 4:
        raise HTTPException(400, "Mật khẩu phải ít nhất 4 ký tự")
    if not register_user(name, pw):
        raise HTTPException(409, "Tên này đã được dùng")
    return {"ok": True}

@app.post("/api/login")
def api_login(body: AuthBody):
    name = body.username.strip()
    pw   = body.password.strip()
    if not login_user(name, pw):
        raise HTTPException(401, "Tên hoặc mật khẩu không đúng")
    return {"ok": True, "username": name}

# ── WebSocket ─────────────────────────────────────────
connected_clients: Dict[WebSocket, str] = {}

async def broadcast(message: dict, exclude: WebSocket = None):
    data = json.dumps(message, ensure_ascii=False)
    disconnected = []
    for ws in connected_clients:
        if ws != exclude:
            try:
                await ws.send_text(data)
            except:
                disconnected.append(ws)
    for ws in disconnected:
        connected_clients.pop(ws, None)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    username = None
    try:
        data = await websocket.receive_text()
        payload = json.loads(data)
        username = payload.get("username", "").strip()

        if not username or not user_exists(username):
            await websocket.send_text(json.dumps({"type": "error", "text": "Chưa đăng nhập"}))
            await websocket.close()
            return

        connected_clients[websocket] = username

        history = get_recent_messages(50)
        await websocket.send_text(json.dumps({
            "type": "init",
            "username": username,
            "online": len(connected_clients),
            "history": [{"username": r["username"], "text": r["text"], "time": r["created_at"]} for r in history]
        }, ensure_ascii=False))

        await broadcast({
            "type": "system",
            "text": f"👋 {username} đã tham gia.",
            "online": len(connected_clients)
        })

        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            if msg.get("type") == "message":
                text = msg.get("text", "").strip()
                if text:
                    time_str = datetime.now().strftime("%H:%M %d/%m/%Y")
                    save_message(username, text)
                    await broadcast({
                        "type": "message",
                        "username": username,
                        "text": text,
                        "time": time_str,
                        "online": len(connected_clients)
                    })

    except WebSocketDisconnect:
        connected_clients.pop(websocket, None)
        if username:
            await broadcast({
                "type": "system",
                "text": f"👋 {username} đã rời phòng.",
                "online": len(connected_clients)
            })
    except Exception:
        connected_clients.pop(websocket, None)

app.mount("/static", StaticFiles(directory=BASE_DIR), name="static")

@app.get("/")
async def root():
    return FileResponse(os.path.join(BASE_DIR, "index.html"))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
