from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import json, sqlite3, os
from typing import Dict
from datetime import datetime

app = FastAPI()

# Trên Render dùng /data để lưu lâu dài, local thì lưu cạnh file
DB_PATH = "/data/chat.db" if os.path.isdir("/data") else "chat.db"

# ── Database ──────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            username  TEXT NOT NULL,
            text      TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

def save_message(username: str, text: str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO messages (username, text, created_at) VALUES (?, ?, ?)",
        (username, text, datetime.now().strftime("%H:%M %d/%m/%Y"))
    )
    conn.commit()
    conn.close()

def get_recent_messages(limit: int = 50):
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT username, text, created_at FROM messages ORDER BY id DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return list(reversed(rows))  # Trả về theo thứ tự cũ → mới

init_db()

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
        # Nhận tên người dùng
        data = await websocket.receive_text()
        payload = json.loads(data)
        username = payload.get("username", "Ẩn danh").strip() or "Ẩn danh"
        connected_clients[websocket] = username

        # Gửi lịch sử 50 tin nhắn gần nhất cho người mới vào
        history = get_recent_messages(50)
        await websocket.send_text(json.dumps({
            "type": "init",
            "username": username,
            "online": len(connected_clients),
            "history": [
                {"username": u, "text": t, "time": ts}
                for u, t, ts in history
            ]
        }, ensure_ascii=False))

        # Thông báo người mới vào cho mọi người
        await broadcast({
            "type": "system",
            "text": f"👋 {username} đã tham gia phòng chat.",
            "online": len(connected_clients)
        })

        # Vòng lặp nhận tin nhắn
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
                "text": f"👋 {username} đã rời phòng chat.",
                "online": len(connected_clients)
            })
    except Exception:
        connected_clients.pop(websocket, None)

app.mount("/static", StaticFiles(directory="."), name="static")

@app.get("/")
async def root():
    return FileResponse("index.html")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
