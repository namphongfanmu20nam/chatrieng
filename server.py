from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import json, os
from typing import Dict
from datetime import datetime

app = FastAPI()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

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
        username = payload.get("username", "Ẩn danh").strip() or "Ẩn danh"
        connected_clients[websocket] = username

        await websocket.send_text(json.dumps({
            "type": "init",
            "username": username,
            "online": len(connected_clients)
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
                    await broadcast({
                        "type": "message",
                        "username": username,
                        "text": text,
                        "time": datetime.now().strftime("%H:%M"),
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
