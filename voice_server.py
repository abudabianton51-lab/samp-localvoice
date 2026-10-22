"""Standalone proximity voice relay for SA:MP; does not import SAMP Messenger.
Run: uvicorn voice_server:app --host 0.0.0.0 --port $PORT
One uvicorn worker per deployment (in-memory peer routing).
"""
import asyncio
import json
import math
import os
import re
import secrets
import time
from dataclasses import dataclass, field
from typing import Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI(title="SA:MP Local Voice", version="0.1.0")
NICK_RE = re.compile(r"^[a-zA-Z0-9_]{3,24}$")
CODE = os.environ.get("VOICE_ACCESS_CODE", "")
MAX_AUDIO = 800  # PCM 16kHz mono int16, 20ms = 640B
MAX_PEERS = 150
STALE = 2.5

@dataclass
class Peer:
    socket: WebSocket
    pid: str
    nick: str
    room: str
    position: tuple[float,float,float] = (0,0,0)
    radius: float = 20
    near: set[str] = field(default_factory=set)
    updated: float = 0
    last_audio: float = 0
    audio_bucket: int = 0
    bucket_start: float = 0
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

peers: dict[str, Peer] = {}

@app.get("/")
def home():
    return {"status":"online","name":"SA:MP Local Voice","peers":len(peers),"password_required":bool(CODE)}


def audible(sender: Peer, listener: Peer, now: float) -> bool:
    if sender.pid == listener.pid or sender.room != listener.room:
        return False
    if now - sender.updated > STALE or now - listener.updated > STALE:
        return False
    # Each SA:MP client must report that the other player is actually streamed in.
    # Avoid cross-world and cross-interior voice if positions happen to coincide.
    if listener.nick.lower() not in sender.near or sender.nick.lower() not in listener.near:
        return False
    dx = sender.position[0]-listener.position[0]
    dy = sender.position[1]-listener.position[1]
    dz = sender.position[2]-listener.position[2]
    return dx*dx+dy*dy+dz*dz <= min(sender.radius, listener.radius)**2


async def safe_send(p:Peer, content):
    try:
        async with p.send_lock:
            if isinstance(content, bytes):
                await asyncio.wait_for(p.socket.send_bytes(content), timeout=0.3)
            else:
                await asyncio.wait_for(p.socket.send_json(content), timeout=0.8)
        return True
    except (Exception, asyncio.CancelledError):
        return False


async def update_peer(p:Peer, packet:dict):
    try:
        xyz = [float(packet[k]) for k in ("x","y","z")]
        if any(not math.isfinite(v) or abs(v)>6000 for v in xyz): return
        near = packet.get("near",[])
        if not isinstance(near,list): return
        p.near = {str(n).lower() for n in near[:100] if NICK_RE.fullmatch(str(n))}
        p.position = tuple(xyz)
        p.radius = min(40.0,max(5.0,float(packet.get("radius",20))))
        p.updated = time.monotonic()
    except (TypeError,ValueError,KeyError,OverflowError):
        return


async def peers_publisher(p:Peer):
    while True:
        await asyncio.sleep(0.6)
        now = time.monotonic()
        nearby=[]
        for other in list(peers.values()):
            if audible(p,other,now):
                distance=math.dist(p.position,other.position)
                nearby.append({"id":other.pid,"nick":other.nick,"distance":round(distance,1),
                  "talking":now-other.last_audio<0.35})
        if not await safe_send(p,{"type":"peers","users":nearby[:70]}):
            return


@app.websocket('/voice')
async def voice_socket(ws:WebSocket):
    await ws.accept()
    p: Optional[Peer]=None
    publisher=None
    try:
        initial=await asyncio.wait_for(ws.receive_text(), timeout=10)
        if len(initial)>1200:
            await ws.close(code=1008);return
        hello=json.loads(initial)
        nick=str(hello.get('nick',''))
        room=str(hello.get('room',''))
        provided=str(hello.get('code',''))
        if (hello.get('type')!='hello' or not NICK_RE.fullmatch(nick) or
            not re.fullmatch(r'[a-zA-Z0-9.\-:]{4,100}',room) or
            (CODE and not secrets.compare_digest(provided,CODE)) or len(peers)>=MAX_PEERS):
            await ws.close(code=1008,reason='Invalid session or access code');return
        # Same nickname cannot join a room twice (no identity proof without game plugin).
        if any(q.room==room and q.nick.lower()==nick.lower() for q in peers.values()):
            await ws.close(code=1008,reason='Nickname already connected');return
        p=Peer(socket=ws,pid=secrets.token_hex(16),nick=nick,room=room)
        peers[p.pid]=p
        await safe_send(p,{"type":"welcome","id":p.pid})
        publisher=asyncio.create_task(peers_publisher(p))
        while True:
            msg=await ws.receive()
            if msg.get('type')=='websocket.disconnect':break
            now=time.monotonic()
            raw=msg.get('bytes')
            if raw is not None:
                if len(raw)!=640 or now-p.updated>STALE:continue
                # at most ~55 20ms frames/second/client
                if now-p.bucket_start>=1:
                    p.bucket_start=now;p.audio_bucket=0
                p.audio_bucket+=1
                if p.audio_bucket>55:continue
                p.last_audio=now
                packet=bytes.fromhex(p.pid)+raw
                for dst in list(peers.values()):
                    if audible(p,dst,now):
                        await safe_send(dst,packet)
            elif msg.get('text'):
                data=msg['text']
                if len(data)>5000:continue
                try:
                    packet=json.loads(data)
                except (ValueError,TypeError):continue
                if isinstance(packet,dict) and packet.get('type')=='state':
                    await update_peer(p,packet)
    except (WebSocketDisconnect,TimeoutError,ValueError,RuntimeError):
        pass
    finally:
        if publisher:
            publisher.cancel()
        if p is not None:
            peers.pop(p.pid,None)
        try:
            await ws.close()
        except Exception:
            pass
