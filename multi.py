import json, time, asyncio, hashlib, base64, threading
import httpx
import http.server
from datetime import datetime, timezone
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from accounts import ACCOUNTS

# ---------------- RENDER HEALTH CHECK ---------------- #
# This satisfies Render's requirement for a web service to listen on a port.
def run_health_server():
    class HealthHandler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Bot is alive")

    server_address = ('0.0.0.0', 10000)
    httpd = http.server.HTTPServer(server_address, HealthHandler)
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Health server started on port 10000")
    httpd.serve_forever()

# ---------------- CONFIG & CONSTANTS ---------------- #
REQUEST_TIMEOUT = 30
SALT = "j8n5HxYA0ZVF"
ENCRYPTION_KEY = "6fbJwIfT6ibAkZo1VVKlKVl8M2Vb7GSs"
FAIRBID_BURST = 50      
FAIRBID_DELAY = 0  

# ---------------- LOG ---------------- #
def log(msg, name=None):
    prefix = f"[{name}] " if name else ""
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {prefix}{msg}", flush=True)

# ---------------- CLIENT ---------------- #
async def create_client():
    return httpx.AsyncClient(
        http2=True,
        timeout=httpx.Timeout(REQUEST_TIMEOUT),
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=50),
        headers={"User-Agent": "Mozilla/5.0 (Android)"}
    )

# ---------------- CONFIG ---------------- #
async def load_config(client, url):
    r = await client.get(url)
    r.raise_for_status()
    j = r.json()
    return {
        "user_id": j["client_params"]["publisher_supplied_user_id"],
        "payload": json.dumps(j, separators=(",", ":"))
    }

# ---------------- AUTH ---------------- #
async def get_id_token(client, firebase_key, refresh_token):
    r = await client.post(
        f"https://securetoken.googleapis.com/v1/token?key={firebase_key}",
        data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    r.raise_for_status()
    j = r.json()
    return j["id_token"], j["user_id"], int(j["expires_in"])

class TokenManager:
    def __init__(self, firebase_key, refresh_token):
        self.firebase_key = firebase_key
        self.refresh_token = refresh_token
        self.token = None
        self.uid = None
        self.expiry = 0

    async def get(self, client):
        if not self.token or time.time() >= self.expiry:
            self.token, self.uid, ttl = await get_id_token(client, self.firebase_key, self.refresh_token)
            self.expiry = time.time() + ttl - 30
        return self.token, self.uid

# ---------------- HASH ---------------- #
_last_ts = 0
def build_hash_payload(user_id, url):
    global _last_ts
    now = int(time.time())
    if now <= _last_ts:
        now = _last_ts + 1
    _last_ts = now
    ts = datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    raw = f"{url}{ts}{SALT}"
    return json.dumps({
        "user_id": user_id,
        "timestamp": now,
        "hash_value": hashlib.sha512(raw.encode()).hexdigest()
    }, separators=(",", ":"))

# ---------------- ENCRYPT ---------------- #
def encrypt_offer(offer_id):
    key = hashlib.sha256(ENCRYPTION_KEY.encode()).digest()
    raw = json.dumps({"offerId": offer_id}, separators=(",", ":")).encode()
    enc = AES.new(key, AES.MODE_ECB).encrypt(pad(raw, AES.block_size))
    return {"data": {"data": base64.b64encode(enc).decode()}}

# ---------------- FIRESTORE ---------------- #
async def get_super_offer(client, token, project_id, uid):
    r = await client.post(
        f"https://firestore.googleapis.com/v1/projects/{project_id}/databases/(default)/documents/users/{uid}:runQuery",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "structuredQuery": {
                "from": [{"collectionId": "superOffers"}],
                "where": {
                    "fieldFilter": {
                        "field": {"fieldPath": "status"},
                        "op": "NOT_EQUAL",
                        "value": {"stringValue": "COMPLETED"}
                    }
                },
                "limit": 1
            }
        }
    )
    for item in r.json():
        if "document" in item:
            f = item["document"]["fields"]
            return {
                "offerId": f["offerId"]["stringValue"],
                "fees": int(f["fees"]["integerValue"])
            }
    return None

async def get_boosts(client, token, project_id, uid):
    r = await client.get(
        f"https://firestore.googleapis.com/v1/projects/{project_id}/databases/(default)/documents/users/{uid}?mask.fieldPaths=boosts",
        headers={"Authorization": f"Bearer {token}"}
    )
    return int(r.json().get("fields", {}).get("boosts", {}).get("integerValue", 0))

# ---------------- FAIRBID ---------------- #
async def run_fairbid(client, acc, cfg):
    try:
        r = await client.post(f"{acc['BASE_URL']}?spotId={acc['SPOT_ID']}", content=cfg["payload"])
        t = r.text
        tasks = []
        if 'impression":"' in t:
            imp = t.split('impression":"')[1].split('"')[0]
            tasks.append(client.get(imp))
        if 'completion":"' in t:
            url = t.split('completion":"')[1].split('"')[0]
            payload = build_hash_payload(cfg["user_id"], url)
            tasks.append(client.post(url, content=payload))
        if tasks:
            await asyncio.gather(*tasks)
    except Exception:
        pass

# ---------------- FUNCTIONS ---------------- #
async def call_fn(client, token, project_id, name, offer_id):
    r = await client.post(
        f"https://us-central1-{project_id}.cloudfunctions.net/{name}",
        headers={"Authorization": f"Bearer {token}"},
        json=encrypt_offer(offer_id)
    )
    return r.json()

# ---------------- BOT LOOP ---------------- #
async def bot_loop(acc):
    client = await create_client()
    try:
        cfg = await load_config(client, acc["JSON_URL"])
        tm = TokenManager(acc["FIREBASE_KEY"], acc["REFRESH_TOKEN"])
        log("STARTED", acc["NAME"])
        while True:
            try:
                token, uid = await tm.get(client)
                offer = await get_super_offer(client, token, acc["PROJECT_ID"], uid)
                if not offer:
                    await asyncio.sleep(10)
                    continue
                log(f"OFFER FOUND | ID={offer['offerId']} | FEES={offer['fees']}", acc["NAME"])
                target = offer["fees"] + 1
                while True:
                    boosts = await get_boosts(client, token, acc["PROJECT_ID"], uid)
                    log(f"BOOSTS {boosts}/{target}", acc["NAME"])
                    if boosts >= target: break
                    await asyncio.gather(*(run_fairbid(client, acc, cfg) for _ in range(FAIRBID_BURST)))
                    await asyncio.sleep(FAIRBID_DELAY)
                await call_fn(client, token, acc["PROJECT_ID"], "superOffer_unlock", offer["offerId"])
                await call_fn(client, token, acc["PROJECT_ID"], "superOffer_claim", offer["offerId"])
                await asyncio.sleep(5)
            except Exception as e:
                log(f"Error: {e}", acc["NAME"])
                await asyncio.sleep(10)
    finally:
        await client.aclose()

# ---------------- MAIN ---------------- #
async def main():
    log("Royal Cash Bot - Multi Account Starting")
    # Start the web server for Render health checks
    threading.Thread(target=run_health_server, daemon=True).start()
    # Start the accounts
    await asyncio.gather(*(bot_loop(a) for a in ACCOUNTS))

if __name__ == "__main__":
    asyncio.run(main())
                
