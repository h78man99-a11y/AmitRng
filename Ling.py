import json
import hashlib
import asyncio
import httpx
import time
import base64
import os
from datetime import datetime, timezone

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from aiohttp import web

from accounts import ACCOUNTS

# ================= GLOBAL CONFIG =================

REQUEST_TIMEOUT = 30
SALT = "j8n5HxYA0ZVF"
ENCRYPTION_KEY = "6fbJwIfT6ibAkZo1VVKlKVl8M2Vb7GSs"

FORCE_REFRESH_INTERVAL = 30 * 60  # 30 minutes

_last_timestamp = 0
_processed_offers = set()

# ================= LOG =================

def log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

# ================= HTTP CLIENT =================

async def create_client():
    return httpx.AsyncClient(
        headers={
            "User-Agent": "Mozilla/5.0 (Android)",
            "Accept": "application/json",
        },
        timeout=httpx.Timeout(REQUEST_TIMEOUT),
    )

# ================= TOKEN =================

async def refresh_token(client, api_key, refresh_token):
    r = await client.post(
        f"https://securetoken.googleapis.com/v1/token?key={api_key}",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    r.raise_for_status()
    j = r.json()
    return j["id_token"], j["user_id"], int(j["expires_in"])

class TokenManager:
    def __init__(self, name):
        self.name = name
        self.token = None
        self.uid = None
        self.expiry = 0
        self.last_refresh = 0

    async def get(self, client, acc):
        now = time.time()

        if (
            not self.token
            or now >= self.expiry
            or now - self.last_refresh >= FORCE_REFRESH_INTERVAL
        ):
            self.token, self.uid, ttl = await refresh_token(
                client,
                acc["FIREBASE_KEY"],
                acc["REFRESH_TOKEN"],
            )
            self.expiry = now + ttl - 30
            self.last_refresh = now

            log(f"[AUTH][{self.name}] Token refreshed")

        return self.token, self.uid

# ================= CONFIG =================

async def load_config(client, url):
    r = await client.get(url)
    r.raise_for_status()
    data = r.json()
    user_id = data["client_params"]["publisher_supplied_user_id"]

    return {
        "user_id": user_id,
        "payload": json.dumps(data, separators=(",", ":")),
    }

# ================= HASH =================

def build_hash_payload(user_id, url):
    global _last_timestamp

    now = int(time.time())
    if now <= _last_timestamp:
        now = _last_timestamp + 1
    _last_timestamp = now

    ts = datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    raw = f"{url}{ts}{SALT}"

    return json.dumps(
        {
            "user_id": user_id,
            "timestamp": now,
            "hash_value": hashlib.sha512(raw.encode()).hexdigest(),
        },
        separators=(",", ":"),
    )

# ================= ENCRYPT =================

def encrypt_offer(offer_id):
    key = hashlib.sha256(ENCRYPTION_KEY.encode()).digest()
    raw = json.dumps({"offerId": offer_id}, separators=(",", ":")).encode()
    cipher = AES.new(key, AES.MODE_ECB)
    enc = cipher.encrypt(pad(raw, AES.block_size))
    return {"data": {"data": base64.b64encode(enc).decode()}}

# ================= FIRESTORE =================

async def get_super_offer(client, acc, token, uid):
    r = await client.post(
        f"https://firestore.googleapis.com/v1/projects/{acc['PROJECT_ID']}/databases/(default)/documents/users/{uid}:runQuery",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "structuredQuery": {
                "from": [{"collectionId": "superOffers"}],
                "where": {
                    "fieldFilter": {
                        "field": {"fieldPath": "status"},
                        "op": "NOT_EQUAL",
                        "value": {"stringValue": "COMPLETED"},
                    }
                },
                "limit": 1,
            }
        },
    )

    for item in r.json():
        doc = item.get("document")
        if not doc:
            continue

        f = doc["fields"]
        offer_id = f["offerId"]["stringValue"]

        if offer_id in _processed_offers:
            return None

        return {
            "offerId": offer_id,
            "fees": int(f.get("fees", {}).get("integerValue", 0)),
        }

    return None

async def get_boosts(client, acc, token, uid):
    r = await client.get(
        f"https://firestore.googleapis.com/v1/projects/{acc['PROJECT_ID']}/databases/(default)/documents/users/{uid}?mask.fieldPaths=boosts",
        headers={"Authorization": f"Bearer {token}"},
    )
    return int(r.json().get("fields", {}).get("boosts", {}).get("integerValue", 0))

# ================= FAIRBID =================

async def run_fairbid(client, acc, cfg):
    r = await client.post(
        f"{acc['BASE_URL']}?spotId={acc['SPOT_ID']}",
        content=cfg["payload"],
    )

    text = r.text

    if 'impression":"' in text:
        imp = text.split('impression":"')[1].split('"')[0]
        await client.get(imp)

    if 'completion":"' in text:
        comp = text.split('completion":"')[1].split('"')[0]
        await client.post(
            comp,
            content=build_hash_payload(cfg["user_id"], comp),
        )

# ================= CLOUD FUNCTIONS =================

async def call_fn(client, acc, token, name, offer_id):
    r = await client.post(
        f"https://us-central1-{acc['PROJECT_ID']}.cloudfunctions.net/{name}",
        headers={"Authorization": f"Bearer {token}"},
        json=encrypt_offer(offer_id),
    )
    return r.json()

async def unlock_and_claim(client, acc, token, offer_id):
    u = await call_fn(client, acc, token, "superOffer_unlock", offer_id)
    if u.get("result", {}).get("status") != "SUCCESS":
        return False

    c = await call_fn(client, acc, token, "superOffer_claim", offer_id)
    return c.get("result", {}).get("status") == "SUCCESS"

# ================= ACCOUNT LOOP =================

async def run_account(acc):
    client = await create_client()
    tm = TokenManager(acc["NAME"])
    cfg = await load_config(client, acc["JSON_URL"])

    log(f"[START] {acc['NAME']}")

    while True:
        token, uid = await tm.get(client, acc)

        offer = await get_super_offer(client, acc, token, uid)
        if not offer:
            await asyncio.sleep(5)
            continue

        target = offer["fees"] + 1

        while await get_boosts(client, acc, token, uid) < target:
            await run_fairbid(client, acc, cfg)
            await asyncio.sleep(0.5)

        if await unlock_and_claim(client, acc, token, offer["offerId"]):
            log(f"[CLAIMED][{acc['NAME']}] {offer['offerId']}")
            _processed_offers.add(offer["offerId"])

        await asyncio.sleep(2)

# ================= WEB SERVER =================

async def health(request):
    return web.Response(text="Royal Bot running")

async def start_server():
    app = web.Application()
    app.router.add_get("/", health)

    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    log(f"[WEB] Listening on port {port}")

# ================= MAIN =================

async def main():
    await start_server()
    await asyncio.gather(*(run_account(acc) for acc in ACCOUNTS))

if __name__ == "__main__":
    asyncio.run(main())
