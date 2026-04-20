import hmac

from fastapi import HTTPException, Request

from app.core.config import settings


async def verify_auth(request: Request) -> None:
    """Verify the request comes from an authorised marketplace or the owner.

    Checks, in order:
        1. X-RapidAPI-Proxy-Secret
        3. X-Zyla-API-Gateway-Secret
        4. X-API-Key  (master / direct access)

    If *no* secrets are configured at all the check is skipped (dev mode).
    """
    checks = [
        (settings.rapidapi_proxy_secret, "x-rapidapi-proxy-secret"),
        (settings.zyla_proxy_secret, "authorization"),
        (settings.master_api_key, "x-api-key"),
    ]

    configured = [(secret, header) for secret, header in checks if secret]

    # Development mode — no secrets configured
    if not configured:
        return

    for secret, header in configured:
        value = request.headers.get(header, "")
        # Strip "Bearer " prefix if present (e.g. Zyla sends Authorization: Bearer <secret>)
        if value.lower().startswith("bearer "):
            value = value[7:]
        if value and hmac.compare_digest(value, secret):
            return

    raise HTTPException(status_code=403, detail={"error": "Unauthorized"})
