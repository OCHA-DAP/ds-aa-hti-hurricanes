"""Listmonk send path.

Bodies are built with base64-embedded images (self-contained for
previews); at send time every data-URI is swapped for an uploaded
Listmonk media URL (Gmail clips bodies over ~100 KB).

Env switches (both default to the safe value):
- TEST_EMAIL (default True): send to the internal test list instead of
  the real distribution lists.
- DRY_RUN (default True): build everything, send nothing.
"""

import base64
import os
import re

from ocha_relay.listmonk import ListmonkClient

from src.constants import (
    LISTMONK_INFO_LIST_ID,
    LISTMONK_TEST_LIST_ID,
    LISTMONK_TRIGGER_LIST_ID,
)
from src.utils.logging import get_logger

logger = get_logger(__name__)


def _parse_bool_env(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in ("false", "0", "no")


TEST_EMAIL = _parse_bool_env("TEST_EMAIL", default=True)
DRY_RUN = _parse_bool_env("DRY_RUN", default=True)

_B64_IMG_RE = re.compile(r"data:image/png;base64,([A-Za-z0-9+/=]+)")


def resolve_list_ids(email_type: str) -> list[int]:
    """info emails -> info list; stage triggers -> trigger + info lists."""
    if TEST_EMAIL:
        return [LISTMONK_TEST_LIST_ID]
    if email_type == "info":
        return [LISTMONK_INFO_LIST_ID]
    return [LISTMONK_TRIGGER_LIST_ID, LISTMONK_INFO_LIST_ID]


def send_campaign(
    campaign_name: str, subject: str, body: str, email_type: str
) -> int | None:
    """Upload images, create the campaign, send. Returns campaign id."""
    if TEST_EMAIL:
        subject = f"[TEST] {subject}"
        campaign_name = f"[test] {campaign_name}"
    if DRY_RUN:
        logger.info(
            f"DRY_RUN: would send '{subject}' "
            f"({len(body) / 1000:.0f} kB body) to "
            f"{resolve_list_ids(email_type)}"
        )
        return None

    client = ListmonkClient.from_env()
    uploaded: dict[str, str] = {}

    def _upload_image(m: re.Match) -> str:
        b64 = m.group(1)
        if b64 not in uploaded:
            uploaded[b64] = client.upload_media(
                base64.b64decode(b64), "chart.png"
            )
        return uploaded[b64]

    body = _B64_IMG_RE.sub(_upload_image, body)
    logger.info(f"Uploaded {len(uploaded)} images to Listmonk media.")

    cid = client.create_campaign(
        name=campaign_name,
        subject=subject,
        body=body,
        list_ids=resolve_list_ids(email_type),
    )
    client.send_campaign(cid, skip_confirmation=True)
    logger.info(f"Sent campaign {cid}: {subject}")
    return cid
