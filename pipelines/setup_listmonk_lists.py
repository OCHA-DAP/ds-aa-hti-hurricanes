"""Provision the Listmonk lists for Haiti hurricane AA monitoring emails.

Idempotent: discovers existing lists by tag before creating anything.
Uses the admin credential pair (list creation is an admin-only operation).

Lists created (private, single opt-in):
- info emails ("informations"): every advisory while a storm is relevant
- trigger emails ("déclencheurs"): one per storm per stage when a stage fires

Run: python pipelines/setup_listmonk_lists.py [--dry-run]
"""

import argparse
import logging
import os

import requests
from ocha_relay.listmonk import ListmonkClient

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

PROJECT_TAG = "ds-aa-hti-hurricanes"

LISTS = {
    "role:info": "AA Haïti ouragans - informations",
    "role:trigger": "AA Haïti ouragans - déclencheurs",
}


def admin_client() -> ListmonkClient:
    return ListmonkClient(
        base_url=os.environ["DSCI_LISTMONK_BASE_URL"].rstrip("/"),
        username=os.environ["DSCI_LISTMONK_ADMIN_API_USERNAME"],
        password=os.environ["DSCI_LISTMONK_ADMIN_API_KEY"],
    )


def get_list_ids(client: ListmonkClient | None = None) -> dict:
    """Return {role_tag: list_id} for the project's existing lists."""
    client = client or ListmonkClient.from_env()
    ids = {}
    for lst in client.fetch_all_lists(tag=PROJECT_TAG):
        for tag in lst.get("tags", []):
            if tag in LISTS:
                ids[tag] = lst["id"]
    return ids


def add_subscriber(
    client: ListmonkClient, email: str, name: str, list_ids: list[int]
):
    """Add (or update) a subscriber on the given lists.

    ocha-relay v0.3.0 has no subscriber-write method, so hit the API
    directly. preconfirm avoids opt-in emails for these private lists.
    """
    resp = requests.post(
        f"{client.base_url}/subscribers",
        auth=(client.username, client.password),
        json={
            "email": email,
            "name": name,
            "status": "enabled",
            "lists": list_ids,
            "preconfirm_subscriptions": True,
        },
        timeout=client.timeout,
    )
    if resp.status_code == 409:
        logger.info(f"{email} already exists; adding to lists by id")
        lookup = requests.get(
            f"{client.base_url}/subscribers",
            auth=(client.username, client.password),
            params={"query": f"subscribers.email = '{email}'"},
            timeout=client.timeout,
        )
        lookup.raise_for_status()
        sub_id = lookup.json()["data"]["results"][0]["id"]
        resp = requests.put(
            f"{client.base_url}/subscribers/lists",
            auth=(client.username, client.password),
            json={
                "ids": [sub_id],
                "action": "add",
                "target_list_ids": list_ids,
                "status": "confirmed",
            },
            timeout=client.timeout,
        )
    resp.raise_for_status()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--subscribe",
        nargs=2,
        metavar=("EMAIL", "NAME"),
        action="append",
        default=[],
        help="subscribe EMAIL NAME to both lists (repeatable)",
    )
    args = parser.parse_args()

    client = admin_client()
    existing = get_list_ids(client)
    for role_tag, name in LISTS.items():
        if role_tag in existing:
            logger.info(
                f"List already exists for {role_tag}: id={existing[role_tag]}"
            )
            continue
        if args.dry_run:
            logger.info(f"[dry-run] would create list '{name}' ({role_tag})")
            continue
        list_id = client.create_list(
            name=name,
            list_type="private",
            optin="single",
            tags=[PROJECT_TAG, role_tag],
        )
        existing[role_tag] = list_id
        logger.info(f"Created list '{name}' ({role_tag}): id={list_id}")

    for email, name in args.subscribe:
        if args.dry_run:
            logger.info(f"[dry-run] would subscribe {email}")
            continue
        add_subscriber(client, email, name, list(existing.values()))
        logger.info(f"Subscribed {email} to {sorted(existing.values())}")
