#!/usr/bin/env python3
"""
ClickUp ETL — Pull tasks from ClickUp into the client intelligence database.

Usage:
  python3 pull_clickup_data.py                          # All clients
  python3 pull_clickup_data.py --client 7123434733      # Single client
  python3 pull_clickup_data.py --backfill               # Full historical pull

Pulls tasks from each client's ClickUp list/folder, syncs status, assignees,
due dates, and task type into the client_tasks table.

Requires:
  - CLICKUP_API_KEY env var (personal API token, pk_...)
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import urllib.request
import urllib.error

# ── Config ──────────────────────────────────────────────────

DB_CONFIG = {
    "dbname": "blueprint",
    "user": "blueprint",
    "host": "localhost",
    "port": 5432,
}

CLICKUP_BASE = "https://api.clickup.com/api/v2"
CLICKUP_API_KEY = os.environ.get("CLICKUP_API_KEY", "")
RATE_LIMIT_DELAY = 0.6  # stay under 100 req/min

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('clickup-sync')


# ── Database ────────────────────────────────────────────────

def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


# ── ClickUp API ─────────────────────────────────────────────

def clickup_request(endpoint, params=None, retries=3):
    """Make a ClickUp API request."""
    url = f"{CLICKUP_BASE}{endpoint}"
    if params:
        qs = "&".join(f"{k}={urllib.request.quote(str(v))}" for k, v in params.items())
        url = f"{url}?{qs}"

    req = urllib.request.Request(url, headers={
        "Authorization": CLICKUP_API_KEY,
        "Content-Type": "application/json",
    })

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 429 and retries > 0:
            retry_after = int(e.headers.get("X-RateLimit-Reset", 60)) - int(time.time())
            wait = max(retry_after, 5)
            log.warning(f"Rate limited (429), waiting {wait}s...")
            time.sleep(wait)
            return clickup_request(endpoint, params, retries - 1)
        body = e.read().decode()[:200]
        log.error(f"HTTP error {e.code}: {body}")
        return None
    except Exception as e:
        log.error(f"Request error: {e}")
        return None


def get_tasks_for_list(list_id, page=0, include_closed=False):
    """Get tasks from a ClickUp list with pagination."""
    params = {
        "page": str(page),
        "include_closed": "true" if include_closed else "false",
        "subtasks": "true",
    }
    return clickup_request(f"/list/{list_id}/task", params)


def get_tasks_for_folder(folder_id):
    """Get all lists in a folder, then all tasks from each list."""
    lists_data = clickup_request(f"/folder/{folder_id}/list")
    if not lists_data:
        return []

    all_tasks = []
    for lst in lists_data.get("lists", []):
        list_id = lst["id"]
        list_name = lst.get("name", "")
        log.info(f"    List: {list_name} ({list_id})")

        page = 0
        while True:
            data = get_tasks_for_list(list_id, page=page, include_closed=True)
            if not data:
                break
            tasks = data.get("tasks", [])
            all_tasks.extend(tasks)
            if len(tasks) < 100:
                break
            page += 1
            time.sleep(RATE_LIMIT_DELAY)

        time.sleep(RATE_LIMIT_DELAY)

    return all_tasks


def get_tasks_for_space(space_id):
    """Get all folders in a space, then all tasks."""
    # First get folderless lists
    lists_data = clickup_request(f"/space/{space_id}/list")
    all_tasks = []

    if lists_data:
        for lst in lists_data.get("lists", []):
            list_id = lst["id"]
            list_name = lst.get("name", "")
            log.info(f"    List: {list_name} ({list_id})")

            page = 0
            while True:
                data = get_tasks_for_list(list_id, page=page, include_closed=True)
                if not data:
                    break
                tasks = data.get("tasks", [])
                all_tasks.extend(tasks)
                if len(tasks) < 100:
                    break
                page += 1
                time.sleep(RATE_LIMIT_DELAY)
            time.sleep(RATE_LIMIT_DELAY)

    # Then get folders
    folders_data = clickup_request(f"/space/{space_id}/folder")
    if folders_data:
        for folder in folders_data.get("folders", []):
            folder_id = folder["id"]
            folder_name = folder.get("name", "")
            log.info(f"  Folder: {folder_name} ({folder_id})")
            folder_tasks = get_tasks_for_folder(folder_id)
            all_tasks.extend(folder_tasks)

    return all_tasks


# ── Task Processing ─────────────────────────────────────────

STATUS_MAP = {
    "to do": "todo",
    "open": "todo",
    "in progress": "in_progress",
    "in review": "in_progress",
    "complete": "done",
    "closed": "done",
    "done": "done",
    "blocked": "blocked",
    "cancelled": "cancelled",
}

TASK_TYPE_KEYWORDS = {
    "routine": re.compile(r'month\s*\d+|milestone|quarterly|monthly|recurring|routine', re.I),
    "website_edit": re.compile(r'website|landing\s*page|webflow|web\s*edit|web\s*update|page\s*edit', re.I),
}


def classify_task_type(task_name, tags):
    """Determine task type from name and tags."""
    combined = task_name + " " + " ".join(tags)
    for task_type, pattern in TASK_TYPE_KEYWORDS.items():
        if pattern.search(combined):
            return task_type
    return "custom"


def ms_to_date(ms_str):
    """Convert millisecond timestamp to date string."""
    if not ms_str:
        return None
    try:
        ts = int(ms_str) / 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc).date()
    except (ValueError, TypeError, OSError):
        return None


def upsert_task(cur, customer_id, task):
    """Insert or update a single ClickUp task."""
    task_id = task.get("id")
    name = task.get("name", "")
    description = task.get("description", "")
    status_raw = task.get("status", {}).get("status", "").lower()
    status = STATUS_MAP.get(status_raw, "todo")

    assignees = task.get("assignees", [])
    assigned_to = assignees[0].get("username", assignees[0].get("email", "")) if assignees else None

    due_date = ms_to_date(task.get("due_date"))
    date_closed = ms_to_date(task.get("date_closed"))

    tags = [t.get("name", "") for t in task.get("tags", [])]
    priority_raw = task.get("priority")
    priority_map = {"1": "urgent", "2": "high", "3": "normal", "4": "low"}
    priority = priority_map.get(str(priority_raw.get("id")) if priority_raw else "", "normal")

    task_type = classify_task_type(name, tags)

    cur.execute("""
        INSERT INTO client_tasks (
            customer_id, clickup_task_id, task_type, title, description,
            status, assigned_to, due_date, completed_date, priority, tags,
            updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (clickup_task_id) DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            status = EXCLUDED.status,
            assigned_to = EXCLUDED.assigned_to,
            due_date = EXCLUDED.due_date,
            completed_date = EXCLUDED.completed_date,
            priority = EXCLUDED.priority,
            task_type = EXCLUDED.task_type,
            tags = EXCLUDED.tags,
            updated_at = NOW()
    """, (customer_id, task_id, task_type, name, description[:2000] if description else None,
          status, assigned_to, due_date, date_closed, priority, tags))


def pull_client_tasks(conn, customer_id, client_name, space_id=None, folder_id=None, list_id=None):
    """Pull tasks for a single client from ClickUp."""
    stats = {"tasks": 0, "errors": []}

    if list_id:
        log.info(f"  Pulling from list {list_id}")
        all_tasks = []
        page = 0
        while True:
            data = get_tasks_for_list(list_id, page=page, include_closed=True)
            if not data:
                break
            tasks = data.get("tasks", [])
            all_tasks.extend(tasks)
            if len(tasks) < 100:
                break
            page += 1
            time.sleep(RATE_LIMIT_DELAY)
    elif folder_id:
        log.info(f"  Pulling from folder {folder_id}")
        all_tasks = get_tasks_for_folder(folder_id)
    elif space_id:
        log.info(f"  Pulling from space {space_id}")
        all_tasks = get_tasks_for_space(space_id)
    else:
        log.warning(f"  No ClickUp ID configured for {client_name}")
        return stats

    log.info(f"  Found {len(all_tasks)} tasks")

    with conn.cursor() as cur:
        for task in all_tasks:
            try:
                cur.execute("SAVEPOINT task_sp")
                upsert_task(cur, customer_id, task)
                stats["tasks"] += 1
                cur.execute("RELEASE SAVEPOINT task_sp")
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT task_sp")
                stats["errors"].append(f"task {task.get('id')}: {e}")
                log.warning(f"  Error: {e}")

    conn.commit()
    return stats


# ── Main ────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Pull ClickUp tasks for client intelligence")
    parser.add_argument('--client', type=str, help='Pull only one customer_id')
    parser.add_argument('--backfill', action='store_true', help='Include closed/done tasks')
    args = parser.parse_args()

    if not CLICKUP_API_KEY:
        log.error("CLICKUP_API_KEY environment variable required")
        sys.exit(1)

    conn = get_db()

    try:
        with conn.cursor() as cur:
            query = """
                SELECT c.customer_id, c.name,
                       cp.clickup_space_id, cp.clickup_folder_id, cp.clickup_list_id
                FROM clients c
                JOIN client_profiles cp ON cp.customer_id = c.customer_id
                WHERE c.status = 'active'
                  AND (cp.clickup_space_id IS NOT NULL
                       OR cp.clickup_folder_id IS NOT NULL
                       OR cp.clickup_list_id IS NOT NULL)
            """
            params = []
            if args.client:
                query += " AND c.customer_id = %s"
                params.append(int(args.client))
            query += " ORDER BY c.name"
            cur.execute(query, params)
            clients = cur.fetchall()

        if not clients:
            log.warning("No clients with ClickUp IDs found in client_profiles.")
            log.info("Update client_profiles with clickup_space_id, clickup_folder_id, or clickup_list_id.")
            return

        log.info(f"Pulling ClickUp tasks for {len(clients)} clients...")

        # Log the pull
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO client_intelligence_pull_log (source, started_at)
                VALUES ('clickup', NOW()) RETURNING id
            """)
            pull_id = cur.fetchone()[0]
        conn.commit()

        total_tasks = 0
        total_errors = 0

        for customer_id, name, space_id, folder_id, list_id in clients:
            log.info(f"\n{'='*60}")
            log.info(f"Client: {name}")
            log.info(f"{'='*60}")

            stats = pull_client_tasks(conn, customer_id, name, space_id, folder_id, list_id)
            total_tasks += stats["tasks"]
            total_errors += len(stats["errors"])

            log.info(f"  Tasks synced: {stats['tasks']}, Errors: {len(stats['errors'])}")
            time.sleep(RATE_LIMIT_DELAY)

        # Update pull log
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE client_intelligence_pull_log
                SET finished_at = NOW(),
                    records_processed = %s,
                    status = 'completed'
                WHERE id = %s
            """, (total_tasks, pull_id))
        conn.commit()

        log.info(f"\n{'='*60}")
        log.info(f"DONE — {total_tasks} tasks, {total_errors} errors")
        log.info(f"{'='*60}")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
