import gzip
import json
import os
from dataclasses import dataclass

import requests


CF_API_URL = os.environ.get("CF_API_URL", "").rstrip("/")
CF_API_TOKEN = os.environ.get("CF_API_TOKEN", "")

if not CF_API_URL or not CF_API_TOKEN:
    raise RuntimeError("Missing CF_API_URL or CF_API_TOKEN")


def _headers():
    return {
        "Authorization": f"Bearer {CF_API_TOKEN}"
    }


def put_json(name, value):
    raw = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":")
    ).encode("utf-8")

    body = gzip.compress(raw, compresslevel=6)

    r = requests.put(
        f"{CF_API_URL}/internal/data/{name}",
        data=body,
        headers={
            **_headers(),
            "Content-Type": "application/json",
            "Content-Encoding": "gzip",
        },
        timeout=120,
    )

    r.raise_for_status()


def get_json(name, default=None):
    r = requests.get(
        f"{CF_API_URL}/data/{name}",
        headers=_headers(),
        timeout=120,
    )

    if r.status_code == 404:
        return default

    r.raise_for_status()

    # requests automatically decompresses HTTP gzip responses.
    # Therefore r.content is already the decompressed JSON body.
    raw = r.content

    return json.loads(raw.decode("utf-8"))


STATE_FILE = "state_maps.json.gz"

STATE_TABLES = {
    "awb_last_seen": (
        "awb_number",
        "item_last_updated",
    ),
    "rdc_last_seen": (
        "awb_number",
        "rdc_time",
    ),
    "at_dock_last_seen": (
        "awb_number",
        "at_dock_time",
        "bin_level",
    ),
    "pfc_first_seen": (
        "awb_number",
        "first_seen_at",
        "pendency_type",
    ),
}


class _StateStore:
    def __init__(self):
        self.loaded = False
        self.dirty = False
        self.data = {}

    def load(self):
        if self.loaded:
            return

        raw = get_json(STATE_FILE, {}) or {}

        self.data = raw if isinstance(raw, dict) else {}
        self.loaded = True

    def rows(self, table):
        self.load()

        return list(
            (self.data.get(table) or {}).values()
        )

    def upsert(self, table, rows, ignore=False):
        self.load()

        bucket = self.data.setdefault(table, {})
        key = STATE_TABLES[table][0]

        if not isinstance(rows, list):
            rows = [rows]

        for r in rows:
            k = str(r.get(key) or "")

            if not k:
                continue

            if ignore and k in bucket:
                continue

            bucket[k] = r

        self.dirty = True

    def delete(self, table, filters):
        self.load()

        bucket = self.data.setdefault(table, {})

        rows = list(bucket.values())
        keep = []

        for r in rows:
            if _local_filter([r], filters):
                continue

            keep.append(r)

        self.data[table] = {
            str(r[STATE_TABLES[table][0]]): r
            for r in keep
        }

        self.dirty = True

    def flush(self):
        if self.dirty:
            put_json(
                STATE_FILE,
                self.data
            )

            self.dirty = False


_STATE = _StateStore()

import atexit

atexit.register(_STATE.flush)


def query(
    table,
    select="*",
    filters=None,
    order=None,
    limit=5000,
    offset=0,
):
    # Large current snapshots and persistent state maps
    # live in Workers KV.

    if table == "audit_master":
        rows = get_json(
            "audit_master.json.gz",
            []
        ) or []

        rows = _local_filter(
            rows,
            filters or []
        )

    elif table in STATE_TABLES:
        rows = _STATE.rows(table)

        rows = _local_filter(
            rows,
            filters or []
        )

        if order:
            rows.sort(
                key=lambda r: (
                    r.get(order["column"]) is None,
                    r.get(order["column"]),
                ),
                reverse=not order.get(
                    "ascending",
                    True
                ),
            )

        return rows[offset:offset + limit]

    payload = {
        "table": table,
        "select": select,
        "filters": filters or [],
        "order": order,
        "limit": limit,
        "offset": offset,
    }

    r = requests.post(
        f"{CF_API_URL}/internal/query",
        json=payload,
        headers=_headers(),
        timeout=120,
    )

    r.raise_for_status()

    return r.json().get("data", [])


def _local_filter(rows, filters):
    def one(row, f):
        v = row.get(f["column"])
        op = f["op"]
        x = f.get("value")

        if op == "in":
            return v in x

        if op == "is":
            return (
                v is None
                if x is None
                else v is not None
            )

        if op in ("=", "!="):
            return (
                v == x
                if op == "="
                else v != x
            )

        # ISO strings compare lexically.
        # This is intentional for timestamp strings.
        if op in (">", ">=", "<", "<="):
            if v is None:
                return False

            return {
                ">": v > x,
                ">=": v >= x,
                "<": v < x,
                "<=": v <= x,
            }[op]

        return False

    return [
        r
        for r in rows
        if all(
            one(r, f)
            for f in filters
        )
    ]


@dataclass
class Response:
    data: list


class Query:
    def __init__(self, client, table):
        self.client = client
        self.table_name = table

        self._select = "*"
        self._filters = []
        self._order = None
        self._limit = 5000
        self._offset = 0

        self._action = None
        self._values = None
        self._on_conflict = None

    def select(self, columns="*"):
        self._select = columns
        return self

    def eq(self, c, v):
        self._filters.append({
            "column": c,
            "op": "=",
            "value": v,
        })
        return self

    def neq(self, c, v):
        self._filters.append({
            "column": c,
            "op": "!=",
            "value": v,
        })
        return self

    def gt(self, c, v):
        self._filters.append({
            "column": c,
            "op": ">",
            "value": v,
        })
        return self

    def gte(self, c, v):
        self._filters.append({
            "column": c,
            "op": ">=",
            "value": v,
        })
        return self

    def lt(self, c, v):
        self._filters.append({
            "column": c,
            "op": "<",
            "value": v,
        })
        return self

    def lte(self, c, v):
        self._filters.append({
            "column": c,
            "op": "<=",
            "value": v,
        })
        return self

    def in_(self, c, v):
        self._filters.append({
            "column": c,
            "op": "in",
            "value": list(v),
        })
        return self

    def is_(self, c, v):
        self._filters.append({
            "column": c,
            "op": "is",
            "value": (
                None
                if str(v).lower() == "null"
                else True
            ),
        })
        return self

    def order(self, c, ascending=True):
        self._order = {
            "column": c,
            "ascending": ascending,
        }
        return self

    def range(self, start, end):
        self._offset = start
        self._limit = end - start + 1
        return self

    def limit(self, n):
        self._limit = n
        return self

    def insert(self, values):
        self._action = "insert"
        self._values = values
        return self

    def upsert(
        self,
        values,
        on_conflict=None,
        ignore_duplicates=False,
    ):
        self._action = "upsert"
        self._values = values
        self._on_conflict = on_conflict
        return self

    def update(self, values):
        self._action = "update"
        self._values = values
        return self

    def delete(self):
        self._action = "delete"
        return self

    def execute(self):
        # Persistent state tables live in KV.
        if self.table_name in STATE_TABLES and self._action:

            if self._action == "upsert":
                _STATE.upsert(
                    self.table_name,
                    self._values,
                    ignore=False,
                )

                return Response(
                    self._values
                    if isinstance(self._values, list)
                    else [self._values]
                )

            if self._action == "insert":
                _STATE.upsert(
                    self.table_name,
                    self._values,
                    ignore=False,
                )

                return Response(
                    self._values
                    if isinstance(self._values, list)
                    else [self._values]
                )

            if self._action == "delete":
                _STATE.delete(
                    self.table_name,
                    self._filters,
                )

                return Response([])

            if self._action == "update":
                rows = _STATE.rows(
                    self.table_name
                )

                key = STATE_TABLES[
                    self.table_name
                ][0]

                for r in rows:
                    if _local_filter(
                        [r],
                        self._filters
                    ):
                        r.update(
                            self._values or {}
                        )

                _STATE.dirty = True

                return Response([])

        # Normal D1-backed tables.
        if self._action:
            payload = {
                "table": self.table_name,
                "action": self._action,
                "values": self._values,
                "filters": self._filters,
            }

            r = requests.post(
                f"{CF_API_URL}/internal/mutate",
                json=payload,
                headers=_headers(),
                timeout=120,
            )

            if r.status_code >= 400:
                try:
                    err = r.json()

                    e = RuntimeError(
                        err.get(
                            "error",
                            "mutation failed"
                        )
                    )

                    e.code = err.get("code")

                    raise e

                except ValueError:
                    r.raise_for_status()

            return Response(
                r.json().get(
                    "data",
                    []
                )
            )

        return Response(
            query(
                self.table_name,
                self._select,
                self._filters,
                self._order,
                self._limit,
                self._offset,
            )
        )


class CFStore:
    def table(self, name):
        return Query(self, name)

    def rpc(self, *args, **kwargs):
        return Response([])
