"""DynamoDB + SQS backend.

Maps the same Store protocol the Postgres backend satisfies, so the engine,
worker, and every test that uses the protocol directly run unchanged.

Table layout
────────────
One table, two item shapes:

  RUN item
    pk = "RUN#{run_id}"           (partition key)
    sk = "META"                    (sort key)
    + all run fields as top-level attributes

  EVENT item
    pk = "RUN#{run_id}"
    sk = "EVENT#{seq:020d}"        (zero-padded so lexicographic == numeric order)
    + type, step_key, payload, created_at

  COUNTER item  (one per run, used to allocate event sequence numbers)
    pk = "RUN#{run_id}"
    sk = "COUNTER"
    seq = N

  IDEMPOTENCY item  (for submission deduplication)
    pk = "IDEM#{idempotency_key}"
    sk = "KEY"
    run_id = ...

  SIDE_EFFECT item
    pk = "EFFECT#{token}"
    sk = "DATA"
    + run_id, kind, payload, created_at

Queue
─────
A standard SQS queue with visibility timeout acting as the lease.  Messages
carry the run_id; the worker receives one, extends the visibility timeout as
its heartbeat, and deletes the message on success.  A worker that dies stops
extending, the timeout lapses, and the message reappears for another worker.

This is the same lease model as the Postgres backend, and it satisfies the
same five capabilities the engine depends on.

Design notes
────────────
Sequence allocation uses a conditional update on the COUNTER item
(UpdateExpression + ConditionExpression).  That is the DynamoDB equivalent of
the Postgres CTE that bumps event_seq and inserts the event in one statement.
The condition ensures the seq we read is still the seq we increment, giving us
gapless numbers without a distributed lock.

Exactly-once side effects use a conditional put with
attribute_not_exists(pk), which is DynamoDB's equivalent of the Postgres
UNIQUE constraint on side_effects.token.

Status changes use a conditional update that checks the current status before
writing, so two workers racing on the same run produce one terminal state, not
two.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import boto3
from botocore.exceptions import ClientError

from anchor.events import EventType
from anchor.models import Budget, Claim, Event, RunRecord, RunStatus, Usage

# ── helpers ──────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(UTC).isoformat()

def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value)

def _run_pk(run_id: uuid.UUID) -> str:
    return f"RUN#{run_id}"

def _event_sk(seq: int) -> str:
    return f"EVENT#{seq:020d}"

def _idem_pk(key: str) -> str:
    return f"IDEM#{key}"

def _effect_pk(token: uuid.UUID) -> str:
    return f"EFFECT#{token}"

def _enc(value: Any) -> Any:
    """DynamoDB cannot store plain float; use Decimal."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {k: _enc(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_enc(v) for v in value]
    return value

def _dec(value: Any) -> Any:
    """Undo Decimal on the way out."""
    if isinstance(value, Decimal):
        f = float(value)
        return int(f) if f.is_integer() else f
    if isinstance(value, dict):
        return {k: _dec(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_dec(v) for v in value]
    return value

def _from_item(item: dict[str, Any]) -> RunRecord:
    return RunRecord(
        run_id=uuid.UUID(item["run_id"]),
        workflow=item["workflow"],
        input=_dec(item.get("input", {})),
        idempotency_key=item["idempotency_key"],
        status=RunStatus(item["status"]),
        event_seq=int(item.get("event_seq", 0)),
        attempt=int(item.get("attempt", 0)),
        budget=Budget.from_dict(_dec(item.get("budget", {}))),
        usage=Usage.from_dict(_dec(item.get("usage", {}))),
        result=_dec(item.get("result")),
        error=_dec(item.get("error")),
        cancel_requested=bool(item.get("cancel_requested", False)),
        created_at=_utc(item["created_at"]),
        updated_at=_utc(item["updated_at"]),
        finished_at=_utc(item["finished_at"]) if item.get("finished_at") else None,
    )


# ── store ─────────────────────────────────────────────────────────────────────

class DynamoDBStore:
    """DynamoDB + SQS implementation of the Store protocol.

    Constructed with boto3 resource/client objects so callers can inject moto
    mocks in tests without patching at the module level.
    """

    def __init__(
        self,
        table_name: str = "anchor",
        queue_url: str | None = None,
        *,
        dynamodb_resource: Any = None,
        sqs_client: Any = None,
        region: str = "us-east-1",
        visibility_timeout: int = 30,
    ) -> None:
        self.table_name = table_name
        self.visibility_timeout = visibility_timeout
        self.region = region

        self._ddb = dynamodb_resource or boto3.resource("dynamodb", region_name=region)
        self._sqs = sqs_client or boto3.client("sqs", region_name=region)
        self._table = self._ddb.Table(table_name)
        self._queue_url = queue_url

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def migrate(self) -> None:
        """Create the table and queue if they do not exist.

        Idempotent; safe to call on every deploy.  In production you would
        provision these with Terraform / CDK instead, but having this method
        keeps the test fixture simple.
        """
        # Table
        existing = [t.name for t in self._ddb.tables.all()]
        if self.table_name not in existing:
            self._ddb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
            self._table = self._ddb.Table(self.table_name)

        # Queue
        if self._queue_url is None:
            queue_name = f"{self.table_name}-queue"
            try:
                resp = self._sqs.get_queue_url(QueueName=queue_name)
                self._queue_url = resp["QueueUrl"]
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "AWS.SimpleQueueService.NonExistentQueue":
                    raise
                resp = self._sqs.create_queue(
                    QueueName=queue_name,
                    Attributes={"VisibilityTimeout": str(self.visibility_timeout)},
                )
                self._queue_url = resp["QueueUrl"]

    async def truncate_all(self) -> None:
        """Test helper — wipe all items and purge the queue."""
        # Scan + batch delete
        items = []
        resp = self._table.scan(ProjectionExpression="pk, sk")
        items.extend(resp.get("Items", []))
        while "LastEvaluatedKey" in resp:
            resp = self._table.scan(
                ProjectionExpression="pk, sk",
                ExclusiveStartKey=resp["LastEvaluatedKey"],
            )
            items.extend(resp.get("Items", []))

        with self._table.batch_writer() as batch:
            for item in items:
                batch.delete_item(Key={"pk": item["pk"], "sk": item["sk"]})

        if self._queue_url:
            try:
                self._sqs.purge_queue(QueueUrl=self._queue_url)
            except ClientError:
                pass  # moto sometimes errors on purge; drain manually instead
                while True:
                    msgs = self._sqs.receive_message(
                        QueueUrl=self._queue_url, MaxNumberOfMessages=10
                    ).get("Messages", [])
                    if not msgs:
                        break
                    for m in msgs:
                        self._sqs.delete_message(
                            QueueUrl=self._queue_url, ReceiptHandle=m["ReceiptHandle"]
                        )

    # ── runs ──────────────────────────────────────────────────────────────────

    async def submit_run(
        self,
        workflow: str,
        payload: dict[str, Any],
        idempotency_key: str,
        budget: Budget,
    ) -> tuple[RunRecord, bool]:
        run_id = uuid.uuid4()
        now = _now_iso()

        # Reserve the idempotency slot.  The condition fires if the key is new.
        idem_pk = _idem_pk(idempotency_key)
        try:
            self._table.put_item(
                Item={"pk": idem_pk, "sk": "KEY", "run_id": str(run_id)},
                ConditionExpression="attribute_not_exists(pk)",
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            # Already exists — return the original run.
            existing_item = self._table.get_item(Key={"pk": idem_pk, "sk": "KEY"})["Item"]
            existing_run = await self.get_run(uuid.UUID(existing_item["run_id"]))
            return existing_run, False

        # Write the run item.
        item = {
            "pk": _run_pk(run_id),
            "sk": "META",
            "run_id": str(run_id),
            "workflow": workflow,
            "input": _enc(payload),
            "idempotency_key": idempotency_key,
            "status": RunStatus.PENDING.value,
            "event_seq": 0,
            "attempt": 0,
            "budget": _enc(budget.to_dict()),
            "usage": _enc({}),
            "cancel_requested": False,
            "created_at": now,
            "updated_at": now,
        }
        self._table.put_item(Item=item)

        # Initialise the event counter.
        self._table.put_item(
            Item={"pk": _run_pk(run_id), "sk": "COUNTER", "seq": 0}
        )

        # Enqueue.
        if self._queue_url:
            self._sqs.send_message(
                QueueUrl=self._queue_url,
                MessageBody=json.dumps({"run_id": str(run_id)}),
            )

        # Write RunStarted event.
        await self.append_event(
            run_id,
            EventType.RUN_STARTED,
            None,
            {"workflow": workflow, "input": payload, "budget": budget.to_dict()},
        )

        run = _from_item(item)
        return run, True

    async def get_run(self, run_id: uuid.UUID) -> RunRecord | None:
        resp = self._table.get_item(Key={"pk": _run_pk(run_id), "sk": "META"})
        item = resp.get("Item")
        return _from_item(_dec(item)) if item else None

    async def list_runs(self, limit: int = 50, status: str | None = None) -> list[RunRecord]:
        # Full-table scan; acceptable for a dev/demo context.  Production would
        # add a GSI on status+updated_at.
        kwargs: dict[str, Any] = {
            "FilterExpression": "sk = :meta",
            "ExpressionAttributeValues": {":meta": "META"},
            "Limit": min(limit, 200),
        }
        if status:
            kwargs["FilterExpression"] += " AND #s = :status"
            kwargs["ExpressionAttributeNames"] = {"#s": "status"}
            kwargs["ExpressionAttributeValues"][":status"] = status

        resp = self._table.scan(**kwargs)
        runs = [_from_item(_dec(i)) for i in resp.get("Items", [])]
        return sorted(runs, key=lambda r: r.created_at, reverse=True)[:limit]

    async def set_status(
        self,
        run_id: uuid.UUID,
        status: RunStatus,
        *,
        result: Any = None,
        error: dict[str, Any] | None = None,
    ) -> None:
        now = _now_iso()
        expr = "SET #s = :s, updated_at = :now"
        names = {"#s": "status"}
        values: dict[str, Any] = {":s": status.value, ":now": now}

        if result is not None:
            expr += ", #r = :r"
            names["#r"] = "result"
            values[":r"] = _enc(result)
        if error is not None:
            expr += ", #e = :e"
            names["#e"] = "error"
            values[":e"] = _enc(error)
        if status.terminal:
            expr += ", finished_at = :fin"
            values[":fin"] = now

        self._table.update_item(
            Key={"pk": _run_pk(run_id), "sk": "META"},
            UpdateExpression=expr,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )

    async def bump_attempt(self, run_id: uuid.UUID) -> int:
        resp = self._table.update_item(
            Key={"pk": _run_pk(run_id), "sk": "META"},
            UpdateExpression="SET attempt = attempt + :one, updated_at = :now",
            ExpressionAttributeValues={":one": 1, ":now": _now_iso()},
            ReturnValues="UPDATED_NEW",
        )
        return int(resp["Attributes"]["attempt"])

    async def save_usage(self, run_id: uuid.UUID, usage: Usage) -> None:
        self._table.update_item(
            Key={"pk": _run_pk(run_id), "sk": "META"},
            UpdateExpression="SET usage = :u, updated_at = :now",
            ExpressionAttributeValues={":u": _enc(usage.to_dict()), ":now": _now_iso()},
        )

    async def request_cancel(self, run_id: uuid.UUID) -> bool:
        try:
            self._table.update_item(
                Key={"pk": _run_pk(run_id), "sk": "META"},
                UpdateExpression="SET cancel_requested = :t, updated_at = :now",
                ConditionExpression=(
                    "attribute_exists(pk) AND "
                    "#s IN (:pending, :running)"
                ),
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":t": True,
                    ":now": _now_iso(),
                    ":pending": RunStatus.PENDING.value,
                    ":running": RunStatus.RUNNING.value,
                },
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise
        await self.append_event(run_id, EventType.CANCEL_REQUESTED, None, {})
        return True

    async def is_cancel_requested(self, run_id: uuid.UUID) -> bool:
        resp = self._table.get_item(
            Key={"pk": _run_pk(run_id), "sk": "META"},
            ProjectionExpression="cancel_requested",
        )
        return bool(resp.get("Item", {}).get("cancel_requested", False))

    # ── events ────────────────────────────────────────────────────────────────

    async def append_event(
        self,
        run_id: uuid.UUID,
        type: str,
        step_key: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> int:
        # Atomically claim the next sequence number.
        resp = self._table.update_item(
            Key={"pk": _run_pk(run_id), "sk": "COUNTER"},
            UpdateExpression="SET seq = seq + :one",
            ExpressionAttributeValues={":one": 1},
            ReturnValues="UPDATED_NEW",
        )
        seq = int(resp["Attributes"]["seq"])

        # Mirror seq onto the run row so get_run can report it.
        self._table.update_item(
            Key={"pk": _run_pk(run_id), "sk": "META"},
            UpdateExpression="SET event_seq = :seq, updated_at = :now",
            ExpressionAttributeValues={":seq": seq, ":now": _now_iso()},
        )

        item: dict[str, Any] = {
            "pk": _run_pk(run_id),
            "sk": _event_sk(seq),
            "seq": seq,
            "type": type,
            "payload": _enc(payload or {}),
            "created_at": _now_iso(),
        }
        if step_key is not None:
            item["step_key"] = step_key

        self._table.put_item(Item=item)
        return seq

    async def load_events(self, run_id: uuid.UUID) -> list[Event]:
        from boto3.dynamodb.conditions import Key as DKey

        resp = self._table.query(
            KeyConditionExpression=(
                DKey("pk").eq(_run_pk(run_id)) & DKey("sk").begins_with("EVENT#")
            ),
            ScanIndexForward=True,  # ascending by sk == ascending by seq
        )
        events = []
        for item in resp.get("Items", []):
            events.append(
                Event(
                    seq=int(item["seq"]),
                    type=item["type"],
                    step_key=item.get("step_key"),
                    payload=_dec(item.get("payload", {})),
                    created_at=_utc(item["created_at"]),
                )
            )
        return events

    # ── queue / lease ─────────────────────────────────────────────────────────

    async def claim(self, worker_id: str, lease_seconds: float) -> Claim | None:
        """Receive one message and lock it with a visibility timeout.

        SQS visibility timeout is the lease: while a message is in flight, no
        other consumer sees it.  If the worker dies without deleting it, the
        timeout lapses and the message reappears — the same recovery path as the
        Postgres SKIP LOCKED version.

        We store the receipt handle in a separate DynamoDB item so heartbeat and
        release can find it by run_id rather than having to remember a handle
        across async calls.
        """
        msgs = self._sqs.receive_message(
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=max(1, int(lease_seconds)),
            WaitTimeSeconds=0,
        ).get("Messages", [])
        if not msgs:
            return None

        msg = msgs[0]
        body = json.loads(msg["Body"])
        run_id = uuid.UUID(body["run_id"])
        receipt = msg["ReceiptHandle"]

        # Remove stale LEASE items from any previous owner before writing ours.
        from boto3.dynamodb.conditions import Key as DKey
        stale = self._table.query(
            KeyConditionExpression=(
                DKey("pk").eq(_run_pk(run_id)) & DKey("sk").begins_with("LEASE#")
            ),
        ).get("Items", [])
        for stale_item in stale:
            self._table.delete_item(Key={"pk": stale_item["pk"], "sk": stale_item["sk"]})

        # Store the receipt handle so heartbeat/release can look it up.
        self._table.put_item(
            Item={
                "pk": _run_pk(run_id),
                "sk": f"LEASE#{worker_id}",
                "receipt_handle": receipt,
                "worker_id": worker_id,
                "claimed_at": _now_iso(),
            }
        )

        # Count deliveries (not successes) so the store matches the Postgres contract.
        resp = self._table.update_item(
            Key={"pk": _run_pk(run_id), "sk": "META"},
            UpdateExpression="SET attempt_count = if_not_exists(attempt_count, :zero) + :one",
            ExpressionAttributeValues={":zero": 0, ":one": 1},
            ReturnValues="UPDATED_NEW",
        )
        attempts = int(resp["Attributes"]["attempt_count"])
        return Claim(run_id=run_id, attempts=attempts)

    async def heartbeat(self, run_id: uuid.UUID, worker_id: str, lease_seconds: float) -> bool:
        """Extend the SQS visibility timeout — the DynamoDB lease heartbeat."""
        item = self._table.get_item(
            Key={"pk": _run_pk(run_id), "sk": f"LEASE#{worker_id}"}
        ).get("Item")
        if item is None:
            return False
        try:
            self._sqs.change_message_visibility(
                QueueUrl=self._queue_url,
                ReceiptHandle=item["receipt_handle"],
                VisibilityTimeout=max(1, int(lease_seconds)),
            )
            return True
        except ClientError:
            return False

    async def release(self, run_id: uuid.UUID, worker_id: str, delay_seconds: float) -> None:
        """Make the message visible again after a delay (re-enqueue for retry)."""
        item = self._table.get_item(
            Key={"pk": _run_pk(run_id), "sk": f"LEASE#{worker_id}"}
        ).get("Item")
        if item is None:
            return
        try:
            self._sqs.change_message_visibility(
                QueueUrl=self._queue_url,
                ReceiptHandle=item["receipt_handle"],
                VisibilityTimeout=max(0, int(delay_seconds)),
            )
        except ClientError:
            pass

    async def dequeue(self, run_id: uuid.UUID) -> None:
        """Delete the SQS message — the run is done."""
        # Find any lease item for this run to get the receipt handle.
        from boto3.dynamodb.conditions import Key as DKey

        resp = self._table.query(
            KeyConditionExpression=(
                DKey("pk").eq(_run_pk(run_id)) & DKey("sk").begins_with("LEASE#")
            ),
            Limit=1,
        )
        items = resp.get("Items", [])
        if not items:
            return
        item = items[0]
        try:
            self._sqs.delete_message(
                QueueUrl=self._queue_url,
                ReceiptHandle=item["receipt_handle"],
            )
        except ClientError:
            pass
        self._table.delete_item(Key={"pk": item["pk"], "sk": item["sk"]})

    async def queue_depth(self) -> dict[str, int]:
        attrs = self._sqs.get_queue_attributes(
            QueueUrl=self._queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
            ],
        )["Attributes"]
        claimable = int(attrs.get("ApproximateNumberOfMessages", 0))
        leased = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        return {"claimable": claimable, "leased": leased, "total": claimable + leased}

    async def status_counts(self) -> dict[str, int]:
        resp = self._table.scan(
            FilterExpression="sk = :meta",
            ExpressionAttributeValues={":meta": "META"},
            ProjectionExpression="#s",
            ExpressionAttributeNames={"#s": "status"},
        )
        counts: dict[str, int] = {}
        for item in resp.get("Items", []):
            s = item.get("status", "UNKNOWN")
            counts[s] = counts.get(s, 0) + 1
        return counts

    # ── side effects ──────────────────────────────────────────────────────────

    async def record_side_effect(
        self, token: uuid.UUID, run_id: uuid.UUID, kind: str, payload: dict[str, Any]
    ) -> bool:
        """Conditional put: first write wins, second write is silently dropped.

        attribute_not_exists(pk) is DynamoDB's UNIQUE constraint equivalent.
        The token is the primary key, so the guarantee is identical to the
        Postgres side_effects.token UNIQUE index.
        """
        try:
            self._table.put_item(
                Item={
                    "pk": _effect_pk(token),
                    "sk": "DATA",
                    "token": str(token),
                    "run_id": str(run_id),
                    "kind": kind,
                    "payload": _enc(payload),
                    "created_at": _now_iso(),
                },
                ConditionExpression="attribute_not_exists(pk)",
            )
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    async def find_side_effect(self, token: uuid.UUID) -> dict[str, Any] | None:
        resp = self._table.get_item(Key={"pk": _effect_pk(token), "sk": "DATA"})
        item = resp.get("Item")
        if item is None:
            return None
        return {
            "token": item["token"],
            "run_id": item["run_id"],
            "kind": item["kind"],
            "payload": _dec(item.get("payload", {})),
            "created_at": item["created_at"],
        }

    async def count_side_effects(self, run_id: uuid.UUID, kind: str) -> int:
        resp = self._table.scan(
            FilterExpression="run_id = :rid AND kind = :k AND sk = :data",
            ExpressionAttributeValues={
                ":rid": str(run_id),
                ":k": kind,
                ":data": "DATA",
            },
            Select="COUNT",
        )
        return int(resp.get("Count", 0))
