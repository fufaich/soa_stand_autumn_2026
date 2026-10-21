"""Надёжный Orders для заданий 1 и 2.

HTTP-обработчик только сохраняет новый заказ в PostgreSQL. Saga-воркеры
резервируют товар и оплачивают заказ в фоне, поэтому покупатель не ждёт
медленные соседние сервисы. PostgreSQL хранит прогресс и позволяет продолжить
работу после kill -9. Transactional outbox гарантирует публикацию order.paid.
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import random
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import psycopg
import requests
from flask import Flask, jsonify, request
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DATABASE_URL = os.environ["DATABASE_URL"]
PAYMENT_URL = os.environ["PAYMENT_URL"].rstrip("/")
INVENTORY_URL = os.environ["INVENTORY_URL"].rstrip("/")
EVENTS_URL = os.environ.get("EVENTS_URL", "").rstrip("/")

# Короткий таймаут освобождает воркер при зависшем соседе. Число платёжных
# воркеров намеренно мало: это bulkhead — зависший Payment не съедает все
# ресурсы и не получает шторм запросов.
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "1.0"))
# Это timeout только фонового вызова, а не POST /orders. Он должен позволять
# получить намеренно медленный ответ банка; при полном зависании bulkhead из
# четырёх слотов всё равно ограничивает нагрузку.
PAYMENT_TIMEOUT = float(os.environ.get("PAYMENT_TIMEOUT", "4.0"))
SAGA_WORKERS = int(os.environ.get("SAGA_WORKERS", "8"))
PAYMENT_CONCURRENCY = int(os.environ.get("PAYMENT_CONCURRENCY", "4"))
OUTBOX_WORKERS = int(os.environ.get("OUTBOX_WORKERS", "4"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(threadName)s %(message)s")
log = logging.getLogger("orders")

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'paid', 'rejected')),
    stage TEXT NOT NULL CHECK (stage IN ('new', 'reserved', 'paying', 'paid', 'rejected')),
    amount_cents BIGINT NOT NULL,
    items JSONB NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS orders_work_idx
    ON orders (next_attempt_at) WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS outbox (
    event_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE REFERENCES orders(id),
    payload JSONB NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_until TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS outbox_work_idx
    ON outbox (next_attempt_at) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS delivery_events (
    event_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    bonus_points BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS bonuses (
    user_id TEXT PRIMARY KEY,
    points BIGINT NOT NULL CHECK (points >= 0)
);
"""

pool = ConnectionPool(DATABASE_URL, min_size=2, max_size=24, open=False,
                      kwargs={"autocommit": True, "row_factory": dict_row})
stop_event = threading.Event()
payment_slots = threading.BoundedSemaphore(PAYMENT_CONCURRENCY)
payment_state_lock = threading.Lock()
payment_retry_after = 0.0
payment_failures = 0
workers: list[threading.Thread] = []
started = False
start_lock = threading.Lock()

app = Flask(__name__)
app.json.sort_keys = False


def init_db() -> None:
    """Открыть пул и создать схему; при старте дождаться PostgreSQL."""
    pool.open(wait=True, timeout=60)
    with pool.connection() as conn:
        conn.execute(SCHEMA)


def validate_order(body: Any) -> tuple[str, list[dict[str, Any]], int] | None:
    """Проверить контракт и безопасно посчитать сумму заказа."""
    if not isinstance(body, dict) or set(body) - {"user_id", "items"}:
        return None
    user_id, items = body.get("user_id"), body.get("items")
    if not isinstance(user_id, str) or not user_id or not isinstance(items, list) or not items:
        return None
    amount = 0
    clean: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict) or set(item) != {"sku", "qty", "price_cents"}:
            return None
        sku, qty, price = item.get("sku"), item.get("qty"), item.get("price_cents")
        # bool является подклассом int в Python, но JSON true не должен быть ценой.
        if (not isinstance(sku, str) or not sku or not isinstance(qty, int) or isinstance(qty, bool)
                or qty < 1 or not isinstance(price, int) or isinstance(price, bool) or price < 0):
            return None
        amount += qty * price
        if amount > 9_223_372_036_854_775_807:
            return None
        clean.append({"sku": sku, "qty": qty, "price_cents": price})
    # Payment принимает только положительную сумму.
    if amount <= 0:
        return None
    return user_id, clean, amount


def validate_delivery(body: Any) -> tuple[str, str, int] | None:
    if not isinstance(body, dict) or body.get("type") != "delivery.confirmed":
        return None
    event_id, user_id, points = body.get("id"), body.get("user_id"), body.get("bonus_points")
    if (not isinstance(event_id, str) or not event_id or not isinstance(user_id, str) or not user_id
            or not isinstance(points, int) or isinstance(points, bool) or points <= 0):
        return None
    return event_id, user_id, points


def retry_delay(attempts: int, maximum: float = 5.0) -> float:
    """Exponential backoff с jitter: соседа не атакует синхронная лавина retry."""
    base = min(maximum, 0.25 * (2 ** min(attempts, 5)))
    return base * random.uniform(0.8, 1.2)


def claim_order() -> dict[str, Any] | None:
    with pool.connection() as conn, conn.transaction():
        row = conn.execute(
            """SELECT * FROM orders
               WHERE status = 'pending' AND next_attempt_at <= now()
                 AND (claimed_until IS NULL OR claimed_until < now())
               ORDER BY next_attempt_at, created_at
               FOR UPDATE SKIP LOCKED LIMIT 1"""
        ).fetchone()
        if not row:
            return None
        # Lease длиннее одного HTTP timeout и удерживается между шагами saga.
        # Поэтому второй воркер не начнёт тот же платёж параллельно.
        conn.execute("UPDATE orders SET claimed_until = now() + interval '5 seconds' WHERE id = %s", (row["id"],))
        return row


def reschedule_order(order_id: str, attempts: int, message: str) -> None:
    delay = retry_delay(attempts)
    log.warning("заказ %s: %s; повтор через %.2f с", order_id, message, delay)
    with pool.connection() as conn:
        conn.execute(
            """UPDATE orders SET attempts = attempts + 1, claimed_until = NULL,
               next_attempt_at = now() + (%s * interval '1 second'), updated_at = now()
               WHERE id = %s AND status = 'pending'""", (delay, order_id)
        )


def reserve(order: dict[str, Any]) -> bool:
    payload = {"order_id": order["id"], "items": [
        {"sku": item["sku"], "qty": item["qty"]} for item in order["items"]
    ]}
    try:
        response = requests.post(f"{INVENTORY_URL}/reservations", json=payload, timeout=HTTP_TIMEOUT)
        if response.status_code not in (200, 201):
            reschedule_order(order["id"], order["attempts"], f"Inventory ответил {response.status_code}")
            return False
    except requests.RequestException as exc:
        reschedule_order(order["id"], order["attempts"], f"Inventory недоступен: {exc}")
        return False
    with pool.connection() as conn:
        conn.execute("""UPDATE orders SET stage = 'reserved', attempts = 0,
                        next_attempt_at = now(), updated_at = now()
                        WHERE id = %s AND stage = 'new'""", (order["id"],))
    return True


def pay(order: dict[str, Any]) -> None:
    global payment_retry_after, payment_failures
    # Сначала фиксируем намерение платить. Если процесс умрёт после списания,
    # новый процесс увидит stage=paying и повторит запрос с тем же ключом.
    with pool.connection() as conn:
        conn.execute("""UPDATE orders SET stage = 'paying',
                        next_attempt_at = now(), updated_at = now()
                        WHERE id = %s AND stage = 'reserved'""", (order["id"],))

    # Общий circuit breaker: если Payment только что не ответил, остальные
    # заказы немного ждут вместо того, чтобы каждый проверял outage сам.
    with payment_state_lock:
        blocked_for = payment_retry_after - time.monotonic()
    if blocked_for > 0:
        reschedule_order(order["id"], order["attempts"], "Payment временно отключён circuit breaker")
        return
    if not payment_slots.acquire(timeout=0.05):
        reschedule_order(order["id"], order["attempts"], "все платёжные слоты заняты")
        return
    started_at = time.monotonic()
    try:
        response = requests.post(
            f"{PAYMENT_URL}/payments",
            json={"order_id": order["id"], "amount_cents": order["amount_cents"], "currency": "RUB"},
            headers={"Idempotency-Key": f"payment:{order['id']}"}, timeout=PAYMENT_TIMEOUT,
        )
    except requests.RequestException as exc:
        # Закрываем вход для новых POST до контрольного GET. Это существенно
        # ограничивает число запросов при полном outage.
        with payment_state_lock:
            payment_failures += 1
            pause = min(5.0, 0.5 * (2 ** min(payment_failures - 1, 4)))
            payment_retry_after = max(payment_retry_after, time.monotonic() + pause)
        # POST мог выполниться, а ответ — потеряться. Проверка по order_id
        # превращает неизвестный результат в известный и особенно быстро
        # завершает сценарий lost response. Во время настоящего outage GET
        # тоже не ответит — тогда включается общий circuit breaker.
        # Быстрый обрыв обычно означает lost response после выполненной
        # операции. При полном outage POST уже ждал timeout, и дополнительный
        # GET лишь удвоил бы нагрузку на недоступный сервис.
        if time.monotonic() - started_at < HTTP_TIMEOUT:
            try:
                lookup = requests.get(f"{PAYMENT_URL}/payments", params={"order_id": order["id"]},
                                      timeout=HTTP_TIMEOUT)
                if lookup.status_code == 200 and lookup.json():
                    with payment_state_lock:
                        payment_failures = 0
                        payment_retry_after = 0.0
                    mark_paid(order)
                    return
            except (requests.RequestException, ValueError):
                pass
        reschedule_order(order["id"], order["attempts"], f"результат Payment неизвестен: {exc}")
        return
    finally:
        payment_slots.release()

    # HTTP-ответ означает, что сосед снова доступен. 500 обрабатывается обычным
    # backoff конкретного заказа, но не открывает circuit для всех.
    with payment_state_lock:
        payment_failures = 0
        payment_retry_after = 0.0
    if response.status_code in (200, 201):
        mark_paid(order)
    elif response.status_code == 402:
        reject_order(order)
    else:
        reschedule_order(order["id"], order["attempts"], f"Payment ответил {response.status_code}")


def mark_paid(order: dict[str, Any]) -> None:
    """Статус paid и outbox появляются атомарно — либо обе записи, либо ни одной."""
    event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"order.paid:{order['id']}"))
    payload = {
        "id": event_id, "type": "order.paid", "order_id": order["id"],
        "occurred_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    with pool.connection() as conn, conn.transaction():
        conn.execute("""UPDATE orders SET status = 'paid', stage = 'paid', attempts = 0,
                        claimed_until = NULL, updated_at = now()
                        WHERE id = %s AND status = 'pending'""", (order["id"],))
        conn.execute("""INSERT INTO outbox (event_id, order_id, payload) VALUES (%s, %s, %s)
                        ON CONFLICT (order_id) DO NOTHING""", (event_id, order["id"], json.dumps(payload)))


def reject_order(order: dict[str, Any]) -> None:
    """После явного 402 снять идемпотентный резерв и только затем завершить saga."""
    try:
        response = requests.delete(f"{INVENTORY_URL}/reservations/{order['id']}", timeout=HTTP_TIMEOUT)
        if response.status_code != 204:
            reschedule_order(order["id"], order["attempts"], f"снятие резерва: {response.status_code}")
            return
    except requests.RequestException as exc:
        reschedule_order(order["id"], order["attempts"], f"не удалось снять резерв: {exc}")
        return
    with pool.connection() as conn:
        conn.execute("""UPDATE orders SET status = 'rejected', stage = 'rejected', attempts = 0,
                        claimed_until = NULL, updated_at = now()
                        WHERE id = %s AND status = 'pending'""", (order["id"],))


def saga_worker() -> None:
    while not stop_event.is_set():
        try:
            order = claim_order()
            if not order:
                stop_event.wait(0.05)
            elif order["stage"] == "new":
                # После резерва этот же владелец сразу продолжает к оплате;
                # lease не даёт другому потоку выполнить её параллельно.
                if reserve(order):
                    order["stage"] = "reserved"
                    order["attempts"] = 0
                    pay(order)
            else:
                pay(order)
        except Exception:
            log.exception("ошибка saga-воркера")
            stop_event.wait(0.2)


def claim_event() -> dict[str, Any] | None:
    with pool.connection() as conn, conn.transaction():
        row = conn.execute(
            """SELECT * FROM outbox WHERE published_at IS NULL AND next_attempt_at <= now()
               AND (claimed_until IS NULL OR claimed_until < now())
               ORDER BY next_attempt_at, created_at FOR UPDATE SKIP LOCKED LIMIT 1"""
        ).fetchone()
        if not row:
            return None
        conn.execute("UPDATE outbox SET claimed_until = now() + interval '4 seconds' WHERE event_id = %s",
                     (row["event_id"],))
        return row


def outbox_worker() -> None:
    while not stop_event.is_set():
        try:
            event = claim_event()
            if not event:
                stop_event.wait(0.1)
                continue
            if not EVENTS_URL:
                stop_event.wait(1)
                continue
            try:
                response = requests.post(f"{EVENTS_URL}/events", json=event["payload"], timeout=HTTP_TIMEOUT)
                accepted = response.status_code == 202
            except requests.RequestException:
                accepted = False
            with pool.connection() as conn:
                if accepted:
                    conn.execute("UPDATE outbox SET published_at = now(), claimed_until = NULL WHERE event_id = %s",
                                 (event["event_id"],))
                else:
                    delay = retry_delay(event["attempts"], maximum=3.0)
                    conn.execute("""UPDATE outbox SET attempts = attempts + 1, claimed_until = NULL,
                                    next_attempt_at = now() + (%s * interval '1 second')
                                    WHERE event_id = %s""", (delay, event["event_id"]))
        except Exception:
            log.exception("ошибка outbox-воркера")
            stop_event.wait(0.2)


def start_background_workers() -> None:
    global started
    with start_lock:
        if started:
            return
        init_db()
        for index in range(SAGA_WORKERS):
            workers.append(threading.Thread(target=saga_worker, name=f"saga-{index}", daemon=True))
        for index in range(OUTBOX_WORKERS):
            workers.append(threading.Thread(target=outbox_worker, name=f"outbox-{index}", daemon=True))
        for worker in workers:
            worker.start()
        started = True
        log.info("сервис готов: saga=%d, payment=%d, outbox=%d", SAGA_WORKERS, PAYMENT_CONCURRENCY, OUTBOX_WORKERS)


@atexit.register
def shutdown() -> None:
    stop_event.set()
    if started:
        pool.close()


@app.get("/healthz")
def health() -> tuple[Any, int]:
    try:
        with pool.connection(timeout=1) as conn:
            conn.execute("SELECT 1")
        return jsonify(status="ok"), 200
    except Exception:
        return jsonify(status="not_ready"), 503


@app.post("/orders")
def create_order() -> tuple[Any, int]:
    parsed = validate_order(request.get_json(silent=True))
    if parsed is None:
        return jsonify(error="bad_request", message="некорректный заказ"), 400
    user_id, items, amount = parsed
    order_id = str(uuid.uuid4())
    try:
        with pool.connection() as conn:
            conn.execute("""INSERT INTO orders (id, user_id, status, stage, amount_cents, items)
                            VALUES (%s, %s, 'pending', 'new', %s, %s)""",
                         (order_id, user_id, amount, json.dumps(items)))
        return jsonify(id=order_id, status="pending"), 201
    except Exception:
        log.exception("не удалось создать заказ")
        # Нельзя подтверждать неперсистентный заказ: клиент может безопасно
        # повторить запрос после восстановления хранилища.
        return jsonify(error="orders storage is temporarily unavailable"), 503


@app.get("/orders/<order_id>")
def get_order(order_id: str) -> tuple[Any, int]:
    with pool.connection() as conn:
        row = conn.execute("SELECT id, status, amount_cents FROM orders WHERE id = %s", (order_id,)).fetchone()
    if row is None:
        return jsonify(error="not_found"), 404
    return jsonify(id=row["id"], status=row["status"], amount_cents=row["amount_cents"]), 200


@app.post("/webhooks/delivery")
def delivery_webhook() -> tuple[Any, int]:
    parsed = validate_delivery(request.get_json(silent=True))
    if parsed is None:
        return jsonify(error="bad_request"), 400
    event_id, user_id, points = parsed
    try:
        with pool.connection() as conn, conn.transaction():
            inserted = conn.execute(
                """INSERT INTO delivery_events (event_id, user_id, bonus_points)
                   VALUES (%s, %s, %s) ON CONFLICT (event_id) DO NOTHING RETURNING event_id""",
                (event_id, user_id, points),
            ).fetchone()
            if inserted:
                conn.execute("""INSERT INTO bonuses (user_id, points) VALUES (%s, %s)
                                ON CONFLICT (user_id) DO UPDATE SET points = bonuses.points + EXCLUDED.points""",
                             (user_id, points))
        return jsonify(status="ok"), 200
    except Exception:
        log.exception("не удалось обработать delivery.confirmed")
        # 5xx нужен намеренно: шина повторит доставку и событие не потеряется.
        return jsonify(error="temporarily_unavailable"), 503


@app.get("/users/<user_id>/bonus")
def get_bonus(user_id: str) -> tuple[Any, int]:
    with pool.connection() as conn:
        row = conn.execute("SELECT points FROM bonuses WHERE user_id = %s", (user_id,)).fetchone()
    return jsonify(points=row["points"] if row else 0), 200


# Gunicorn импортирует модуль, поэтому инициализация должна происходить здесь.
start_background_workers()
