"""
cloud.py — Distributed Order Processing Simulator
==================================================
Demonstrates: Priority Queues · Rate Limiting · Circuit Breaker
             · Idempotency · Saga Transactions · DLQ · Event Replay
"""

import uuid
import time
import queue
from dataclasses import dataclass, field
from typing import Callable, Optional


# ──────────────────────────────────────────────────────────
# TERMINAL STYLING
# ──────────────────────────────────────────────────────────

class C:
    """ANSI color/style shortcuts."""
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    DIM    = "\033[2m"
    RED    = "\033[91m"
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    BLUE   = "\033[94m"
    CYAN   = "\033[96m"
    WHITE  = "\033[97m"


def banner(title: str) -> None:
    width = 56
    print(f"\n{C.BOLD}{C.BLUE}{'─' * width}{C.RESET}")
    print(f"{C.BOLD}{C.WHITE}  {title}{C.RESET}")
    print(f"{C.BOLD}{C.BLUE}{'─' * width}{C.RESET}")


def log(symbol: str, color: str, label: str, msg: str) -> None:
    print(f"  {color}{symbol} {C.BOLD}{label:<16}{C.RESET}{C.DIM}{msg}{C.RESET}")


def ok(label: str, msg: str)    -> None: log("✔", C.GREEN,  label, msg)
def warn(label: str, msg: str)  -> None: log("⚠", C.YELLOW, label, msg)
def err(label: str, msg: str)   -> None: log("✘", C.RED,    label, msg)
def info(label: str, msg: str)  -> None: log("→", C.CYAN,   label, msg)
def dim(label: str, msg: str)   -> None: log("·", C.WHITE,  label, msg)


# ──────────────────────────────────────────────────────────
# CONSTANTS
# ──────────────────────────────────────────────────────────

class Priority:
    HIGH   = 1   # VIP orders — dispatched first
    NORMAL = 2

class Status:
    PLACED   = "PLACED"
    RETRYING = "RETRYING"
    DLQ      = "DLQ"


# ──────────────────────────────────────────────────────────
# DATA MODEL
# ──────────────────────────────────────────────────────────

@dataclass
class Order:
    """A single order event flowing through the system."""
    order_id:    str
    user_id:     str
    items:       list
    amount:      float
    priority:    int   = Priority.NORMAL
    retry_count: int   = 0
    status:      str   = Status.PLACED


# ──────────────────────────────────────────────────────────
# RATE LIMITER
# Prevents any single user from flooding the system.
# ──────────────────────────────────────────────────────────

class RateLimiter:
    def __init__(self, max_requests: int = 3):
        self.max_requests = max_requests
        self._counts: dict[str, int] = {}

    def is_allowed(self, user_id: str) -> bool:
        self._counts[user_id] = self._counts.get(user_id, 0) + 1
        return self._counts[user_id] <= self.max_requests


# ──────────────────────────────────────────────────────────
# CIRCUIT BREAKER
# Opens after repeated failures so bad calls stop immediately.
# ──────────────────────────────────────────────────────────

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 3):
        self.threshold = failure_threshold
        self.failures  = 0
        self.is_open   = False

    def call(self, func: Callable, *args):
        if self.is_open:
            raise RuntimeError("Circuit is OPEN — calls blocked until reset")
        try:
            result = func(*args)
            self.failures = 0   # reset on success
            return result
        except Exception:
            self.failures += 1
            if self.failures >= self.threshold:
                self.is_open = True
                err("CircuitBreaker", f"opened after {self.failures} failures")
            raise

    def reset(self) -> None:
        self.failures = 0
        self.is_open  = False
        ok("CircuitBreaker", "reset — circuit CLOSED")


# ──────────────────────────────────────────────────────────
# MESSAGE BROKER
# Priority queue with idempotency guard and a Dead Letter Queue.
# ──────────────────────────────────────────────────────────

class Broker:
    def __init__(self):
        self._queue:     queue.PriorityQueue = queue.PriorityQueue()
        self._counter:   int                 = 0          # tie-breaker for equal priorities
        self._seen:      set[str]            = set()      # idempotency tracking
        self._consumers: list[Callable]      = []
        self.dlq:        list[Order]         = []         # Dead Letter Queue

    def register_consumer(self, handler: Callable) -> None:
        """Subscribe a service function to receive order events."""
        self._consumers.append(handler)

    def publish(self, order: Order) -> bool:
        """
        Enqueue an order. Returns False if it's a duplicate.
        Priority 1 (HIGH) is dispatched before Priority 2 (NORMAL).
        """
        if order.order_id in self._seen:
            warn("Broker", f"duplicate ignored → {order.order_id[:8]}…")
            return False

        self._queue.put((order.priority, self._counter, order))
        self._counter += 1
        self._seen.add(order.order_id)
        ok("Broker", f"enqueued order {order.order_id[:8]}… (priority={order.priority})")
        return True

    def send_to_dlq(self, order: Order) -> None:
        """Move a permanently failing order to the Dead Letter Queue."""
        order.status = Status.DLQ
        self.dlq.append(order)
        err("DLQ", f"order {order.order_id[:8]}… parked after exhausted retries")

    def dispatch_all(self) -> None:
        """Drain the queue, delivering each order to all registered consumers."""
        if self._queue.empty():
            dim("Broker", "queue is empty — nothing to dispatch")
            return

        info("Broker", "dispatching all queued orders…")
        while not self._queue.empty():
            _, _, order = self._queue.get()
            for handler in self._consumers:
                handler(order)


# ──────────────────────────────────────────────────────────
# PUBLISHER  (retry + circuit breaker wrapper around Broker)
# ──────────────────────────────────────────────────────────

class Publisher:
    MAX_RETRIES   = 3
    BACKOFF_BASE  = 2   # seconds; sleep = BACKOFF_BASE ** attempt

    def __init__(self, broker: Broker):
        self.broker  = broker
        self.breaker = CircuitBreaker()

    def publish(self, order: Order) -> bool:
        """
        Try to publish with exponential back-off.
        If all retries fail, route to DLQ.
        """
        for attempt in range(self.MAX_RETRIES):
            try:
                return self.breaker.call(self.broker.publish, order)
            except Exception as exc:
                delay = self.BACKOFF_BASE ** attempt
                warn("Publisher", f"attempt {attempt + 1} failed ({exc}) — retrying in {delay}s")
                order.status = Status.RETRYING
                time.sleep(delay)

        self.broker.send_to_dlq(order)
        return False


# ──────────────────────────────────────────────────────────
# SAGA  (distributed transaction coordinator)
# Tracks multi-step workflows; compensates on failure.
# ──────────────────────────────────────────────────────────

class Saga:
    def __init__(self):
        self._state: dict[str, dict | str] = {}

    def start(self, order_id: str) -> None:
        self._state[order_id] = {"payment": False, "inventory": False}
        info("Saga", f"transaction started → {order_id[:8]}…")

    def complete_step(self, order_id: str, step: str) -> None:
        if isinstance(self._state.get(order_id), dict):
            self._state[order_id][step] = True
            ok("Saga", f"step '{step}' completed → {order_id[:8]}…")

    def compensate(self, order_id: str) -> None:
        """Roll back all completed steps for this order."""
        self._state[order_id] = "COMPENSATED"
        err("Saga", f"rollback triggered → {order_id[:8]}…")


# ──────────────────────────────────────────────────────────
# DOWNSTREAM SERVICES  (consumers)
# ──────────────────────────────────────────────────────────

def payment_service(order: Order)    -> None: ok("Payment",    f"charged ₹{order.amount} for order {order.order_id[:8]}…")
def inventory_service(order: Order)  -> None: ok("Inventory",  f"reserved items {order.items} for {order.order_id[:8]}…")
def restaurant_service(order: Order) -> None: ok("Restaurant", f"kitchen notified for order {order.order_id[:8]}…")
def dispatch_service(order: Order)   -> None: ok("Dispatch",   f"rider assigned for order {order.order_id[:8]}…")


# ──────────────────────────────────────────────────────────
# ORDER SERVICE  (entry point for placing orders)
# ──────────────────────────────────────────────────────────

class OrderService:
    def __init__(self, publisher: Publisher, saga: Saga, limiter: RateLimiter):
        self.publisher = publisher
        self.saga      = saga
        self.limiter   = limiter

    def place_order(
        self,
        user_id: str,
        items:   list,
        amount:  float,
        vip:     bool = False
    ) -> Optional[str]:

        # 1. Validate input
        if not user_id or not items:
            err("Validation", "user_id and items are required")
            return None

        # 2. Rate limit check
        if not self.limiter.is_allowed(user_id):
            warn("RateLimit", f"user '{user_id}' exceeded {self.limiter.max_requests} requests")
            return None

        # 3. Build event
        order = Order(
            order_id = str(uuid.uuid4()),
            user_id  = user_id,
            items    = items,
            amount   = amount,
            priority = Priority.HIGH if vip else Priority.NORMAL,
        )

        # 4. Start saga transaction
        self.saga.start(order.order_id)

        # 5. Publish
        if self.publisher.publish(order):
            self.saga.complete_step(order.order_id, "payment")
            ok("OrderService", f"order placed → {order.order_id[:8]}… {'[VIP]' if vip else ''}")
            return order.order_id
        else:
            self.saga.compensate(order.order_id)
            return None


# ──────────────────────────────────────────────────────────
# MAIN  — twelve progressive scenarios
# ──────────────────────────────────────────────────────────

def main():
    # ── Setup ─────────────────────────────────────────────
    broker    = Broker()
    publisher = Publisher(broker)
    saga      = Saga()
    limiter   = RateLimiter(max_requests=3)
    service   = OrderService(publisher, saga, limiter)

    broker.register_consumer(payment_service)
    broker.register_consumer(inventory_service)
    broker.register_consumer(restaurant_service)
    broker.register_consumer(dispatch_service)

    # ── 1. Normal order ───────────────────────────────────
    banner("1 · Normal Order")
    service.place_order("alice", ["Burger", "Fries"], 250)

    # ── 2. VIP / high-priority order ──────────────────────
    banner("2 · VIP Order  (priority bumped to HIGH)")
    service.place_order("bob_vip", ["Biryani"], 600, vip=True)

    # ── 3. Idempotency — duplicate event ──────────────────
    banner("3 · Duplicate Event  (idempotency guard)")
    fixed_order = Order("fixed-id-001", "carol", ["Tea"], 30)
    broker.publish(fixed_order)
    broker.publish(fixed_order)   # ← should be silently rejected

    # ── 4. Invalid input ──────────────────────────────────
    banner("4 · Invalid Input  (empty user & items)")
    service.place_order("", [], 100)

    # ── 5. Rate limiting ──────────────────────────────────
    banner("5 · Rate Limiting  (cap = 3 requests/user)")
    for i in range(5):
        service.place_order("spammer", ["Coffee"], 50)

    # ── 6. Saga compensation ──────────────────────────────
    banner("6 · Saga Compensation  (manual rollback)")
    saga.start("manual-order-xyz")
    saga.complete_step("manual-order-xyz", "payment")
    saga.compensate("manual-order-xyz")   # simulate downstream failure

    # ── 7. Broker failure + Circuit Breaker ───────────────
    banner("7 · Broker Failure  (circuit breaker opens)")

    original_publish = broker.publish

    def always_fail(order):          # simulate a broken broker
        raise ConnectionError("Broker unreachable")

    broker.publish = always_fail
    service.place_order("dave", ["Sandwich"], 120)
    broker.publish = original_publish   # restore

    # ── 8. Dead Letter Queue inspection ───────────────────
    banner("8 · Dead Letter Queue  (parked failures)")
    if broker.dlq:
        for o in broker.dlq:
            err("DLQ entry", f"{o.order_id[:8]}…  user={o.user_id}  status={o.status}")
    else:
        dim("DLQ", "empty")

    # ── 9. Event Replay from DLQ ──────────────────────────
    banner("9 · Event Replay  (retry DLQ orders)")
    MAX_REPLAY_ATTEMPTS = 2
    publisher.breaker.reset()   # close the circuit before replay

    replayed = 0
    for order in list(broker.dlq):
        if order.retry_count >= MAX_REPLAY_ATTEMPTS:
            err("Replay", f"permanent failure — skipping {order.order_id[:8]}…")
            continue
        order.retry_count = 0
        info("Replay", f"re-publishing {order.order_id[:8]}…")
        publisher.publish(order)
        replayed += 1

    broker.dlq.clear()
    ok("Replay", f"{replayed} order(s) re-queued")

    # ── 10. Dispatch all queued events ────────────────────
    banner("10 · Dispatch  (deliver queued orders to services)")
    broker.dispatch_all()

    # ── 11. Graceful shutdown ─────────────────────────────
    banner("11 · Graceful Shutdown  (drain remaining queue)")
    if broker._queue.empty():
        ok("Shutdown", "queue is empty — safe to shut down")
    else:
        warn("Shutdown", "queue not empty — draining…")
        broker.dispatch_all()

    # ── 12. Final consistency summary ─────────────────────
    banner("12 · System Summary")
    ok("Processed",  f"{len(broker._seen)} unique order(s) accepted")
    info("DLQ",      f"{len(broker.dlq)} order(s) in dead letter queue")
    info("Saga",     f"{len(saga._state)} transaction(s) tracked")
    dim("Consumers", f"{len(broker._consumers)} service(s) registered")

    print(f"\n  {C.DIM}{'─' * 54}{C.RESET}\n")


if __name__ == "__main__":
    main()