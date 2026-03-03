import os
import json
from uuid import uuid4
from datetime import timedelta
from typing import Any, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, Response
from dapr.clients import DaprClient
from dapr.ext.workflow import (
    WorkflowRuntime,
    DaprWorkflowContext,
    WorkflowActivityContext,
    DaprWorkflowClient,
    RetryPolicy,
)

# -----------------------------
# Config
# -----------------------------
APP_ID = os.getenv("DAPR_APP_ID", "workflow-worker")
STATE_STORE = os.getenv("STATE_STORE", "kvstore")
INVENTORY_APP_ID = os.getenv("INVENTORY_APP_ID", "inventory-svc")
PAYMENT_APP_ID = os.getenv("PAYMENT_APP_ID", "payment-svc")
WORKFLOW_NAME = os.getenv("WORKFLOW_NAME", "order_processing_workflow")

print(
    f"[CONFIG] APP_ID={APP_ID} STATE_STORE={STATE_STORE} "
    f"WORKFLOW_NAME={WORKFLOW_NAME} INVENTORY_APP_ID={INVENTORY_APP_ID} PAYMENT_APP_ID={PAYMENT_APP_ID}"
)

wfr = WorkflowRuntime()


# -----------------------------
# FastAPI lifespan (start/shutdown workflow runtime)
# -----------------------------
@asynccontextmanager
async def lifespan(_app: FastAPI):
    wfr.start()
    try:
        yield
    finally:
        wfr.shutdown()


app = FastAPI(title=APP_ID, lifespan=lifespan)


# -----------------------------
# Helpers: State
# -----------------------------
def _order_key(order_id: str) -> str:
    return f"order:{order_id}"


def _get_state_json(store: str, key: str) -> dict[str, Any]:
    with DaprClient() as d:
        resp = d.get_state(store_name=store, key=key)
        if not resp.data:
            return {}
        try:
            return json.loads(resp.data.decode("utf-8"))
        except Exception:
            return {}


def _save_state_json(store: str, key: str, value: dict[str, Any]) -> None:
    body = json.dumps(value)
    with DaprClient() as d:
        d.save_state(store_name=store, key=key, value=body)


def set_order_status(order_id: str, status: str, extra: Optional[dict[str, Any]] = None) -> None:
    extra = extra or {}
    state = _get_state_json(STATE_STORE, _order_key(order_id))
    state["order_id"] = order_id
    state["status"] = status
    state.update(extra)
    print(f"[SET STATUS] order_id={order_id} status={status}")
    _save_state_json(STATE_STORE, _order_key(order_id), state)


# -----------------------------
# Activities
# -----------------------------
@wfr.activity(name="set_status_activity")
def set_status_activity(_ctx: WorkflowActivityContext, inp: dict[str, Any]) -> None:
    set_order_status(inp["order_id"], inp["status"], inp.get("extra"))


@wfr.activity(name="reserve_inventory")
def reserve_inventory(_ctx: WorkflowActivityContext, inp: dict[str, Any]) -> dict[str, Any]:
    payload = {"order_id": inp["order_id"], "sku": inp.get("sku"), "qty": inp.get("qty")}
    with DaprClient() as d:
        resp = d.invoke_method(
            app_id=INVENTORY_APP_ID,
            method_name="reserve",
            data=json.dumps(payload),
            http_verb="POST",
        )
    return json.loads(resp.data.decode("utf-8")) if resp.data else {}


@wfr.activity(name="charge_payment")
def charge_payment(_ctx: WorkflowActivityContext, inp: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "order_id": inp["order_id"],
        "customer_id": inp.get("customer_id"),
        "amount_usd": inp.get("amount_usd"),
    }
    with DaprClient() as d:
        resp = d.invoke_method(
            app_id=PAYMENT_APP_ID,
            method_name="charge",
            data=json.dumps(payload),
            http_verb="POST",
        )
    return json.loads(resp.data.decode("utf-8")) if resp.data else {}


# -----------------------------
# Orchestrator
# -----------------------------
@wfr.workflow(name=WORKFLOW_NAME)
def order_processing_workflow(ctx: DaprWorkflowContext, order: dict[str, Any]):
    order_id = order["order_id"]
    retry = RetryPolicy(first_retry_interval=timedelta(seconds=2), max_number_of_attempts=5)

    try:
        yield ctx.call_activity(set_status_activity, input={"order_id": order_id, "status": "PROCESSING"})

        reserve = yield ctx.call_activity(reserve_inventory, input=order, retry_policy=retry)
        yield ctx.call_activity(
            set_status_activity,
            input={"order_id": order_id, "status": "INVENTORY_RESERVED", "extra": reserve},
        )

        payment = yield ctx.call_activity(charge_payment, input=order, retry_policy=retry)
        yield ctx.call_activity(
            set_status_activity,
            input={"order_id": order_id, "status": "PAID", "extra": payment},
        )

        yield ctx.call_activity(set_status_activity, input={"order_id": order_id, "status": "COMPLETED"})
        return {"order_id": order_id, "result": "COMPLETED"}

    except Exception as e:
        print(f"[WORKFLOW FAILED] order_id={order_id} err={repr(e)}")
        try:
            yield ctx.call_activity(
                set_status_activity,
                input={"order_id": order_id, "status": "FAILED", "extra": {"error": str(e)}},
            )
        except Exception:
            pass
        raise


# -----------------------------
# Catalyst Subscription Endpoint
# -----------------------------
@app.post("/start-workflow")
async def start_workflow_from_message(request: Request):
    """
    Handles either:
      - {"order_id":"..."}  (start workflow for existing persisted order)
      - {"customer_id":"c-123","sku":"sku-1","qty":2,"amount_usd":19.99}  (create order & start)
    """

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid json")

    # Unwrap common envelope (CloudEvent: {"data": {...}})
    data: Any = payload.get("data") if isinstance(payload, dict) and "data" in payload else payload

    # If the broker sent JSON as a string, parse it
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            pass

    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="invalid message payload")

    # If caller provided order_id -> use existing flow
    order_id = data.get("order_id")

    # If no order_id, create a new order and persist it
    if not order_id:
        # basic validation for creation event
        if not data.get("customer_id") or not data.get("sku"):
            raise HTTPException(status_code=400, detail="missing order_id or required creation fields")

        order_id = str(uuid4())
        order = {
            "order_id": order_id,
            "customer_id": data.get("customer_id"),
            "sku": data.get("sku"),
            "qty": data.get("qty"),
            "amount_usd": data.get("amount_usd"),
            "status": "CREATED",
        }

        try:
            _save_state_json(STATE_STORE, _order_key(order_id), order)
            print(f"[ORDER CREATED] order_id={order_id} persisted")
        except Exception as e:
            print(f"[ERROR] saving new order {order_id}: {e}")
            raise HTTPException(status_code=500, detail="failed to persist new order")
    else:
        # Load existing order from state store
        order = _get_state_json(STATE_STORE, _order_key(order_id))
        if not order:
            # Return 500 to force retry rather than acking bad messages
            raise HTTPException(status_code=500, detail=f"order not found in state store: {order_id}")

    # Start workflow via Dapr Workflow client
    wf = DaprWorkflowClient()
    try:
        # Idempotency: if instance exists, ack and move on
        try:
            existing = wf.get_workflow_state(order_id)
            if existing is not None:
                return Response(status_code=204)
        except Exception:
            # If SDK call fails, proceed to schedule and let errors surface
            pass

        wf.schedule_new_workflow(
            workflow=order_processing_workflow,
            instance_id=order_id,
            input=order,
        )
    except Exception as e:
        print(f"[ERROR] failed to start workflow for order_id={order_id}: {e}")
        raise HTTPException(status_code=500, detail=f"failed to start workflow: {e}")

    return Response(status_code=204)


# -----------------------------
# Debug + health
# -----------------------------
@app.get("/debug/orders/{order_id}")
def debug_get_order(order_id: str):
    return _get_state_json(STATE_STORE, _order_key(order_id))


@app.get("/healthz")
def healthz():
    return {"ok": True, "app_id": APP_ID}

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=APP_PORT)
