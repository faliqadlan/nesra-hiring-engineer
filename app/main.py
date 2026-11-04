"""HTTP routes. You should not need to change this file."""

from contextlib import asynccontextmanager
from datetime import date
import logging

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from app import raw, reports

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")


@asynccontextmanager
async def lifespan(_: FastAPI):
    with raw.lock:
        reports.startup()
    yield


app = FastAPI(lifespan=lifespan)


@app.exception_handler(NotImplementedError)
async def not_implemented(_: Request, exc: NotImplementedError):
    return JSONResponse(status_code=501, content={"detail": str(exc)})


@app.get("/shops/{shop_id}/weekly-report")
def weekly_report(shop_id: str, week: str = Query(pattern=r"^\d{4}-W\d{2}$", examples=["2025-W11"])):
    try:
        week_start = date.fromisocalendar(int(week[:4]), int(week[6:]), 1)
    except ValueError:
        raise HTTPException(422, f"{week} is not a valid ISO week") from None
    with raw.lock:
        return reports.weekly_report(shop_id, week_start)


@app.get("/stats")
def stats():
    return raw.stats()


@app.post("/load-delta")
def load_delta():
    with raw.lock:
        if not raw.land_delta():
            return {"loaded": False, "detail": "delta was already loaded"}
        reports.on_delta_loaded()
    return {"loaded": True}
