import os
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.config import settings
from app.connectors.airflow import airflow_connector
from app.connectors.clickhouse import clickhouse_connector
from app.connectors.minio_client import minio_connector
from app.connectors.wsis import wsis_connector

from app.routers.auth_routes import router as auth_router
from app.routers.matrix_routes import router as matrix_router
from app.routers.rerun_routes import router as rerun_router
from app.routers.airflow_routes import router as airflow_router
from app.routers.lakehouse_routes import router as lakehouse_router
from app.routers.wsis_routes import router as wsis_router
from app.routers.audit_routes import router as audit_router
from app.routers.schedule_routes import router as schedule_router
from app.routers.trading_pair_routes import router as trading_pair_router
from app.routers.catalog_routes import router as catalog_router
from app.routers.profile_routes import router as profile_router
from app.routers.notification_routes import router as notification_router
from app.routers.ai_models_routes import router as ai_models_router

app = FastAPI(
    title="NHFinance Admin Portal - Ecosystem Command Center",
    description="Central Orchestration & Operations Gateway for Airflow, ClickHouse, MinIO, WSIS, and SYS-04 AI Models",
    version="1.1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Routers
app.include_router(auth_router)
app.include_router(matrix_router)
app.include_router(rerun_router)
app.include_router(airflow_router)
app.include_router(lakehouse_router)
app.include_router(wsis_router)
app.include_router(audit_router)
app.include_router(schedule_router)
app.include_router(trading_pair_router)
app.include_router(catalog_router)
app.include_router(profile_router)
app.include_router(notification_router)
app.include_router(ai_models_router)

@app.get("/healthz")
def healthz():
    return {"status": "ok", "app": "admin-portal"}

@app.get("/readyz")
def readyz():
    ws_health = wsis_connector.check_health()
    ch_health = clickhouse_connector.get_health()
    return {
        "status": "ready",
        "airflow": airflow_connector.get_health(),
        "clickhouse": ch_health,
        "minio": minio_connector.get_health(),
        "wsis": ws_health,
        "ai_models": {"status": "ONLINE" if ch_health.get("online", False) else "OFFLINE", "subsystem": "SYS-04", "count": 3}
    }

@app.get("/api/v1/system/status")
def get_system_status():
    """Quick scan of all 5 connected subsystems with real health probes."""
    af = airflow_connector.get_health()
    ch = clickhouse_connector.get_health()
    mn = minio_connector.get_health()
    ws_health = wsis_connector.check_health()

    return {
        "services": [
            {
                "id": "airflow",
                "name": "Apache Airflow",
                "system": "SYS-02",
                "port": 8085,
                "type": "Orchestrator",
                "online": af.get("online", False),
                "url": settings.AIRFLOW_URL
            },
            {
                "id": "clickhouse",
                "name": "ClickHouse OLAP",
                "system": "SYS-05",
                "port": settings.CLICKHOUSE_PORT,
                "type": "Data Warehouse",
                "online": ch.get("online", False),
                "url": f"http://{settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_PORT}"
            },
            {
                "id": "minio",
                "name": "MinIO Lakehouse",
                "system": "SYS-05",
                "port": 9000,
                "type": "Object Storage",
                "online": mn.get("online", False),
                "url": f"http://{settings.MINIO_ENDPOINT}"
            },
            {
                "id": "wsis",
                "name": "WebSocket Ingestion (WSIS)",
                "system": "WSIS",
                "port": settings.WSIS_PORT,
                "type": "Realtime Stream",
                "online": ws_health.get("online", False),
                "url": f"http://{settings.WSIS_HOST}:{settings.WSIS_PORT}",
                "status": ws_health.get("status", "OFFLINE")
            },
            {
                "id": "aimodels",
                "name": "AI Models & Governance",
                "system": "SYS-04",
                "port": 8090,
                "type": "ML Inference & Risk",
                "online": ch.get("online", False),
                "url": "/api/v1/ai-models/summary"
            }
        ]
    }

# Mount Frontend static files
frontend_candidates = [
    Path(__file__).resolve().parent.parent.parent / "frontend",
    Path("/frontend"),
    Path(__file__).resolve().parent.parent / "frontend",
    Path("frontend")
]
frontend_dir = None
for candidate in frontend_candidates:
    if candidate.exists() and (candidate / "index.html").exists():
        frontend_dir = candidate
        break

if frontend_dir and frontend_dir.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")

    @app.get("/")
    def serve_index():
        index_file = frontend_dir / "index.html"
        if index_file.exists():
            return FileResponse(str(index_file))
        return {"message": "Admin Portal backend running. Frontend index.html not found."}
else:
    @app.get("/")
    def serve_fallback():
        return {"message": "Admin Portal API running. Access /docs for OpenAPI documentation."}
