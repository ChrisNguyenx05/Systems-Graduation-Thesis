from fastapi import APIRouter, Depends
from app.connectors.wsis import wsis_connector
from app.auth import get_current_user

router = APIRouter(prefix="/api/v1/wsis", tags=["WSIS Monitor"])

@router.get("/status")
def get_wsis_status(force: bool = False, current_user: dict = Depends(get_current_user)):
    return wsis_connector.get_status(force=force)

@router.get("/history")
def get_wsis_history(force: bool = False, current_user: dict = Depends(get_current_user)):
    status = wsis_connector.get_status(force=force)
    return {
        "history": status.get("historical_match_rates", []),
        "max_records": 10,
        "current_window": status.get("full_display_timestamp")
    }

@router.post("/refresh")
def refresh_wsis(current_user: dict = Depends(get_current_user)):
    return wsis_connector.get_status(force=True)

