from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from typing import List, Optional
from datetime import datetime, timedelta
import logging

from app.services.reconciler_service import ReconcilerService
from app.config.field_mappings import RecordType

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/run")
async def run_reconciliation(
    background_tasks: BackgroundTasks,
    hours_back: int = Query(1, ge=1, le=168, description="Hours to look back"),
    record_types: Optional[List[str]] = Query(None, description="Specific record types to reconcile"),
    force: bool = Query(False, description="Force reconciliation even if recently run")
):
    """
    Trigger manual reconciliation process.
    
    Args:
        hours_back: Number of hours to look back for changes
        record_types: Optional list of specific record types to reconcile
        force: Force reconciliation even if recently completed
    
    Returns:
        Reconciliation job details
    """
    try:
        logger.info(f"Manual reconciliation triggered - hours_back: {hours_back}, record_types: {record_types}")
        
        # Validate record types if provided
        if record_types:
            valid_types = [rt.value for rt in RecordType]
            invalid_types = [rt for rt in record_types if rt not in valid_types]
            if invalid_types:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Invalid record types: {invalid_types}. Valid types: {valid_types}"
                )
        
        # Create reconciliation job
        reconciler = ReconcilerService()
        
        # Run reconciliation in background
        job_id = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        background_tasks.add_task(
            reconciler.run_reconciliation,
            hours_back=hours_back,
            record_types=record_types,
            job_id=job_id
        )
        
        return {
            "job_id": job_id,
            "status": "started",
            "hours_back": hours_back,
            "record_types": record_types or "all",
            "started_at": datetime.now().isoformat(),
            "message": "Reconciliation job started in background"
        }
        
    except Exception as e:
        logger.error(f"Failed to start reconciliation: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to start reconciliation: {str(e)}")


@router.get("/status")
async def get_reconciliation_status():
    """
    Get current reconciliation status and recent history.
    """
    try:
        reconciler = ReconcilerService()
        status = await reconciler.get_status()
        
        return {
            "current_status": status.get("current_job"),
            "last_completed": status.get("last_completed"),
            "next_scheduled": status.get("next_scheduled"),
            "recent_jobs": status.get("recent_jobs", []),
            "system_health": status.get("health", {})
        }
        
    except Exception as e:
        logger.error(f"Failed to get reconciliation status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get status: {str(e)}")


@router.get("/history")
async def get_reconciliation_history(
    limit: int = Query(10, ge=1, le=100, description="Number of recent jobs to return"),
    days_back: int = Query(7, ge=1, le=30, description="Number of days to look back")
):
    """
    Get reconciliation history for monitoring and analysis.
    """
    try:
        reconciler = ReconcilerService()
        history = await reconciler.get_history(limit=limit, days_back=days_back)
        
        return {
            "total_jobs": len(history),
            "period_days": days_back,
            "jobs": history,
            "summary": {
                "successful_jobs": len([j for j in history if j.get("status") == "completed"]),
                "failed_jobs": len([j for j in history if j.get("status") == "failed"]),
                "total_drifts_detected": sum(j.get("total_drifts", 0) for j in history),
                "avg_execution_time_seconds": sum(j.get("execution_time_seconds", 0) for j in history) / len(history) if history else 0
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to get reconciliation history: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get history: {str(e)}")


@router.get("/drift/{record_type}")
async def get_drift_details(
    record_type: str,
    hours_back: int = Query(24, ge=1, le=168),
    include_resolved: bool = Query(False, description="Include already resolved drifts")
):
    """
    Get detailed drift information for a specific record type.
    """
    try:
        # Validate record type
        if record_type not in [rt.value for rt in RecordType]:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid record type: {record_type}"
            )
        
        reconciler = ReconcilerService()
        drift_details = await reconciler.get_drift_details(
            record_type=record_type,
            hours_back=hours_back,
            include_resolved=include_resolved
        )
        
        return {
            "record_type": record_type,
            "window_hours": hours_back,
            "drift_count": len(drift_details.get("drifts", [])),
            "drifts": drift_details.get("drifts", []),
            "summary": drift_details.get("summary", {}),
            "generated_at": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get drift details for {record_type}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get drift details: {str(e)}")


@router.post("/resolve-drift")
async def resolve_drift(
    background_tasks: BackgroundTasks,
    record_type: str,
    record_id: int,
    force_resolution: bool = Query(False, description="Force resolution even if recently attempted")
):
    """
    Manually trigger resolution for a specific drift.
    """
    try:
        # Validate record type
        if record_type not in [rt.value for rt in RecordType]:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid record type: {record_type}"
            )
        
        logger.info(f"Manual drift resolution triggered - type: {record_type}, id: {record_id}")
        
        reconciler = ReconcilerService()
        
        # Create resolution job
        resolution_id = f"resolve_{record_type}_{record_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        background_tasks.add_task(
            reconciler.resolve_specific_drift,
            record_type=record_type,
            record_id=record_id,
            resolution_id=resolution_id,
            force=force_resolution
        )
        
        return {
            "resolution_id": resolution_id,
            "status": "started",
            "record_type": record_type,
            "record_id": record_id,
            "started_at": datetime.now().isoformat(),
            "message": "Drift resolution started in background"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to start drift resolution: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to start drift resolution: {str(e)}")


@router.get("/metrics")
async def get_reconciliation_metrics():
    """
    Get reconciliation metrics for monitoring and alerting.
    """
    try:
        reconciler = ReconcilerService()
        metrics = await reconciler.get_metrics()
        
        return {
            "timestamp": datetime.now().isoformat(),
            "reconciliation_metrics": metrics.get("reconciliation", {}),
            "drift_metrics": metrics.get("drift", {}),
            "performance_metrics": metrics.get("performance", {}),
            "health_metrics": metrics.get("health", {})
        }
        
    except Exception as e:
        logger.error(f"Failed to get reconciliation metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get metrics: {str(e)}")


@router.get("/config")
async def get_reconciliation_config():
    """
    Get current reconciliation configuration.
    """
    try:
        reconciler = ReconcilerService()
        config = reconciler.get_configuration()
        
        return {
            "service_mappings": config.get("service_mappings", {}),
            "field_mappings": config.get("field_mappings", {}),
            "schedule_config": config.get("schedule", {}),
            "thresholds": config.get("thresholds", {}),
            "endpoints": config.get("endpoints", {})
        }
        
    except Exception as e:
        logger.error(f"Failed to get reconciliation config: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get config: {str(e)}")
