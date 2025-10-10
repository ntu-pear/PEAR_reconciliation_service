import asyncio
import httpx
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta

from app.config.field_mappings import (
    FIELD_MAPPINGS, RecordType, ServiceType, ServiceMapping,
    get_service_configs, get_authoritative_service_for_record_type,
    get_eventual_service_for_record_type, transform_eventual_to_authoritative_format
)
from app.config.settings import get_settings
from app.messaging.publisher import get_drift_publisher

logger = logging.getLogger(__name__)
settings = get_settings()


class ReconcilerService:
    """
    Main reconciler service that handles drift detection between services.
    """
    
    def __init__(self):
        self.field_mappings = FIELD_MAPPINGS
        self.service_configs = get_service_configs()
        self.job_history = []  # In production, this would be stored in a database
        self.drift_publisher = None  # Will be initialized on first use
        
    def _get_drift_publisher(self):
        """Get or create drift publisher instance"""
        if self.drift_publisher is None:
            self.drift_publisher = get_drift_publisher()
        return self.drift_publisher
        
    async def run_reconciliation(
        self, 
        hours_back: int = 1,
        record_types: Optional[List[str]] = None,
        job_id: Optional[str] = None,
        publish_drifts: bool = True
    ) -> Dict[str, Any]:
        """
        Run full reconciliation for specified or all configured record types.
        
        Args:
            hours_back: Number of hours to look back for changes
            record_types: Specific record types to reconcile (None = all)
            job_id: Optional job identifier
            publish_drifts: Whether to publish drift notifications to RabbitMQ
        """
        job_id = job_id or f"recon_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        start_time = datetime.now()
        
        logger.info(f"Starting reconciliation job {job_id} - hours_back: {hours_back}, publish_drifts: {publish_drifts}")
        
        results = {
            "reconciliation_id": job_id,
            "started_at": start_time.isoformat(),
            "hours_back": hours_back,
            "record_types": {},
            "summary": {
                "total_drifts": 0,
                "drifts_published": 0,
                "by_type": {},
                "by_service": {}
            },
            "status": "running"
        }
        
        # Determine which record types to process
        types_to_process = []
        if record_types:
            for rt in record_types:
                try:
                    types_to_process.append(RecordType(rt))
                except ValueError:
                    logger.warning(f"Invalid record type: {rt}")
        else:
            types_to_process = list(self.field_mappings.keys())
        
        # Process each record type
        for record_type in types_to_process:
            try:
                logger.info(f"Processing reconciliation for {record_type.value}")
                
                type_results = await self.reconcile_record_type(
                    record_type=record_type,
                    hours_back=hours_back,
                    publish_drifts=publish_drifts
                )
                
                results["record_types"][record_type.value] = type_results
                results["summary"]["total_drifts"] += len(type_results["drifts"])
                results["summary"]["drifts_published"] += type_results.get("drifts_published", 0)
                results["summary"]["by_type"][record_type.value] = len(type_results["drifts"])
                
            except Exception as e:
                logger.error(f"Failed to reconcile {record_type.value}: {str(e)}")
                results["record_types"][record_type.value] = {
                    "error": str(e),
                    "drifts": [],
                    "status": "failed"
                }
        
        # Complete the job
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        results["completed_at"] = end_time.isoformat()
        results["execution_time_seconds"] = execution_time
        results["status"] = "completed"
        
        # Store job history
        self.job_history.append(results)
        
        logger.info(
            f"Reconciliation job {job_id} completed in {execution_time:.2f}s - "
            f"{results['summary']['total_drifts']} drifts found, "
            f"{results['summary']['drifts_published']} published"
        )
        
        return results
    
    async def reconcile_record_type(
        self, 
        record_type: RecordType, 
        hours_back: int = 1,
        publish_drifts: bool = True
    ) -> Dict[str, Any]:
        """
        Reconcile a specific record type between authoritative and eventual services.
        """
        mapping = self.field_mappings[record_type]
        
        # Determine which services to query
        auth_service = get_authoritative_service_for_record_type(record_type)
        eventual_service = get_eventual_service_for_record_type(record_type)
        
        try:
            # Fetch data from both services concurrently
            auth_data, eventual_data = await asyncio.gather(
                self.fetch_integrity_data(auth_service, mapping.authoritative_endpoint, hours_back),
                self.fetch_integrity_data(eventual_service, mapping.eventual_endpoint, hours_back),
                return_exceptions=True
            )
            
            if isinstance(auth_data, Exception):
                raise auth_data
            if isinstance(eventual_data, Exception):
                raise eventual_data
            
            # Transform and compare data
            auth_records = auth_data.get("records", [])
            eventual_records = self.transform_records(eventual_data.get("records", []), mapping)
            
            # Detect drifts
            drifts = self.detect_drifts(auth_records, eventual_records, mapping)
            
            # Publish drift notifications if enabled
            drifts_published = 0
            if publish_drifts and drifts:
                drifts_published = await self._publish_drifts(drifts, record_type)
            
            return {
                "record_type": record_type.value,
                "authoritative_service": auth_service,
                "eventual_service": eventual_service,
                "auth_count": len(auth_records),
                "eventual_count": len(eventual_records),
                "drifts": drifts,
                "drifts_published": drifts_published,
                "drift_summary": {
                    "missing": len([d for d in drifts if d["type"] == "missing"]),
                    "stale": len([d for d in drifts if d["type"] == "stale"]),
                    "orphaned": len([d for d in drifts if d["type"] == "orphaned"])
                },
                "status": "completed"
            }
            
        except Exception as e:
            logger.error(f"Error reconciling {record_type.value}: {str(e)}")
            return {
                "record_type": record_type.value,
                "error": str(e),
                "drifts": [],
                "drifts_published": 0,
                "status": "failed"
            }
    
    async def _publish_drifts(self, drifts: List[Dict[str, Any]], record_type: RecordType) -> int:
        """
        Publish drift notifications to RabbitMQ.
        
        Returns:
            Number of drifts successfully published
        """
        try:
            publisher = self._get_drift_publisher()
            published_count = 0
            
            for drift in drifts:
                success = publisher.publish_drift_detected(
                    record_type=drift["record_type"],
                    record_id=drift["record_id"],
                    details={
                        "auth_timestamp": drift.get("auth_timestamp"),
                        "eventual_timestamp": drift.get("eventual_timestamp"),
                        "auth_modified_date": drift.get("auth_modified_date"),
                        "eventual_modified_date": drift.get("eventual_modified_date"),
                        "time_drift_ms": drift.get("time_drift_ms"),
                        "detected_at": drift.get("detected_at")
                    }
                )
                
                if success:
                    published_count += 1
            
            logger.info(f"Published {published_count}/{len(drifts)} drifts for {record_type.value}")
            return published_count
            
        except Exception as e:
            logger.error(f"Error publishing drifts: {str(e)}")
            return 0
    
    async def fetch_integrity_data(
        self, 
        service_name: str, 
        endpoint: str, 
        hours_back: int
    ) -> Dict[str, Any]:
        """
        Fetch integrity data from a specific service endpoint.
        """
        config = self.service_configs[service_name]
        url = f"{config['base_url']}{endpoint}"
        
        params = {
            "hours_back": hours_back,
            "limit": settings.MAX_RECORDS_PER_REQUEST
        }
        
        logger.debug(f"Fetching integrity data from {url}")
        
        timeout = httpx.Timeout(config["timeout"])
        
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                return response.json()
        except httpx.ConnectError as e:
            logger.error(f"Cannot connect to {service_name} service at {url}: {str(e)}")
            raise ConnectionError(f"Cannot connect to {service_name} service at {url}")
        except httpx.TimeoutException as e:
            logger.error(f"Timeout connecting to {service_name} service at {url}: {str(e)}")
            raise TimeoutError(f"Timeout connecting to {service_name} service")
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error from {service_name} service: {e.response.status_code} - {e.response.text}")
            raise RuntimeError(f"HTTP {e.response.status_code} from {service_name} service")
    
    def transform_records(
        self, 
        records: List[Dict], 
        mapping: ServiceMapping
    ) -> List[Dict]:
        """
        Transform eventual service records to authoritative format for comparison.
        """
        logger.debug(f"Transforming {len(records)} records for {mapping.record_type.value}")
        transformed = []
        
        for i, record in enumerate(records):
            transformed_record = transform_eventual_to_authoritative_format(record, mapping)
            if i == 0:  # Log first record for debugging
                logger.debug(f"Sample transformation: {record} -> {transformed_record}")
            transformed.append(transformed_record)
        
        logger.debug(f"Transformation complete: {len(transformed)} records")
        return transformed
    
    def detect_drifts(
        self, 
        auth_records: List[Dict], 
        eventual_records: List[Dict], 
        mapping: ServiceMapping
    ) -> List[Dict]:
        """
        Detect drifts between authoritative and eventual records.
        """
        # Get the primary ID field for comparison
        primary_id_field = list(mapping.id_field_mapping.values())[0]
        
        logger.debug(f"Detecting drifts for {mapping.record_type.value}")
        logger.debug(f"Primary ID field: {primary_id_field}")
        logger.debug(f"Auth records count: {len(auth_records)}")
        logger.debug(f"Eventual records count: {len(eventual_records)}")
        
        # Build dictionaries for comparison
        auth_dict = {}
        for record in auth_records:
            if primary_id_field in record:
                auth_dict[record[primary_id_field]] = record
            else:
                logger.warning(f"Auth record missing primary ID field '{primary_id_field}': {record}")
        
        eventual_dict = {}
        for record in eventual_records:
            if primary_id_field in record:
                eventual_dict[record[primary_id_field]] = record
            else:
                logger.warning(f"Eventual record missing primary ID field '{primary_id_field}': {record}")
        
        logger.debug(f"Auth dict keys: {list(auth_dict.keys())}")
        logger.debug(f"Eventual dict keys: {list(eventual_dict.keys())}")
        
        drifts = []
        
        # Check for missing and stale records
        for record_id, auth_record in auth_dict.items():
            eventual_record = eventual_dict.get(record_id)
            
            if not eventual_record:
                # Record missing in eventual service
                logger.info(f"Missing record detected: {mapping.record_type.value} ID {record_id}")
                drifts.append({
                    "type": "missing",
                    "record_id": record_id,
                    "record_type": mapping.record_type.value,
                    "auth_timestamp": auth_record.get("version_timestamp"),
                    "eventual_timestamp": None,
                    "auth_modified_date": auth_record.get("modified_date"),
                    "eventual_modified_date": None,
                    "detected_at": datetime.now().isoformat()
                })
            else:
                # Compare timestamps
                auth_ts = auth_record.get("version_timestamp", 0)
                eventual_ts = eventual_record.get("version_timestamp", 0)
                
                logger.debug(f"Comparing record {record_id}: auth_ts={auth_ts}, eventual_ts={eventual_ts}")
                
                if auth_ts > eventual_ts:
                    # Eventual service has stale data
                    time_drift_ms = auth_ts - eventual_ts
                    
                    logger.info(f"Stale record detected: {mapping.record_type.value} ID {record_id}, drift={time_drift_ms}ms")
                    logger.debug(f"Auth modified: {auth_record.get('modified_date')}, Eventual modified: {eventual_record.get('modified_date')}")
                    
                    drifts.append({
                        "type": "stale",
                        "record_id": record_id,
                        "record_type": mapping.record_type.value,
                        "auth_timestamp": auth_ts,
                        "eventual_timestamp": eventual_ts,
                        "time_drift_ms": time_drift_ms,
                        "auth_modified_date": auth_record.get("modified_date"),
                        "eventual_modified_date": eventual_record.get("modified_date"),
                        "detected_at": datetime.now().isoformat()
                    })
        
        # Check for orphaned records (in eventual but not in auth)
        for record_id, eventual_record in eventual_dict.items():
            if record_id not in auth_dict:
                logger.info(f"Orphaned record detected: {mapping.record_type.value} ID {record_id}")
                drifts.append({
                    "type": "orphaned",
                    "record_id": record_id,
                    "record_type": mapping.record_type.value,
                    "auth_timestamp": None,
                    "eventual_timestamp": eventual_record.get("version_timestamp"),
                    "auth_modified_date": None,
                    "eventual_modified_date": eventual_record.get("modified_date"),
                    "detected_at": datetime.now().isoformat()
                })
        
        logger.info(f"Drift detection complete: {len(drifts)} drifts found for {mapping.record_type.value}")
        return drifts
    
    async def get_status(self) -> Dict[str, Any]:
        """Get current reconciliation status"""
        recent_jobs = self.job_history[-5:] if self.job_history else []
        last_completed = recent_jobs[-1] if recent_jobs else None
        
        return {
            "last_completed": last_completed,
            "recent_jobs": recent_jobs,
            "health": {
                "service_status": "running",
                "last_check": datetime.now().isoformat()
            }
        }
    
    async def get_history(self, limit: int = 10, days_back: int = 7) -> List[Dict]:
        """Get reconciliation history"""
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        # Filter jobs within the date range
        filtered_jobs = [
            job for job in self.job_history
            if datetime.fromisoformat(job["started_at"]) >= cutoff_date
        ]
        
        # Return most recent jobs up to limit
        return filtered_jobs[-limit:] if filtered_jobs else []
    
    async def get_drift_details(
        self, 
        record_type: str, 
        hours_back: int = 24, 
        include_resolved: bool = False
    ) -> Dict[str, Any]:
        """Get detailed drift information for a specific record type"""
        # This would query a drift tracking database in production
        # For now, return mock data
        return {
            "drifts": [],
            "summary": {
                "total": 0,
                "missing": 0,
                "stale": 0,
                "orphaned": 0
            }
        }
    
    async def resolve_specific_drift(
        self, 
        record_type: str, 
        record_id: int, 
        resolution_id: str,
        force: bool = False
    ):
        """Resolve a specific drift by republishing the event"""
        logger.info(f"Resolving drift {resolution_id} for {record_type}:{record_id}")
        # Implementation would republish the event to RabbitMQ
        # This is a placeholder
        pass
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get reconciliation metrics"""
        return {
            "reconciliation": {
                "total_jobs": len(self.job_history),
                "successful_jobs": len([j for j in self.job_history if j.get("status") == "completed"]),
                "failed_jobs": len([j for j in self.job_history if j.get("status") == "failed"])
            },
            "drift": {
                "total_drifts_detected": sum(j.get("summary", {}).get("total_drifts", 0) for j in self.job_history),
                "total_drifts_published": sum(j.get("summary", {}).get("drifts_published", 0) for j in self.job_history)
            },
            "performance": {
                "avg_execution_time": sum(j.get("execution_time_seconds", 0) for j in self.job_history) / len(self.job_history) if self.job_history else 0
            },
            "health": {
                "service_healthy": True,
                "last_check": datetime.now().isoformat()
            }
        }
    
    def get_configuration(self) -> Dict[str, Any]:
        """Get current reconciliation configuration"""
        return {
            "service_mappings": {rt.value: mapping.__dict__ for rt, mapping in self.field_mappings.items()},
            "field_mappings": {rt.value: mapping.id_field_mapping for rt, mapping in self.field_mappings.items()},
            "schedule": {
                "cron_expression": settings.RECONCILIATION_SCHEDULE,
                "window_hours": settings.RECONCILIATION_WINDOW_HOURS
            },
            "endpoints": self.service_configs
        }
