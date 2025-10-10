import asyncio
import schedule
import time
import logging
from datetime import datetime
from typing import Optional

from app.services.reconciler_service import ReconcilerService
from app.config.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class ReconciliationScheduler:
    """
    Background scheduler for running reconciliation jobs at specified intervals.
    """
    
    def __init__(self):
        self.reconciler = ReconcilerService()
        self.running = False
        
    async def start(self):
        """Start the reconciliation scheduler"""
        logger.info("Starting reconciliation scheduler")
        self.running = True
        
        # Parse the RECONCILIATION_SCHEDULE from .env
        # Format: "minute hour day month day_of_week"
        # Example: "23 * * * *" means run at minute 23 of every hour
        cron_parts = settings.RECONCILIATION_SCHEDULE.split()
        
        if len(cron_parts) >= 2:
            minute = cron_parts[0]
            hour = cron_parts[1]
            
            # Schedule based on the cron expression
            if hour == '*' and minute != '*':
                # Run at specific minute every hour (e.g., "23 * * * *")
                schedule.every().hour.at(f":{minute}").do(
                    lambda: asyncio.create_task(self.run_scheduled_reconciliation())
                )
                logger.info(f"Scheduled reconciliation to run at minute {minute} of every hour")
            elif hour != '*' and minute != '*':
                # Run at specific time every day (e.g., "23 14 * * *" = 14:23 daily)
                schedule.every().day.at(f"{hour}:{minute}").do(
                    lambda: asyncio.create_task(self.run_scheduled_reconciliation())
                )
                logger.info(f"Scheduled reconciliation to run daily at {hour}:{minute}")
            else:
                # Fallback to every hour
                schedule.every().hour.do(
                    lambda: asyncio.create_task(self.run_scheduled_reconciliation())
                )
                logger.info(f"Scheduled reconciliation to run every hour (fallback)")
        else:
            # Invalid format, use default
            schedule.every().hour.do(
                lambda: asyncio.create_task(self.run_scheduled_reconciliation())
            )
            logger.warning(
                f"Invalid RECONCILIATION_SCHEDULE format: {settings.RECONCILIATION_SCHEDULE}. "
                f"Using default: every hour"
            )
        
        logger.info(f"Using schedule from .env: {settings.RECONCILIATION_SCHEDULE}")
        
        # Main scheduler loop
        while self.running:
            try:
                schedule.run_pending()
                await asyncio.sleep(60)  # Check every minute
            except Exception as e:
                logger.error(f"Error in scheduler loop: {str(e)}")
                await asyncio.sleep(60)
    
    async def stop(self):
        """Stop the reconciliation scheduler"""
        logger.info("Stopping reconciliation scheduler")
        self.running = False
        schedule.clear()
    
    async def run_scheduled_reconciliation(self):
        """Run the actual reconciliation process"""
        try:
            job_id = f"scheduled_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            logger.info(f"Starting scheduled reconciliation job: {job_id}")
            
            results = await self.reconciler.run_reconciliation(
                hours_back=settings.RECONCILIATION_WINDOW_HOURS,
                job_id=job_id
            )
            
            total_drifts = results.get("summary", {}).get("total_drifts", 0)
            execution_time = results.get("execution_time_seconds", 0)
            
            logger.info(
                f"Scheduled reconciliation {job_id} completed in {execution_time:.2f}s - "
                f"{total_drifts} drifts detected"
            )
            
            # Log drift details if any found
            if total_drifts > 0:
                logger.warning(f"Data drifts detected in scheduled reconciliation {job_id}:")
                for record_type, type_results in results.get("record_types", {}).items():
                    drift_count = len(type_results.get("drifts", []))
                    if drift_count > 0:
                        logger.warning(f"  - {record_type}: {drift_count} drifts")
            
        except Exception as e:
            logger.error(f"Scheduled reconciliation failed: {str(e)}")
    
    async def run_full_reconciliation(self):
        """Run a full reconciliation with extended time window"""
        try:
            job_id = f"full_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            logger.info(f"Starting full reconciliation job: {job_id}")
            
            results = await self.reconciler.run_reconciliation(
                hours_back=24,
                job_id=job_id
            )
            
            total_drifts = results.get("summary", {}).get("total_drifts", 0)
            execution_time = results.get("execution_time_seconds", 0)
            
            logger.info(
                f"Full reconciliation {job_id} completed in {execution_time:.2f}s - "
                f"{total_drifts} drifts detected"
            )
            
        except Exception as e:
            logger.error(f"Full reconciliation failed: {str(e)}")


# Standalone function for running as a separate process
async def run_standalone_scheduler():
    """Run the scheduler as a standalone process"""
    logger.info("Starting standalone reconciliation scheduler")
    
    scheduler = ReconciliationScheduler()
    
    try:
        await scheduler.start()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Scheduler error: {str(e)}")
    finally:
        await scheduler.stop()
        logger.info("Scheduler stopped")


if __name__ == "__main__":
    asyncio.run(run_standalone_scheduler())
