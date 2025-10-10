from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
import asyncio
import logging
from contextlib import asynccontextmanager

from app.routers import reconciliation
from app.workers.reconciliation_scheduler import ReconciliationScheduler
from app.config.settings import get_settings
from app.config.logging_config import setup_logging

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info("Starting PEAR Reconciliation Service")
    
    # Start background scheduler
    scheduler = ReconciliationScheduler()
    scheduler_task = asyncio.create_task(scheduler.start())
    
    # Store scheduler task in app state for cleanup
    app.state.scheduler_task = scheduler_task
    app.state.scheduler = scheduler
    
    logger.info("Reconciliation scheduler started")
    
    yield
    
    # Shutdown
    logger.info("Shutting down PEAR Reconciliation Service")
    
    # Stop scheduler
    if hasattr(app.state, 'scheduler'):
        await app.state.scheduler.stop()
    
    # Cancel scheduler task
    if hasattr(app.state, 'scheduler_task'):
        app.state.scheduler_task.cancel()
        try:
            await app.state.scheduler_task
        except asyncio.CancelledError:
            pass
    
    logger.info("Reconciliation service shutdown complete")


# Create FastAPI application
app = FastAPI(
    title="PEAR Reconciliation Service",
    description="Ensures data consistency across PEAR microservices",
    version=settings.SERVICE_VERSION,
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(reconciliation.router, prefix="/reconciliation", tags=["reconciliation"])


@app.get("/")
async def root():
    """Root endpoint with service information"""
    return {
        "service": "PEAR Reconciliation Service",
        "version": settings.SERVICE_VERSION,
        "status": "running",
        "description": "Ensures data consistency across PEAR microservices"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # You can add more sophisticated health checks here
        # e.g., check database connectivity, RabbitMQ connectivity, etc.
        
        scheduler_status = "running" if hasattr(app.state, 'scheduler') else "stopped"
        
        return {
            "status": "healthy",
            "service": settings.SERVICE_NAME,
            "version": settings.SERVICE_VERSION,
            "scheduler": scheduler_status,
            "timestamp": asyncio.get_event_loop().time()
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "error": str(e),
                "service": settings.SERVICE_NAME
            }
        )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    logger.error(f"Unhandled exception in {request.url}: {str(exc)}", exc_info=True)
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": "An unexpected error occurred",
            "path": str(request.url)
        }
    )


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.PORT,
        reload=settings.RELOAD,
        log_level=settings.LOG_LEVEL.lower()
    )
