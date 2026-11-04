"""Health check router for liveness and readiness probes."""

from typing import Annotated

from fastapi import APIRouter, Depends, Response, status

from app.config import Settings, get_settings
from app.dependencies import get_directory_client
from app.models.schemas import LivenessResponse, ReadinessResponse
from app.services.directory_client import DirectoryClient

router = APIRouter(tags=["Health"])

ClientDep = Annotated[DirectoryClient, Depends(get_directory_client)]
SettingsDep = Annotated[Settings, Depends(get_settings)]


@router.get(
    "/health",
    response_model=LivenessResponse,
    status_code=status.HTTP_200_OK,
    summary="Liveness Health Check",
)
async def health_check() -> LivenessResponse:
    """Fast local liveness check without downstream dependencies."""
    return LivenessResponse(status="ok")


@router.get(
    "/ready",
    response_model=ReadinessResponse,
    status_code=status.HTTP_200_OK,
    summary="Readiness Check",
)
async def readiness_check(
    response: Response,
    client: ClientDep,
    settings: SettingsDep,
) -> ReadinessResponse:
    """Verify connectivity to target Directory API; returns 503 if unavailable."""
    try:
        res = await client.check_health(timeout_seconds=3.0)
        is_healthy = res.get("status") == "OK"
        if is_healthy:
            return ReadinessResponse(
                status="ok",
                directory_api_status="healthy",
                directory_api_url=settings.directory_api_base_url,
            )
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return ReadinessResponse(
            status="unhealthy",
            directory_api_status="degraded",
            directory_api_url=settings.directory_api_base_url,
        )
    except Exception:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return ReadinessResponse(
            status="unhealthy",
            directory_api_status="unhealthy",
            directory_api_url=settings.directory_api_base_url,
        )
