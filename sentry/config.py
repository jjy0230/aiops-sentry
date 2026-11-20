import os
from pydantic import BaseModel, Field


class SentrySettings(BaseModel):
    """Configuration settings for AIOps-Sentry service."""

    app_name: str = "AIOps-Sentry"
    app_port: int = int(os.getenv("SENTRY_PORT", "8080"))
    app_host: str = os.getenv("SENTRY_HOST", "0.0.0.0")

    # Engine settings
    use_mock_engine: bool = os.getenv("SENTRY_USE_MOCK_ENGINE", "true").lower() in ("true", "1", "yes")
    jev_endpoint: str = os.getenv("SENTRY_JEV_ENDPOINT", "http://localhost:8000/v1/systemone")
    jev_model_name: str = os.getenv("SENTRY_JEV_MODEL", "anyjev-decision-base")
    request_timeout_ms: int = int(os.getenv("SENTRY_REQUEST_TIMEOUT_MS", "50"))

    # Safety Envelope & Blast Radius settings
    min_confidence_threshold: float = float(os.getenv("SENTRY_MIN_CONFIDENCE", "0.75"))
    min_margin_threshold: float = float(os.getenv("SENTRY_MIN_MARGIN", "0.20"))
    cooldown_seconds: int = int(os.getenv("SENTRY_COOLDOWN_SECONDS", "180"))
    max_affected_pod_ratio: float = float(os.getenv("SENTRY_MAX_POD_RATIO", "0.20"))

    # Human Notification
    feishu_webhook_url: str = os.getenv("SENTRY_FEISHU_WEBHOOK", "")
    dingtalk_webhook_url: str = os.getenv("SENTRY_DINGTALK_WEBHOOK", "")


settings = SentrySettings()
