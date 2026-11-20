"""
Human-in-the-Loop Approval Gateway:
Generates signed, tamper-proof, single-use approval tokens and rich interactive notification cards
for Feishu / DingTalk chat ops authorization.
"""

import hmac
import hashlib
import json
import base64
import time
import uuid
from typing import Dict, Any, Optional, Tuple
from pydantic import BaseModel, Field

from sentry.schemas import RemediationAction, FailureContext, ArbitrationResult
from sentry.config import settings


class ApprovalTokenPayload(BaseModel):
    token_id: str
    trace_id: str
    action: RemediationAction
    service_name: str
    namespace: str
    pod_name: Optional[str] = None
    reason: str
    expires_at: float


class ApprovalTokenManager:
    """
    Manages generation, signature verification, and replay protection of approval tokens.
    """

    def __init__(self, secret: str = "sentry-secret-salt-prod-key", ttl_seconds: int = 300):
        self.secret = secret.encode("utf-8")
        self.ttl_seconds = ttl_seconds
        self._consumed_tokens: set = set()

    def create_token(
        self,
        action: RemediationAction,
        ctx: FailureContext,
        reason: str,
        trace_id: Optional[str] = None,
    ) -> str:
        tid = trace_id or str(uuid.uuid4())
        token_id = str(uuid.uuid4())
        expires_at = time.time() + self.ttl_seconds

        payload = {
            "token_id": token_id,
            "trace_id": tid,
            "action": action.value,
            "service_name": ctx.service_name,
            "namespace": ctx.namespace,
            "pod_name": ctx.pod_name,
            "reason": reason,
            "expires_at": expires_at,
        }

        payload_bytes = json.dumps(payload, sort_keys=True).encode("utf-8")
        sig = hmac.new(self.secret, payload_bytes, hashlib.sha256).hexdigest()

        combined = f"{base64.urlsafe_b64encode(payload_bytes).decode('utf-8')}.{sig}"
        return combined

    def verify_and_consume(self, token_str: str) -> Tuple[bool, Optional[ApprovalTokenPayload], str]:
        """
        Validates token integrity, expiration, and ensures single-use replay protection.
        Returns: (valid, payload, error_message)
        """
        try:
            parts = token_str.split(".")
            if len(parts) != 2:
                return False, None, "Malformed token structure."

            raw_b64, expected_sig = parts
            payload_bytes = base64.urlsafe_b64decode(raw_b64.encode("utf-8"))

            computed_sig = hmac.new(self.secret, payload_bytes, hashlib.sha256).hexdigest()
            if not hmac.compare_digest(computed_sig, expected_sig):
                return False, None, "Invalid cryptographic signature."

            data = json.loads(payload_bytes.decode("utf-8"))
            token_id = data.get("token_id")

            # Check single-use
            if token_id in self._consumed_tokens:
                return False, None, "Token has already been consumed (replay attempt)."

            # Check expiration
            expires_at = data.get("expires_at", 0.0)
            if time.time() > expires_at:
                return False, None, "Token has expired."

            # Mark as consumed
            self._consumed_tokens.add(token_id)

            payload = ApprovalTokenPayload(
                token_id=token_id,
                trace_id=data["trace_id"],
                action=RemediationAction(data["action"]),
                service_name=data["service_name"],
                namespace=data["namespace"],
                pod_name=data.get("pod_name"),
                reason=data["reason"],
                expires_at=expires_at,
            )
            return True, payload, "Success"
        except Exception as e:
            return False, None, f"Verification failed: {e}"


class InteractiveCardBuilder:
    """
    Constructs rich ChatOps action cards for Feishu and DingTalk.
    """

    @staticmethod
    def build_feishu_card(
        ctx: FailureContext,
        arbitration: ArbitrationResult,
        approval_token: str,
        verdict: str,
    ) -> Dict[str, Any]:
        """Builds a Feishu/Lark Interactive Message Card."""
        return {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": f"🚨 [AIOps-Sentry] P0 自愈待授权: {ctx.service_name}",
                },
                "template": "red",
            },
            "elements": [
                {
                    "tag": "div",
                    "fields": [
                        {
                            "is_short": True,
                            "text": {"tag": "lark_md", "content": f"**目标服务:**\n`{ctx.service_name}`"},
                        },
                        {
                            "is_short": True,
                            "text": {"tag": "lark_md", "content": f"**命名空间:**\n`{ctx.namespace}`"},
                        },
                        {
                            "is_short": True,
                            "text": {"tag": "lark_md", "content": f"**目标 Pod:**\n`{ctx.pod_name or 'N/A'}`"},
                        },
                        {
                            "is_short": True,
                            "text": {
                                "tag": "lark_md",
                                "content": f"**Jev 推荐动作:**\n`{arbitration.selected_action.value}` ({arbitration.confidence*100:.1f}%)",
                            },
                        },
                    ],
                },
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**触发规则:** {ctx.trigger_rule}\n**安全门禁判决:** {verdict}",
                    },
                },
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**错误堆栈快照:**\n```{ctx.error_logs[:300]}```",
                    },
                },
                {"tag": "hr"},
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "✅ 批准执行"},
                            "type": "danger",
                            "value": {
                                "action": "approve",
                                "approval_token": approval_token,
                            },
                        },
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "❌ 忽略告警"},
                            "type": "default",
                            "value": {
                                "action": "ignore",
                                "approval_token": approval_token,
                            },
                        },
                    ],
                },
            ],
        }

    @staticmethod
    def build_dingtalk_card(
        ctx: FailureContext,
        arbitration: ArbitrationResult,
        approval_token: str,
        callback_url: str = "http://aiops-sentry.monitoring:8080/v1/sentry/approval",
    ) -> Dict[str, Any]:
        """Builds a DingTalk ActionCard JSON."""
        markdown_text = (
            f"### 🚨 [AIOps-Sentry] 故障自愈二次授权\n\n"
            f"- **目标服务**: `{ctx.service_name}` ({ctx.namespace})\n"
            f"- **推荐动作**: `{arbitration.selected_action.value}` (置信度: {arbitration.confidence*100:.1f}%)\n"
            f"- **诊断理由**: {arbitration.reason}\n\n"
            f"> 堆栈: {ctx.error_logs[:200]}"
        )
        return {
            "msgtype": "actionCard",
            "actionCard": {
                "title": f"故障自愈授权: {ctx.service_name}",
                "text": markdown_text,
                "singleTitle": "前往控制台确认",
                "singleURL": f"{callback_url}?token={approval_token}",
            },
        }
