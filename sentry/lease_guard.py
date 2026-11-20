"""
Distributed Lease Guard: Anti-flapping and cooldown coordination.
Supports Kubernetes-native coordination.k8s.io/v1 Lease resources for multi-replica HA,
with transparent, zero-dependency in-memory fallback for local development and CI.
"""

import time
import logging
from typing import Tuple, Dict, Optional
from sentry.config import settings

logger = logging.getLogger("AIOps-Sentry.LeaseGuard")


class DistributedLeaseGuard:
    """
    Coordinates remediation cooldowns across distributed Sentry replicas.
    Prevents cascading flapping and race conditions during alert storms.
    """

    def __init__(self, cooldown_seconds: int = settings.cooldown_seconds, namespace: str = "monitoring"):
        self.cooldown_seconds = cooldown_seconds
        self.namespace = namespace
        self._memory_leases: Dict[str, float] = {}
        self._k8s_coordination_client = None
        self._init_k8s_lease_client()

    def _init_k8s_lease_client(self):
        try:
            from kubernetes import client, config
            try:
                config.load_incluster_config()
                self._k8s_coordination_client = client.CoordinationV1Api()
                logger.info("Loaded in-cluster Kubernetes CoordinationV1Api for distributed leases.")
            except Exception:
                config.load_kube_config()
                self._k8s_coordination_client = client.CoordinationV1Api()
                logger.info("Loaded external kubeconfig for distributed leases.")
        except Exception:
            self._k8s_coordination_client = None

    def _sanitize_lease_name(self, target_key: str) -> str:
        # K8s resource names must be RFC 1123 compliant (lowercase, numbers, dashes)
        safe_name = target_key.replace(":", "-").replace("_", "-").replace("/", "-").lower()
        return f"sentry-cooldown-{safe_name}"[:63]

    def is_in_cooldown(self, target_key: str) -> Tuple[bool, float]:
        """
        Checks if the target is within its cooldown window.
        Returns (is_cooled_down, remaining_seconds).
        """
        if self._k8s_coordination_client is not None:
            try:
                lease_name = self._sanitize_lease_name(target_key)
                lease = self._k8s_coordination_client.read_namespaced_lease(
                    name=lease_name, namespace=self.namespace
                )
                if lease and lease.spec and lease.spec.renew_time:
                    # Parse timestamp
                    renew_ts = lease.spec.renew_time.timestamp()
                    duration = lease.spec.lease_duration_seconds or self.cooldown_seconds
                    now = time.time()
                    elapsed = now - renew_ts
                    if elapsed < duration:
                        return True, duration - elapsed
            except Exception as e:
                # If lease not found (404), target is not in cooldown
                pass

        # Fallback to local thread-safe in-memory lease tracking
        last_exec = self._memory_leases.get(target_key)
        if last_exec is not None:
            elapsed = time.time() - last_exec
            if elapsed < self.cooldown_seconds:
                return True, self.cooldown_seconds - elapsed

        return False, 0.0

    def record_execution(self, target_key: str) -> bool:
        """
        Records an execution, establishing a cooldown lease for this target.
        """
        now = time.time()
        self._memory_leases[target_key] = now

        if self._k8s_coordination_client is not None:
            try:
                from kubernetes import client
                from datetime import datetime, timezone

                lease_name = self._sanitize_lease_name(target_key)
                now_dt = datetime.now(timezone.utc)
                body = client.V1Lease(
                    metadata=client.V1ObjectMeta(
                        name=lease_name,
                        namespace=self.namespace,
                        labels={"app.kubernetes.io/managed-by": "aiops-sentry"},
                    ),
                    spec=client.V1LeaseSpec(
                        holder_identity="aiops-sentry-worker",
                        lease_duration_seconds=self.cooldown_seconds,
                        renew_time=now_dt,
                        acquire_time=now_dt,
                    ),
                )
                try:
                    self._k8s_coordination_client.create_namespaced_lease(
                        namespace=self.namespace, body=body
                    )
                except Exception:
                    self._k8s_coordination_client.replace_namespaced_lease(
                        name=lease_name, namespace=self.namespace, body=body
                    )
                return True
            except Exception as e:
                logger.warning("Failed to record K8s Lease (%s), memory lease active.", e)

        return True

    def reset(self):
        """Clears local memory leases (used in tests)."""
        self._memory_leases.clear()
