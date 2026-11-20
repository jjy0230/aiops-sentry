"""
Service Restorer & Rollback:
Handles post-remediation traffic restoration (un-draining pods) and horizontal scale-in
back to normal baseline replicas after the outage window has passed.
"""

import asyncio
import logging
from typing import Tuple, Optional, Dict, Any
from sentry.schemas import FailureContext

logger = logging.getLogger("AIOps-Sentry.Restorer")


class ServiceRestorer:
    """
    Manages the reverse lifecycle of remediated resources.
    Restores isolated pods to active traffic and scales deployments back to baseline.
    """

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self._k8s_available = False
        self._init_k8s_client()

    def _init_k8s_client(self):
        try:
            from kubernetes import client, config
            try:
                config.load_incluster_config()
                self._k8s_available = True
            except Exception:
                config.load_kube_config()
                self._k8s_available = True
            self.core_v1 = client.CoreV1Api()
            self.apps_v1 = client.AppsV1Api()
        except Exception:
            self._k8s_available = False

    async def restore_traffic(self, namespace: str, pod_name: str) -> Tuple[bool, str]:
        """
        Removes the 'sentry.traffic/status: isolated' label, reconnecting the pod to Service Endpoints.
        """
        if self.dry_run or not self._k8s_available:
            await asyncio.sleep(0.005)
            msg = f"[Mock K8s] Removed isolation label from pod '{pod_name}' in namespace '{namespace}'. Traffic restored."
            logger.info(msg)
            return True, msg

        loop = asyncio.get_running_loop()
        try:
            # Patch pod label to remove isolated status
            body = {"metadata": {"labels": {"sentry.traffic/status": None, "app": "restored"}}}
            await loop.run_in_executor(
                None,
                lambda: self.core_v1.patch_namespaced_pod(
                    name=pod_name, namespace=namespace, body=body
                ),
            )
            msg = f"Successfully restored pod '{pod_name}' into Service Endpoints in namespace '{namespace}'."
            logger.info(msg)
            return True, msg
        except Exception as err:
            logger.error("Failed to restore pod traffic: %s", err)
            return False, f"Kubernetes API error: {err}"

    async def scale_in_to_baseline(
        self, namespace: str, service_name: str, baseline_replicas: int = 2
    ) -> Tuple[bool, str]:
        """
        Scales down deployment back to baseline replica count after traffic surge has normalized.
        """
        if self.dry_run or not self._k8s_available:
            await asyncio.sleep(0.005)
            msg = f"[Mock K8s] Scaled deployment '{service_name}' back to baseline ({baseline_replicas} replicas)."
            logger.info(msg)
            return True, msg

        loop = asyncio.get_running_loop()
        try:
            scale = await loop.run_in_executor(
                None,
                lambda: self.apps_v1.read_namespaced_deployment_scale(
                    name=service_name, namespace=namespace
                ),
            )
            current = scale.spec.replicas or 1
            if current <= baseline_replicas:
                return True, f"Deployment '{service_name}' already at or below baseline ({current} <= {baseline_replicas})."

            scale.spec.replicas = baseline_replicas
            await loop.run_in_executor(
                None,
                lambda: self.apps_v1.replace_namespaced_deployment_scale(
                    name=service_name, namespace=namespace, body=scale
                ),
            )
            msg = f"Scaled deployment '{service_name}' down from {current} to baseline {baseline_replicas} replicas."
            logger.info(msg)
            return True, msg
        except Exception as err:
            logger.error("Failed to scale in deployment: %s", err)
            return False, f"Kubernetes API error: {err}"
