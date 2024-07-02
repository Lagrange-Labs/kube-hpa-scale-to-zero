import argparse
import json
import logging
import os
import threading
from dataclasses import dataclass
from time import sleep

import kubernetes
from kubernetes import watch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOGGER = logging.getLogger(__name__)


def load_kubernetes_config() -> None:
    try:
        kubernetes.config.load_incluster_config()
    except kubernetes.config.ConfigException:
        kubernetes.config.load_kube_config()


load_kubernetes_config()
AUTOSCALING_V1 = kubernetes.client.AutoscalingV1Api()
APP_V1 = kubernetes.client.AppsV1Api()
DYNAMIC = kubernetes.dynamic.DynamicClient(kubernetes.client.api_client.ApiClient())


@dataclass(slots=True, kw_only=True)
class HPA:
    name: str
    namespace: str
    metric_name: str
    target_kind: str
    target_name: str
    target_type: str
    target_value: int


SYNC_INTERVAL = 30
HPAs: dict[str, HPA] = {}


def watch_metrics() -> None:
    """
    periodically watches metrics of HPA and scale the targets accordingly if needed.
    """

    def _watch():
        try:
            while True:
                for hpa in list(HPAs.values()):
                    update_target(hpa)
                sleep(SYNC_INTERVAL)
        except Exception as exc:
            LOGGER.exception(f"Exiting because of: {exc}")
            os._exit(1)

    threading.Thread(target=_watch, daemon=True).start()


def watch_hpa(args) -> None:
    LOGGER.info(f"Will watch HPA with {args.hpa_label_selector=} in {args.hpa_namespace=}.")
    while True:
        try:
            w = watch.Watch()
            for event in w.stream(
                AUTOSCALING_V1.list_namespaced_horizontal_pod_autoscaler,
                args.hpa_namespace,
                label_selector=args.hpa_label_selector,
            ):
                update_hpa(event["object"].metadata)
        except kubernetes.client.exceptions.ApiException as exc:
            if exc.status != 410:
                raise exc


def update_hpa(metadata) -> None:
    """
    inserts/updates/deletes the HPA to/in/from HPAs.
    """
    hpa_namespace, hpa_name = metadata.namespace, metadata.name
    namespaced_name = f"{hpa_namespace}/{hpa_name}"
    try:
        hpa = AUTOSCALING_V1.read_namespaced_horizontal_pod_autoscaler(namespace=hpa_namespace, name=hpa_name)
        metric_name, target_value = build_metric_value_path(hpa)
        HPAs[namespaced_name] = HPA(
            name=hpa_name,
            namespace=hpa_namespace,
            metric_name=metric_name,
            target_kind=hpa.spec.scale_target_ref.kind,
            target_name=hpa.spec.scale_target_ref.name,
            target_type=hpa.spec.metrics[0].external.target.type,
            target_value=target_value,
        )
    except kubernetes.client.exceptions.ApiException as exc:
        if exc.status != 404:
            raise exc
        LOGGER.info(f"HPA {hpa_namespace}/{hpa_name} was not found, will forget about it.")
        HPAs.pop(namespaced_name, None)


def build_metric_value_path(hpa) -> str:
    """
    returns the metric name and target value for the external metric.
    """
    metric = hpa.spec.metrics[0].external
    metric_name = metric.metric.name
    target_value = int(metric.target.average_value)
    
    return metric_name, target_value


def get_external_metric_value(metric_name: str, namespace: str) -> int | None:
    """
    retrieves the value of the external metric.
    """
    try:
        metric_data = DYNAMIC.request("GET", f"apis/external.metrics.k8s.io/v1beta1/namespaces/{namespace}/{metric_name}")
        return int(metric_data.items[0].value)
    except kubernetes.client.exceptions.ApiException as exc:
        if exc.status in {404, 503, 403}:
            LOGGER.exception(f"Could not get external metric {metric_name} in namespace {namespace}: {exc}")
            return None
        else:
            raise exc


def get_needed_replicas(hpa: HPA) -> int | None:
    """
    returns the number of replicas needed based on the external metric value.
    """
    metric_value = get_external_metric_value(hpa.metric_name, hpa.namespace)
    if metric_value is None:
        return None
    
    if hpa.target_type == "AverageValue":
        return max(1, (metric_value // hpa.target_value)) if metric_value > 0 else 0
    else:
        raise ValueError(f"Unsupported target type: {hpa.target_type}")


def update_target(hpa: HPA) -> None:
    needed_replicas = get_needed_replicas(hpa)
    if needed_replicas is None:
        LOGGER.error(f"Will not update {hpa.target_kind} {hpa.namespace}/{hpa.target_name}.")
        return
    # Maybe, be more precise (using target_api_version e.g.?)
    match hpa.target_kind:
        case "Deployment":
            scale_deployment(
                namespace=hpa.namespace,
                name=hpa.target_name,
                needed_replicas=needed_replicas,
            )
        case "StatefulSet":
            scale_statefulset(
                namespace=hpa.namespace,
                name=hpa.target_name,
                needed_replicas=needed_replicas,
            )
        case _:
            raise ValueError(f"Target kind {hpa.target_kind} not supported.")


def scaling_is_needed(*, current_replicas, needed_replicas) -> bool:
    """
    checks if the scale up/down is relevant.
    """
    # Maybe do not scale down if the HPA is unable to retrieve metrics? leave the current only pod do some work
    return bool(current_replicas) != bool(needed_replicas)


def scale_deployment(*, namespace, name, needed_replicas) -> None:
    try:
        scale = APP_V1.read_namespaced_deployment_scale(namespace=namespace, name=name)
        current_replicas = scale.status.replicas
        if not scaling_is_needed(current_replicas=current_replicas, needed_replicas=needed_replicas):
            LOGGER.info(f"No need to scale Deployment {namespace}/{name} {current_replicas=} {needed_replicas=}.")
            return

        scale.spec.replicas = needed_replicas
        # Maybe do not scale immediately? but don't want to reimplement an HPA.
        APP_V1.patch_namespaced_deployment_scale(namespace=namespace, name=name, body=scale)
        LOGGER.info(f"Deployment {namespace}/{name} was scaled {current_replicas=}->{needed_replicas=}.")
    except kubernetes.client.exceptions.ApiException as exc:
        if exc.status != 404:
            raise exc
        LOGGER.warning(f"Deployment {namespace}/{name} was not found.")


def scale_statefulset(*, namespace, name, needed_replicas) -> None:
    try:
        scale = APP_V1.read_namespaced_stateful_set_scale(namespace=namespace, name=name)
        current_replicas = scale.status.replicas
        if not scaling_is_needed(current_replicas=current_replicas, needed_replicas=needed_replicas):
            LOGGER.info(f"No need to scale StatefulSet {namespace}/{name} {current_replicas=} {needed_replicas=}.")
            return

        scale.spec.replicas = needed_replicas
        # Maybe do not scale immediately? but don't want to reimplement an HPA.
        APP_V1.patch_namespaced_stateful_set_scale(namespace=namespace, name=name, body=scale)
        LOGGER.info(f"StatefulSet {namespace}/{name} was scaled {current_replicas=}->{needed_replicas=}.")
    except kubernetes.client.exceptions.ApiException as exc:
        if exc.status != 404:
            raise exc
        LOGGER.warning(f"StatefulSet {namespace}/{name} was not found.")


def parse_cli_args():
    parser = argparse.ArgumentParser(
        description="kube-hpa-scale-to-zero. Check https://github.com/machine424/kube-hpa-scale-to-zero"
    )
    parser.add_argument(
        "--hpa-namespace",
        dest="hpa_namespace",
        default="default",
        help="namespace where the HPA live. (default: 'default' namespace)",
    )
    parser.add_argument(
        "--hpa-label-selector",
        dest="hpa_label_selector",
        default="",
        help="label_selector to get HPA to watch, 'foo=bar,bar=foo' e.g. (default: empty string to select all)",
    )

    return parser.parse_args()


if __name__ == "__main__":
    cli_args = parse_cli_args()
    watch_metrics()
    watch_hpa(cli_args)