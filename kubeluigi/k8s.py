from typing import Dict, List, Callable
import logging
from time import sleep
from base64 import b64encode
from urllib.parse import quote

from kubernetes import config
from kubernetes.client import (
    V1PodSpec,
    V1ObjectMeta,
    V1PodTemplateSpec,
    V1Container,
    V1JobSpec,
    V1Job,
    BatchV1Api,
    V1DeleteOptions,
    V1Pod,
    V1PodCondition,
    V1Volume,
    V1VolumeMount,
)
from kubernetes.client.api.core_v1_api import CoreV1Api
from kubernetes.client.api_client import ApiClient
from kubernetes.client.exceptions import ApiException

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 15


class FailedJob(Exception):
    def __init__(self, job, pod, message):
        self.job = job
        self.pod = pod
        self.message = message
        super().__init__(self.message)


def kubernetes_client() -> BatchV1Api:
    """
    returns a kubernetes client
    """
    config.load_config()
    return BatchV1Api()


def pod_spec_from_dict(
        name, spec_schema,
        labels={},
        restartPolicy="Never",
        **kwargs
) -> V1PodTemplateSpec:
    """
    returns a pod template spec from a dictionary describing a pod
    """
    containers = []
    volumes = []
    for container in spec_schema["containers"]:
        if "imagePullPolicy" in container:
            container["image_pull_policy"] = container.pop("imagePullPolicy")
        if "volume_mounts" in container and container["volume_mounts"]:
            container = get_container_with_volume_mounts(container)
        containers.append(V1Container(**container))
    if "volumes" in spec_schema:
        for volume in spec_schema["volumes"]:
            volumes.append(V1Volume(**volume))
    pod_template = V1PodTemplateSpec(
        metadata=V1ObjectMeta(name=name, labels=labels),
        spec=V1PodSpec(
            restart_policy=restartPolicy, containers=containers,
            volumes=volumes,
            **kwargs
        ),
    )
    return pod_template


def get_container_with_volume_mounts(container):
    """
    Returns a container with V1VolumeMount objects from the spec schema of a container
    and a list of V1volume objects
    """
    volumes_spec = container["volume_mounts"]
    mount_volumes = []
    for volume in volumes_spec:
        mount_path = volume["mountPath"]
        name = volume["name"]
        mount_volumes.append(V1VolumeMount(mount_path=mount_path, name=name))
    container["volume_mounts"] = mount_volumes
    return container


def job_definition(
    job_name: str,
    backoff_limit: int,
    pod_template_spec: V1PodTemplateSpec,
    labels: Dict[str, str],
    namespace: str,
) -> V1Job:
    """
    returns a job object describing a k8s job.
    """
    labels.update({"spawned_by": "luigi"})

    job = V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=V1ObjectMeta(name=job_name, labels=labels, namespace=namespace),
        spec=V1JobSpec(template=pod_template_spec, backoff_limit=backoff_limit),
    )

    return job


def kick_off_job(k8s_client: ApiClient, job: V1Job) -> V1Job:
    try:
        job = k8s_client.create_namespaced_job(
            body=job, namespace=job.metadata.namespace
        )
    except ApiException as e:
        if e.reason == "Conflict":
            logger.warning(
                "The job you tried to start is already running. We will try to track it."
            )
            job = k8s_client.read_namespaced_job(
                job.metadata.name, job.metadata.namespace
            )
        else:
            raise e

    return job


def has_scaling_failed(condition: V1PodCondition) -> bool:
    if (
        "Unschedulable" in condition.reason
        and condition.message
        and "pod didn't trigger scale-up (it wouldn" in condition.message
    ):
        return True
    return False


def get_job_pods(job) -> List[V1Pod]:
    """
    get the pods associated with a kubernetes Job
    """
    core_v1 = CoreV1Api()
    label_selector = "job-name=" + job.metadata.name
    return core_v1.list_namespaced_pod(
        job.metadata.namespace, label_selector=label_selector
    ).items


def job_phase_stream(job):
    previous_phase = {}
    while True:
        sleep(DEFAULT_POLL_INTERVAL)
        pods = get_job_pods(job)
        for pod in pods:
            if previous_phase.get(pod.metadata.name, None) != pod.status.phase:
                yield pod.status.phase, pod
            previous_phase[pod.metadata.name] = pod.status.phase


def are_all_pods_successful(job):
    pods = get_job_pods(job)
    return all([pod.status.phase == "Succeeded" for pod in pods])


def run_and_track_job(
    k8s_client: ApiClient, job: V1Job, onpodstarted: Callable = lambda x: None
) -> None:
    """
    Tracks the execution of a job by following its state changes.
    """
    logger.debug(f"Submitting job: {job.metadata.name}")
    job = kick_off_job(k8s_client, job)
    for phase, pod in job_phase_stream(job):
        logger.debug(f"Task {job.metadata.name} state is {phase}")

        # ToDo: Check if we handle : Scale up but not enough resources
        # Warning: Running onpodstarted is not guaranteed to execute all times..
        if phase == "Running":
            onpodstarted(pod)

        if phase == "Failed":
            raise FailedJob(job, pod, "Failed pod in Job")

        if phase == "Succeeded" and are_all_pods_successful(job):
            return


def clean_job_resources(k8s_client: ApiClient, job: V1Job) -> None:
    """
    delete kubernetes resources associated to a Job
    """
    logger.debug(f"JOB: {job.metadata.name} - Cleaning Job's resources")

    api_response = k8s_client.delete_namespaced_job(
        name=job.metadata.name,
        namespace=job.metadata.namespace,
        body=V1DeleteOptions(propagation_policy="Background", grace_period_seconds=5),
    )
    # fix this error handling
    if api_response.status != "Success":
        logger.warning(
            f"Error while cleaning job: {job.metadata.name} : {api_response}"
        )
        raise Exception(f"error cleaning job: {job.metadata.name} : {api_response}")
    logger.info(f"JOB: {job.metadata.name} -  Finished cleaning Job's resources")


def attach_volume_to_spec(pod_spec, volume):
    volume_spec = volume.pod_volume_spec()
    volume_mnt_spec = volume.pod_mount_spec()
    # updating volume_mounts
    for container in pod_spec["containers"]:
        mounted_volumes = container.get("volume_mounts", [])
        container["volume_mounts"] = (
            mounted_volumes + volume_mnt_spec["volume_mounts"]
        )

    # updating volumes
    current_volumes = pod_spec.get("volumes", [])
    pod_spec["volumes"] = current_volumes + volume_spec["volumes"]
    return pod_spec
