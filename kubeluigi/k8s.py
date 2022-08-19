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
    def __init__(self, job, pods, message):
        self.job = job
        self.pods = pods
        self.message = message
        super().__init__(self.message)


def kubernetes_client() -> BatchV1Api:
    """
    returns a kubernetes client
    """
    config.load_config()
    return BatchV1Api()


def pod_spec_from_dict(name, spec_schema, labels={}, restart_policy="Never", **kwargs) -> V1PodTemplateSpec:
    """
    returns a pod template spec from a dictionary describing a pod
    """

    containers = []
    volumes = []

    spec = dict(spec_schema)

    for container in spec_schema["containers"]:

        if "imagePullPolicy" in container:
            container["image_pull_policy"] = container.pop("imagePullPolicy")

        if "volume_mounts" in container and container["volume_mounts"]:
            container = get_container_with_volume_mounts(container)

        containers.append(V1Container(**container))

    if "volumes" in spec_schema:
        for volume in spec_schema["volumes"]:
            volumes.append(V1Volume(**volume))

    spec["containers"] = containers
    spec["volumes"] = volumes

    if "restart_policy" not in spec_schema:
        spec["restart_policy"] = restart_policy

    pod_template = V1PodTemplateSpec(metadata=V1ObjectMeta(name=name, labels=labels), spec=V1PodSpec(**spec))

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
    job_name: str, backoff_limit: int, pod_template_spec: V1PodTemplateSpec, labels: Dict[str, str], namespace: str,
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
        job = k8s_client.create_namespaced_job(body=job, namespace=job.metadata.namespace)
    except ApiException as e:
        if e.reason == "Conflict":
            logger.warning("The job you tried to start is already running. We will try to track it.")
            job = k8s_client.read_namespaced_job(job.metadata.name, job.metadata.namespace)
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
    return core_v1.list_namespaced_pod(job.metadata.namespace, label_selector=label_selector).items


def reduce_job_state(pods: List[V1Pod]):
    pod_states = []
    error_message = "Unknown Reason of Failure"
    job_state = "Mixed"

    for pod in pods:

        pod_state = pod.status.phase

        try:
            # Boil down all container states into one pod state.
            for status in pod.status.container_statuses:
                if status.state.waiting and status.state.waiting.reason == "InvalidImageName":
                    pod_state = "Failed"
                    error_message = "Invalid Image"

                if status.state.terminated and status.state.terminated.reason == "Error":
                    pod_state = "Failed"
        except TypeError:
            pod_state = pod.status.phase

        pod_states.append(pod_state)

    # Boil down all pod states into one job state.

    # If all states are the same, set that as the job state
    if len(set(pod_states)) == 1:
        job_state = pod_states[0]

    if len(set(pod_states)) > 1:
        job_state = ",".join(pod_states)

    if len(pod_states) == 0:
        job_state = "Failed"

    print(pod_states)

    # If one is Failed, then the job is Failed
    if "Failed" in pod_states:
        job_state = "Failed"

    return job_state, error_message


def job_phase_stream(job):
    previous_job_state = None
    job_state = None

    while True:

        sleep(DEFAULT_POLL_INTERVAL)
        pods = get_job_pods(job)
        # print(pods)
        job_state, error_message = reduce_job_state(pods)

        # Only yield job state changes
        if job_state != previous_job_state:
            yield job_state, pods, error_message

        # Update state tracker
        previous_job_state = job_state


def are_all_pods_successful(job):
    pods = get_job_pods(job)
    return all([pod.status.phase == "Succeeded" for pod in pods])


def run_and_track_job(k8s_client: ApiClient, job: V1Job, onpodstarted: Callable = lambda x: None) -> None:
    """
    Tracks the execution of a job by following its state changes.
    """
    logger.debug(f"Submitting job: {job.metadata.name}")
    job = kick_off_job(k8s_client, job)
    for state, pods, error_message in job_phase_stream(job):
        logger.debug(f"Task {job.metadata.name} state is {state}")

        # ToDo: Check if we handle : Scale up but not enough resources
        # Warning: Running onpodstarted is not guaranteed to execute all times..
        if state == "Running":
            onpodstarted(pods)

        if state == "Failed":
            raise FailedJob(job, pods, error_message)

        if state == "Succeeded" and are_all_pods_successful(job):
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
        logger.warning(f"Error while cleaning job: {job.metadata.name} : {api_response}")
        raise Exception(f"error cleaning job: {job.metadata.name} : {api_response}")
    logger.info(f"JOB: {job.metadata.name} -  Finished cleaning Job's resources")


def attach_volume_to_spec(pod_spec, volume):
    volume_spec = volume.pod_volume_spec()
    volume_mnt_spec = volume.pod_mount_spec()
    # updating volume_mounts
    for container in pod_spec["containers"]:
        mounted_volumes = container.get("volume_mounts", [])
        container["volume_mounts"] = mounted_volumes + volume_mnt_spec["volume_mounts"]

    # updating volumes
    current_volumes = pod_spec.get("volumes", [])
    pod_spec["volumes"] = current_volumes + volume_spec["volumes"]
    return pod_spec
