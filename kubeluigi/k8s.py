from multiprocessing import Process
from typing import Dict, Generator, List, Callable
import logging
import re
from time import sleep
import gzip
import zlib
from base64 import b64encode
from urllib.parse import quote
import urllib3
import sys

from kubernetes import config, watch
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
    V1HostPathVolumeSource,
)
from kubernetes.client.api.core_v1_api import CoreV1Api
from kubernetes.client.api_client import ApiClient
from kubernetes.client.configuration import Configuration
from kubernetes.client.exceptions import ApiException

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 30


class FailedJob(Exception):
    def __init__(self, job, message, job_status=None):
        self.job = job
        self.job_status = job_status
        self.message = message
        super().__init__(self.message)


def kubernetes_client() -> BatchV1Api:
    """
    returns a kubernetes client
    """
    config.load_config()
    return BatchV1Api()


def pod_spec_from_dict(
    name, spec_schema, restartPolicy="Never", labels={}
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
            restart_policy=restartPolicy, containers=containers, volumes=volumes
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
    job_uuid: str,
    backoff_limit: int,
    pod_template_spec: V1PodTemplateSpec,
    labels: Dict[str, str],
    namespace: str,
) -> V1Job:
    """
    returns a job object describing a k8s job.
    """
    # Create the specification of deployment
    spec = V1JobSpec(template=pod_template_spec, backoff_limit=backoff_limit)

    # Instantiate the job object
    labels.update({"spawned_by": "luigi", "luigi_task_id": job_uuid})

    job = V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=V1ObjectMeta(name=job_name, labels=labels, namespace=namespace),
        spec=spec,
    )

    return job


def run_and_track_job(
    k8s_client: ApiClient, job: V1Job, onpodstarted: Callable = lambda x: None
) -> None:
    """
    Tracks the execution of a job by following its state changes.
    """
    logger.debug(f"Submitting job: {job.metadata.name}")
    api_response = k8s_client.create_namespaced_job(
        body=job, namespace=job.metadata.namespace
    )

    config.load_config()
    core_api = CoreV1Api()
    watcher = watch.Watch()
    stream = watcher.stream(
        core_api.list_namespaced_event,
        namespace=job.metadata.namespace,
        timeout_seconds=0,
    )

    for raw_event in stream:
        rel = raw_event["object"].related
        name = raw_event["object"].metadata.name
        reason = raw_event["object"].reason
        message = raw_event["object"].message

        # print(reason, message, name)
        # logger.info(f"{reason}, {message}, {name}")

        # Unfortunately I have not found a way to do this filtering with the kubernetes client so we have to do it manually
        if name.startswith(job.metadata.name):

            reason = raw_event["object"].reason
            message = raw_event["object"].message
            involved_object = raw_event["object"].involved_object

            logger.info(f"{reason}, {message}")

            if reason == "FailedScheduling" and any(
                [
                    case in message
                    for case in ["Insufficient cpu", "Insufficient memory"]
                ]
            ):
                logger.error(f"Pod scheduling failed due to lack of resources.")
                return

            if reason == "Started":
                pod_name = involved_object.name
                onpodstarted(pod_name)

            if reason == "Completed":
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
    logger.debug(f"JOB: {job.metadata.name} -  Finished cleaning Job's resources")


def attach_volume_to_spec(pod_spec, volume):
    volume_spec = volume.pod_volume_spec()
    volume_mnt_spec = volume.pod_mount_spec()
    # updating volume_mounts
    mounted_volumes = pod_spec["containers"][0].get("volume_mounts", [])
    pod_spec["containers"][0]["volume_mounts"] = (
        mounted_volumes + volume_mnt_spec["volume_mounts"]
    )

    # updating volumes
    current_volumes = pod_spec.get("volumes", [])
    pod_spec["volumes"] = current_volumes + volume_spec["volumes"]
    return pod_spec
