from multiprocessing import Process
from typing import Dict, Generator, List
import logging
import re
from time import sleep

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
)
from kubernetes.client.api.core_v1_api import CoreV1Api
from kubernetes.client.api_client import ApiClient
from kubernetes.client.configuration import Configuration
from kubernetes.client.exceptions import ApiException


logger = logging.getLogger("luigi-interface")

DEFAULT_POLL_INTERVAL = 5


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
    config.load_kube_config()
    return BatchV1Api()


def pod_spec_from_dict(
    name, spec_schema, restartPolicy="Never", labels={}
) -> V1PodTemplateSpec:
    """
    returns a pod template spec from a dictionary describing a pod
    """
    containers = []
    for container in spec_schema["containers"]:
        if "imagePullPolicy" in container:
            container["image_pull_policy"] = container.pop("imagePullPolicy")
            containers.append(V1Container(**container))
    pod_template = V1PodTemplateSpec(
        metadata=V1ObjectMeta(name=name, labels=labels),
        spec=V1PodSpec(restart_policy=restartPolicy, containers=containers),
    )
    return pod_template


def get_job_pods(job: V1Job) -> List[V1Pod]:
    """
    get the pods associated with a kubernetes Job
    """
    core_v1 = CoreV1Api()
    label_selector = "job-name=" + job.metadata.name
    return core_v1.list_namespaced_pod(
        job.metadata.namespace, label_selector=label_selector
    ).items


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


def get_pod_log_stream(pod: V1Pod) -> Generator[str, None, None]:
    """
    returns a stream object looping over the logs of a pod
    """
    core_v1 = corev1_client()
    watcher = watch.Watch()
    stream = watcher.stream(
        core_v1.read_namespaced_pod_log,
        name=pod.metadata.name,
        namespace=pod.metadata.namespace,
    )
    return stream


def is_pod_running(pod: V1PodSpec):
    client = corev1_client()
    r = client.read_namespaced_pod(
        name=pod.metadata.name, namespace=pod.metadata.namespace
    )
    pod_is_running = r.status.phase != "Pending"
    return pod_is_running


def corev1_client():
    config.load_kube_config()
    core_v1 = CoreV1Api()
    return core_v1


def print_pod_logs(job: V1Job, pod: V1PodSpec):
    """
    prints in realtime the logs of a running POD
    """
    logs_prefix = "JOB: " + job.metadata.name + " POD: " + pod.metadata.name
    while True:
        logger.info(logs_prefix + " ...waiting for POD to start")
        sleep(DEFAULT_POLL_INTERVAL)
        try:
            if is_pod_running(pod):
                logger.info(logs_prefix + " POD is running")
                break
        except ApiException as e:
            logger.warning("error while fetching pod logs :" + logs_prefix)
            logger.exception(e)
            raise e

    stream = get_pod_log_stream(pod)
    for i in stream:
        l = logs_prefix + ": " + i
        logger.info(l)


class BackgroundJobLogger:
    """
    Prints logs of pods associated to a job.
    Logs get printed in background processes.
    """

    def __init__(self, job: V1Job):
        self.job = job

    def _start_logging(self, job: V1Job):
        pods = get_job_pods(job)
        processes = [Process(target=print_pod_logs, args=(job, pod)) for pod in pods]
        for proc in processes:
            proc.start()
        return processes

    def __enter__(self):
        self.printing_procs = self._start_logging(self.job)

    def __exit__(self, exception_type, exception, traceback):
        if exception is None:
            for p in self.printing_procs:
                p.join()
                p.close()
        if exception is not None:
            for p in self.printing_procs:
                p.kill()
                p.close()


def is_pod_waiting_for_scale_up(condition: V1PodCondition) -> bool:
    """
    Returns true if a pod is waiting to get scheduled
    because cluster is scaling up.
    """
    if "Unschedulable" not in condition.reason or not condition.message:
        return False
    match = re.match(r"(\d)\/(\d) nodes are available.*", condition.message)
    if match:
        current_nodes = int(match.group(1))
        target_nodes = int(match.group(2))
        if current_nodes <= target_nodes:
            return True
        return False

    
def has_scaling_failed(condition: V1PodCondition) -> bool:
    if "Unschedulable" in condition.reason and \
       condition.message and "pod didn't trigger scale-up (it wouldn" in condition.message:
        return True
    return False


def has_job_started(job: V1Job) -> bool:
    """
    Checks if a job has started running.
    It checks the status of pods associated to a job.
    Throws exception if any pod in the job fails at startup
    Throws exception if any pod in the job fails to get scheduled
    """
    WAIT_FOR_JOB_CREATION_INTERVAL = 5
    pods = get_job_pods(job)
    if not pods:
        logger.debug(
            f"JOB: {job.metadata.name} -  No pods found for %s, waiting for cluster state to match the job definition"
        )
        logger.info("waiting for cluster to match job definition")
        sleep(WAIT_FOR_JOB_CREATION_INTERVAL)
        pods = get_job_pods(job)

    for pod in pods:
        logs_prefix = "JOB: " + job.metadata.name + " POD: " + pod.metadata.name
        if pod.status.container_statuses:
            for status in pod.status.container_statuses:
                logger.info(f"{logs_prefix} container status {status}")
                if status.state.terminated:
                    raise FailedJob(
                        job=job,
                        message=f"Job: {job.metadata.name} - Pod: {pod.name} container has a  weird status : {status}",
                    )
                if status.state.waiting:
                    if status.state.waiting.reason != "ContainerCreating":
                        raise FailedJob(
                            job=job,
                            message=f"Job: {job.metadata.name} - Pod: {pod.name} container has a  weird status : {status}",
                        )
        if pod.status.conditions:
            for cond in pod.status.conditions:
                logger.info(f"{logs_prefix} pod condition {cond}")
                if cond.reason == "ContainersNotReady":
                    return False
                if cond.reason == "Unschedulable":
                    if is_pod_waiting_for_scale_up(cond):
                        if has_scaling_failed(cond):
                            logger.info(f"{logs_prefix} Cluster could not scale up.")
                            raise FailedJob(
                                job=job,
                                message=f"Job: {job.metadata.name} - Pod: {pod.name} container has a  weird condition : {cond.reason}  {cond.message}",
                            )
                        logger.info(f"{logs_prefix} Waiting for cluster to Scale up..")
                        return False

    return True


def is_job_completed(k8s_client: ApiClient, job: V1Job):
    """
    returns true if a job is completed
    returns false if a job is still running
    raises an exception if job reaches a failed state
    """
    api_response = k8s_client.read_namespaced_job_status(
        name=job.metadata.name, namespace=job.metadata.namespace
    )
    has_succeded = api_response.status.succeeded is not None
    has_failed = api_response.status.failed is not None
    if has_succeded or has_failed:
        if has_succeded:
            logger.info("JOB: " + job.metadata.name + " has succeded")
            return True

        if has_failed:
            logger.warning("JOB: " + job.metadata.name + " has failed")
            raise FailedJob(
                job,
                job_status=api_response,
                message=f"Job {job.metadata.name} has failed",
            )
    return False


def run_and_track_job(k8s_client: ApiClient, job: V1Job) -> None:
    """
    Tracks the execution of a job by following its state changes.
    """
    logger.info(f"JOB: {job.metadata.name} submitting job")
    api_response = k8s_client.create_namespaced_job(
        body=job, namespace=job.metadata.namespace
    )
    logger.info(f"JOB: {job.metadata.name} submitted")
    logger.debug(f"API response job creation: {api_response}")
    job_completed = False
    with BackgroundJobLogger(job):
        while not has_job_started(job):
            sleep(DEFAULT_POLL_INTERVAL)
            logger.info("Waiting for Kubernetes job " + job.metadata.name + " to start")

        while not job_completed:
            job_completed = is_job_completed(k8s_client, job)
            sleep(DEFAULT_POLL_INTERVAL)


def clean_job_resources(k8s_client: ApiClient, job: V1Job) -> None:
    """
    delete kubernetes resources associated to a Job
    """
    logger.info(f"JOB: {job.metadata.name} - Cleaning Job's resources")
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
