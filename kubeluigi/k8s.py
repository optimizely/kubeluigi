
from typing import Dict, Generator, List

import logging

from kubernetes import config, watch
from kubernetes.client import V1PodSpec, V1ObjectMeta, V1PodTemplateSpec, V1Container, V1JobSpec, V1Job, BatchV1Api, V1DeleteOptions, V1Pod
from kubernetes.client.api import core_v1_api
from kubernetes.client.api_client import ApiClient
from kubernetes.client.configuration import Configuration
from kubernetes.client.exceptions import ApiException

from multiprocessing import Pool

from time import sleep

logger = logging.getLogger('luigi-interface')


class FailedJob(Exception):
    def __init__(self, job, job_status, message):
        self.job = job
        self.job_status = job_status
        self.message = message
        super().__init__(self.message)



def kubernetes_client():
    """
    returns a kubernetes client
    """
    config.load_kube_config()
    return BatchV1Api()
    

def pod_spec_from_dict(name, spec_schema,
                       restartPolicy="never", labels={}) -> V1PodTemplateSpec:
    """
    returns a pod template spec from a dictionary describing a pod
    """
    containers = []
    for container in spec_schema['containers']:
        if 'imagePullPolicy' in container:
            container['image_pull_policy'] = container.pop('imagePullPolicy')
            containers.append(V1Container(**container))
    pod_template = V1PodTemplateSpec(
        metadata=V1ObjectMeta(name=name, labels=labels),
        spec=V1PodSpec(restart_policy="Never", containers=containers))
    return pod_template


def get_job_pods(job: V1Job) -> List[V1Pod]:
    """
    get the pods associated with a kubernetes Job
    """
    core_v1 = core_v1_api.CoreV1Api()
    label_selector = "job-name=" + job.metadata.name
    return core_v1.list_namespaced_pod(
                job.metadata.namespace, label_selector=label_selector
            ).items


def job_definition(job_name: str, job_uuid: str,
                   backoff_limit: int,
                   pod_template_spec: V1PodTemplateSpec,
                   labels: Dict[str, str], namespace: str) -> V1Job:
    """
    returns a job object describing a k8s job.
    """
    # Create the specification of deployment
    spec = V1JobSpec(
        template=pod_template_spec,
        backoff_limit=backoff_limit)
    
    # Instantiate the job object
    labels.update({'spawned_by': 'luigi', 'luigi_task_id': job_uuid})

    job = V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=V1ObjectMeta(name=job_name, labels=labels, namespace=namespace),
        spec=spec)

    return job


def get_pod_log_stream(pod: V1Pod):
    """
    returns a stream object looping over the logs of a pod
    """
    core_v1 = core_v1_api.CoreV1Api()
    watcher = watch.Watch()
    stream = watcher.stream(
        core_v1.read_namespaced_pod_log,
        name=pod.metadata.name,
        namespace=pod.metadata.namespace
        )
    return stream

def is_pod_unscheduble(pod_status):
    print(type(pod_status))
    return pod_status['conditions']['reason'] == 'Unschedulable'


def print_pod_logs(job: V1Job, pod: V1PodSpec):
    """
    prints in realtime the logs of a running POD
    """
    pod_is_running = False
    config.load_kube_config()
    core_v1 = core_v1_api.CoreV1Api()
    logs_prefix = "JOB: " + job.metadata.name +  " POD: " + pod.metadata.name
    while True:
        logger.info(logs_prefix + " ...waiting for POD to start")
        sleep(2)
        try:
            r = core_v1.read_namespaced_pod(name=pod.metadata.name,
                                                namespace=pod.metadata.namespace)
            pod_is_running = r.status.phase != 'Pending'
            print(r.status)
            if pod_is_running:
                logger.info(logs_prefix + " POD is in Phase: " + r.status.phase)
                break
            if is_pod_unscheduble(r.status):
                logger.warning(logs_prefix + "WARNING: POD is Unschedulable: " + r.status['conditions'['message']])
                return
        except ApiException as e:
            logger.warning("error while fetching pod logs :" + logs_prefix)
            logger.exception(e)
            
    stream = get_pod_log_stream(pod)
    for i in stream:
        l = logs_prefix + ": " + i
        logger.info(l)


def print_job_logs(job: V1Job):
    pods = get_job_pods(job)
    logger.debug("Pods associated to Job : {}".format(job.metadata.name))
    logger.debug(pods)
    with Pool(2) as p:
        p.starmap(print_pod_logs, [(job, pod)for pod in pods])

def check_pods_are_runnable():
    pass

def track_job(k8s_client: ApiClient, job: V1Job, real_time_logs=True) -> None:
    """
    Tracks the execution of a job by following its state changes.
    """
    job_completed = False
    if real_time_logs:
        print_job_logs(job)
    while not job_completed:
        api_response = k8s_client.read_namespaced_job_status(
            name=job.metadata.name,
            namespace=job.metadata.namespace)
        has_succeded = api_response.status.succeeded is not None
        has_failed = api_response.status.failed is not None
        if has_succeded or has_failed:
            job_completed = True
            if has_succeded:
                logger.info("JOB: " + job.metadata.name + " has succeded")

            if has_failed:
                logger.warning("JOB: " + job.metadata.name + " has failed")
                raise FailedJob(job, api_response, "Job {} has failed".format(job.metadata.name))
        sleep(1)

    
def run_and_track_job(k8s_client: ApiClient, job: V1Job) -> None:
    """
    Runs a Job and keeps track of its execution and logs
    """
    api_response = k8s_client.create_namespaced_job(body=job, namespace=job.metadata.namespace)
    logger.debug("API response job creation: {}".format(api_response))
    track_job(k8s_client, job)

    
def clean_job_resources(k8s_client: ApiClient, job: V1Job):
    """
    delete kubernetes resources associated to a Job
    """
    logger.info("Cleaning Job's resources: {}".format(job.metadata.name))
    api_response = k8s_client.delete_namespaced_job(
        name=job.metadata.name,
        namespace=job.metadata.namespace,
        body=V1DeleteOptions(
            propagation_policy='Background',
            grace_period_seconds=5))
    # fix this error handling
    if api_response.status != 'Success':
        logger.warning("Error while cleaning job: {} : {}".format(job.metadata.name,
                                                                  api_response))
        raise Exception("error cleaning job: {} : {}".format(job.metadata.name,
                                                             api_response))
    logger.info("Finished cleaning Job's resources: {}".format(job.metadata.name))

