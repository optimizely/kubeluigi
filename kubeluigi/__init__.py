from datetime import datetime
import logging
from time import sleep
import uuid

from kubernetes import config
from kubernetes.client import V1PodSpec, V1ObjectMeta, V1PodTemplateSpec, V1Container, V1JobSpec, V1Job, BatchV1Api


logger = logging.getLogger('luigi-interface')


def submit_job(task):
    pass


def pod_spec_from_dict(name, spec_schema,
                       restartPolicy="never", labels={}) -> V1PodTemplateSpec:
    """
    returns a pod template spec from a dictionary describing a pod
    """
    print("SPEC SCHEMA")
    print(spec_schema)
    containers = [V1Container(**container) for container in spec_schema['containers']]
    pod_template = V1PodTemplateSpec(
        metadata=V1ObjectMeta(name=name, labels=labels),
        spec=V1PodSpec(restart_policy="Never", containers=containers))
    return pod_template


def job_definition(job_name, job_uuid, backoff_limit, pod_template_spec: V1PodTemplateSpec, labels, namespace) -> V1Job:
    """
    returns a job object describing a k8s job
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


def track_job(k8s_client, job: V1Job):
    job_completed = False
    while not job_completed:
        print("runing job")
        api_response = k8s_client.read_namespaced_job_status(
            name=job.metadata.name,
            namespace=job.metadata.namespace)
        if api_response.status.succeeded is not None or \
                api_response.status.failed is not None:
            job_completed = True
        sleep(1)
        print("Job status='%s'" % str(api_response.status))
        

def run_and_track_job(k8s_client, job, namespace="default"):
    api_response = k8s_client.create_namespaced_job(body=job, namespace=namespace)
    # check api_response
    if api_response == "error":
        # fix this line
        raise Exception("some error")
    track_job(k8s_client, job)

    
class KubernetesJobTask:

    def _init_kubernetes(self):
        self.__logger = logger
        config.load_kube_config()
        self.kubernetes_client = BatchV1Api()
        self.job_uuid = str(uuid.uuid4().hex)
        now = datetime.utcnow()
        self.uu_name = "%s-%s-%s" % (self.name, now.strftime('%Y%m%d%H%M%S'), self.job_uuid[:16])

    @property
    def restart_policy(self):
        return "never"

    @property
    def backoff_limit(self):
        """
        Maximum number of retries before considering the job as failed.
        See: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/#pod-backoff-failure-policy
        """
        return 6

    @property
    def delete_on_success(self):
        """
        Delete the Kubernetes workload if the job has ended successfully.
        """
        return True

    @property
    def name(self):
        """
        A name for this job. This task will automatically append a UUID to the
        name before to submit to Kubernetes.
        """
        raise NotImplementedError("subclass must define name")

    @property
    def labels(self):
        """
        Return custom labels for kubernetes job.
        example::
        ``{"run_dt": datetime.date.today().strftime('%F')}``
        """
        return {}

    @property
    def spec_schema(self):
        """
        Kubernetes Job spec schema in JSON format, an example follows.
        .. code-block:: javascript
            {
                "containers": [{
                    "name": "pi",
                    "image": "perl",
                    "command": ["perl",  "-Mbignum=bpi", "-wle", "print bpi(2000)"]
                }]
            }
        """
        raise NotImplementedError("subclass must define spec_schema")


    def run(self):
        self._init_kubernetes()
        pod_template_spec = pod_spec_from_dict(self.uu_name,
                                               self.spec_schema,
                                               self.restart_policy,
                                               self.labels)
        
        job = job_definition(job_name=self.uu_name,
                             job_uuid=self.job_uuid,
                             backoff_limit=self.backoff_limit,
                             pod_template_spec=pod_template_spec)
        self.__logger.info("Submitting Kubernetes Job: " + self.uu_name)
        
        #run_and_track_job(self.kubernetes_client, job)

    def output(self):
        """
        An output target is necessary for checking job completion unless
        an alternative complete method is defined.
        Example::
            return luigi.LocalTarget(os.path.join('/tmp', 'example'))
        """
        pass
