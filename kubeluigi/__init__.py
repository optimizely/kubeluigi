import logging
from typing import List
import yaml

from kubeluigi.k8s import (
    clean_job_resources,
    job_definition,
    pod_spec_from_dict,
    run_and_track_job,
    kubernetes_client,
    attach_volume_to_spec,
    FailedJob,
)

from kubeluigi.volumes import AttachableVolume

from kubernetes.client import ApiClient
from kubernetes.client.models.v1_job import V1Job
from kubernetes.client import V1Toleration
from kubernetes.client.models.v1_pod_spec import V1PodSpec

logger = logging.getLogger(__name__)


class KubernetesJobTask:

    volumes: List[AttachableVolume] = []
    tolerations: List[V1Toleration] = []

    def _init_task_metadata(self):
        self.uu_name = self.name

    def _init_kubernetes(self):
        self.kubernetes_client = kubernetes_client()

    @property
    def restart_policy(self):
        return "Never"

    @property
    def active_deadline_seconds(self):
        return None

    @property
    def backoff_limit(self):
        """
        Maximum number of retries before considering the job as failed.
        See: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/#pod-backoff-failure-policy
        """
        return 6

    @property
    def name(self):
        """
        A name for this job. This needs to be unique otherwise it will fail if another job
        with the same name is running.
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

    def build_job_definition(self) -> V1Job:
        self._init_task_metadata()
        schema = self.spec_schema()
        schema_with_volumes = self._attach_volumes_to_spec(schema)
        pod_template_spec = pod_spec_from_dict(
            self.uu_name, schema_with_volumes, self.labels, self.restart_policy, tolerations=self.tolerations
        )

        job = job_definition(
            job_name=self.uu_name,
            backoff_limit=self.backoff_limit,
            pod_template_spec=pod_template_spec,
            labels=self.labels,
            namespace=self.namespace,
            active_deadline_seconds=self.active_deadline_seconds
        )
        return job

    def onpodstarted(self, pods):
        for pod in pods:
            logger.info(
                f"Tail the Pod logs using: kubectl logs -f -n {pod.metadata.namespace} {pod.metadata.name}"
            )

    def as_yaml(self):
        job = self.build_job_definition()
        job_dict = ApiClient().sanitize_for_serialization(job)
        str_yaml = yaml.safe_dump(job_dict, default_flow_style=False, sort_keys=False)
        return str_yaml

    def run(self):
        self._init_kubernetes()
        job = self.build_job_definition()
        logger.debug("Submitting Kubernetes Job: " + self.uu_name)
        try:
            run_and_track_job(self.kubernetes_client, job, self.onpodstarted)
        except FailedJob as e:
            logger.exception(
                f"Luigi's job has failed running: {e.job.metadata.name}"
            )
            for pod in e.pods:
                logger.exception(
                    f"Luigi's job has failed running: {pod.status.message}"
                )
            raise
        except Exception:
            logger.exception(f"Luigi has failed to run: {job}, starting cleaning")
            raise
        else:
            clean_job_resources(self.kubernetes_client, job)
       
    def output(self):
        """
        An output target is necessary for checking job completion unless
        an alternative complete method is defined.
        Example::
            return luigi.LocalTarget(os.path.join('/tmp', 'example'))
        """
        pass

    def _attach_volumes_to_spec(self, spec_schema):
        """
        overrides the spec_schema of a task to attach a volume
        """
        if "volumes" not in spec_schema and hasattr(self, "volumes"):
            for volume in self.volumes:
                spec_schema = attach_volume_to_spec(spec_schema, volume)
        return spec_schema

    def add_volume(self, volume):
        """
        adds a volume to the task
        """
        return self.volumes.append(volume)

    def add_toleration(self, key, value, effect='NoSchedule', operator='Equal'):
        toleration = V1Toleration(key=key, value=value, effect=effect, operator=operator)

        return self.tolerations.append(toleration)

