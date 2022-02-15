from datetime import datetime
import logging
import uuid
import yaml

from kubeluigi.k8s import (
    clean_job_resources,
    job_definition,
    pod_spec_from_dict,
    run_and_track_job,
    kubernetes_client,
    attach_volume_to_spec,
)

from kubernetes.client import ApiClient

logger = logging.getLogger("kubeluigi")


class KubernetesJobTask:
    def _init_task_metadata(self):
        self.job_uuid = str(uuid.uuid4().hex)
        now = datetime.utcnow()
        self.uu_name = "%s-%s-%s" % (
            self.name,
            now.strftime("%Y%m%d%H%M%S"),
            self.job_uuid[:16],
        )

    def _init_kubernetes(self):
        self.__logger = logger
        self.kubernetes_client = kubernetes_client()

    @property
    def restart_policy(self):
        return "Never"

    @property
    def delete_on_success(self):
        """
        Delete the Kubernetes workload if the job has ended successfully.
        """
        return True

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

    def build_job_definition(self):
        self._init_task_metadata()
        schema = self.spec_schema()
        schema_with_volumes = self._attach_volumes_to_spec(schema)
        pod_template_spec = pod_spec_from_dict(
            self.uu_name, schema_with_volumes, self.restart_policy, self.labels
        )

        job = job_definition(
            job_name=self.uu_name,
            job_uuid=self.job_uuid,
            backoff_limit=self.backoff_limit,
            pod_template_spec=pod_template_spec,
            labels=self.labels,
            namespace=self.namespace,
        )
        return job

    def as_yaml(self):
        job = self.build_job_definition()
        job_dict = ApiClient().sanitize_for_serialization(job)
        str_yaml = yaml.safe_dump(job_dict, default_flow_style=False, sort_keys=False)
        return str_yaml

    def run(self):
        self._init_kubernetes()
        job = self.build_job_definition()
        self.__logger.debug("Submitting Kubernetes Job: " + self.uu_name)
        try:
            run_and_track_job(self.kubernetes_client, job)
        except e:
            logger.exception("Luigi has failed to submit the job, starting cleaning")
            raise e
        finally:
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
