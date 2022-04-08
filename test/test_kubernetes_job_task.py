import yaml


from mock import patch, MagicMock
import pytest

from kubeluigi import KubernetesJobTask

from kubernetes.client import BatchV1Api


class DummyTask(KubernetesJobTask):
    @property
    def name(self):
        return "my_dummy_task"

    def spec_schema(self):
        return {
            "containers": [
                {
                    "name": self.name,
                    "image": "my_container:my_tag",
                    "command": "dummy_cmd",
                }
            ],
        }

    @property
    def namespace(self):
        return "some_namespace"


def test_restart_policy_property():
    task = DummyTask()
    assert type(task.restart_policy) is str


def test_job_definition():
    """
    testing we generate a valid job definition
    """
    task = DummyTask()
    k8s_job_definition = task.build_job_definition()
    assert k8s_job_definition.api_version == "batch/v1"
    assert k8s_job_definition.kind == "Job"
    assert k8s_job_definition.spec.template.spec.containers[0].command == "dummy_cmd"


def test_job_definition_as_yaml():
    """
    testing that we can generate a compliant k8s yaml file
    equivalent to our task
    """
    task = DummyTask()
    yaml_task_definition = task.as_yaml()
    yaml_as_dict = yaml.safe_load(yaml_task_definition)
    assert yaml_as_dict["apiVersion"] == "batch/v1"
    assert yaml_as_dict["kind"] == "Job"
    assert (
        yaml_as_dict["spec"]["template"]["spec"]["containers"][0]["command"]
        == "dummy_cmd"
    )
    assert yaml_as_dict["spec"]["template"]["spec"]["volumes"] == []


@patch("kubeluigi.run_and_track_job")
@patch("kubeluigi.clean_job_resources")
@patch.object(KubernetesJobTask, "build_job_definition")
@patch.object(KubernetesJobTask, "_init_kubernetes")
def test_failing_task_clean_resources(
    mocked_init_kubernetes,
    mocked_build_job_definition,
    mocked_clean_job_resources,
    mocked_run_and_track_job,
):
    """
    testing k8s resources are cleaned when running job fails.
    """
    task = DummyTask()
    task._init_task_metadata()
    task.kubernetes_client = MagicMock()
    task._KubernetesJobTask__logger = MagicMock()

    class DummyException(Exception):
        pass

    e = DummyException()
    mocked_run_and_track_job.side_effect = e
    with pytest.raises(DummyException):
        task.run()

    mocked_build_job_definition.assert_called_once()
    mocked_clean_job_resources.assert_called_once()
    mocked_clean_job_resources.assert_called_once()


def test_name_not_implemented():
    task = KubernetesJobTask()

    with pytest.raises(NotImplementedError):
        task.name


def test_spec_schema_not_implemented():
    task = KubernetesJobTask()

    with pytest.raises(NotImplementedError):
        task.spec_schema()
