from kubernetes.client.models.v1_host_path_volume_source import V1HostPathVolumeSource
from kubernetes.client.models.v1_volume import V1Volume
from kubernetes.client.models.v1_volume_mount import V1VolumeMount
from kubeluigi.k8s import (
    kick_off_job,
    pod_spec_from_dict,
    job_definition,
    FailedJob,
    run_and_track_job,
    get_container_with_volume_mounts,
    attach_volume_to_spec,
    job_phase_stream,
    reduce_job_state
)

from kubernetes.client import V1Pod, V1PodCondition
from kubernetes.client.exceptions import ApiException

from mock import patch, MagicMock, PropertyMock
import pytest


dummy_pod_spec = {
    "containers": [
        {
            "name": "container_name",
            "image": "my_image",
            "args": ["my_arg"],
            "imagePullPolicy": "Always",
            "env": [{"name": "my_env", "value": "env"}],
        }
    ],
}


def test_pod_spec_from_dict():

    labels = {"l1": "label1"}
    pod_spec = pod_spec_from_dict("name_of_pod", dummy_pod_spec, labels=labels)

    assert pod_spec.metadata.name == "name_of_pod"
    assert pod_spec.metadata.labels == labels
    assert pod_spec.spec.restart_policy == "Never"
    container = pod_spec.spec.containers[0]
    assert container.name == dummy_pod_spec["containers"][0]["name"]
    assert container.image == dummy_pod_spec["containers"][0]["image"]
    assert container.env == dummy_pod_spec["containers"][0]["env"]


dummy_pod_spec_with_volume = {
    "containers": [
        {
            "name": "container_name",
            "image": "my_image",
            "args": ["my_arg"],
            "imagePullPolicy": "Always",
            "env": [{"name": "my_env", "value": "env"}],
            "volume_mounts": [
                {"name": "Vname", "mount_path": "VmountPath", "host_path": "VhostPath"}
            ],
        }
    ],
    "volumes": [
        {
            "name": "volname",
            "csi": {
                "driver": "blob.csi.azure.com",
                "volumeAttributes": {
                    "containerName": "volcontainername",
                    "secretName": "volsecret",
                    "mountOptions": "-o allow_other --file-cache-timeout-in-seconds=120",
                },
            },
        }
    ],
}


def test_pod_spec_with_volume_from_dict():

    labels = {"l1": "label1"}
    pod_spec = pod_spec_from_dict(
        "name_of_pod", dummy_pod_spec_with_volume, labels=labels
    )

    assert pod_spec.metadata.name == "name_of_pod"
    assert pod_spec.metadata.labels == labels
    assert pod_spec.spec.restart_policy == "Never"
    assert pod_spec.spec.volumes == [
        V1Volume(**dummy_pod_spec_with_volume["volumes"][0])
    ]
    container = pod_spec.spec.containers[0]
    assert container.name == dummy_pod_spec_with_volume["containers"][0]["name"]
    assert container.image == dummy_pod_spec_with_volume["containers"][0]["image"]
    assert container.env == dummy_pod_spec_with_volume["containers"][0]["env"]
    assert container.volume_mounts == [
        V1VolumeMount(mount_path="VmountPath", name="Vname")
    ]


dummy_container = {
    "name": "container_name",
    "image": "my_image",
    "args": ["my_arg"],
    "imagePullPolicy": "Always",
    "env": [{"name": "my_env", "value": "env"}],
    "volume_mounts": [
        {"name": "Vname", "mount_path": "VmountPath", "host_path": "VhostPath"}
    ],
}


def test_get_container_with_volume_mounts():
    container = get_container_with_volume_mounts(dummy_container)
    assert container["volume_mounts"] == [
        V1VolumeMount(mount_path="VmountPath", name="Vname")
    ]


def test_job_definition():
    labels = {"l1": "label1"}
    pod_spec = pod_spec_from_dict("pod_name", dummy_pod_spec, labels=labels)
    job = job_definition("my_job", 100, pod_spec, labels, "my_namespace")
    expected_labels = {
        "l1": "label1",
        "spawned_by": "luigi",
    }
    assert job.api_version == "batch/v1"
    assert job.kind == "Job"
    assert job.metadata.namespace == "my_namespace"
    assert job.metadata.name == "my_job"
    assert job.metadata.labels == expected_labels


@patch("kubeluigi.k8s.get_job_pods")
@patch("kubeluigi.k8s.DEFAULT_POLL_INTERVAL", 0)
def test_job_phase_stream(mocked_get_job_pods):
    """
    makes sure that job_phase_stream returns the phase of pods in a job
    """
    labels = {"l1": "label1"}
    pod = pod_spec_from_dict("name_of_pod", dummy_pod_spec, labels=labels)
    job = job_definition("my_job", 100, pod, labels, "my_namespace")
    pod.status = MagicMock()
    pod.status.phase = "Running"
    mocked_get_job_pods.return_value = [pod]
    phase, pods_with_changed_state, error_message = next(job_phase_stream(job))
    assert phase == "Running"
    assert pod.metadata.name == pods_with_changed_state[0].metadata.name

    pod.status.phase = "Succeeded"
    mocked_get_job_pods.return_value = [pod]
    phase, pods_with_changed_state, error_message = next(job_phase_stream(job))
    assert phase == "Succeeded"
    assert pod.metadata.name == pods_with_changed_state[0].metadata.name


@patch("kubeluigi.k8s.get_job_pods")
@patch("kubeluigi.k8s.DEFAULT_POLL_INTERVAL", 0)
def test_run_and_track_job_failure(mocked_get_job_pods):
    """
    Tests that a Failure phase in a pod triggers an exception in run_and_track_job
    """
    client = MagicMock()
    labels = {"l1": "label1"}
    pod = pod_spec_from_dict("name_of_pod", dummy_pod_spec, labels=labels)
    job = job_definition("my_job", 100, pod, labels, "my_namespace")
    pod.status = MagicMock()
    pod.status.phase = "Failed"
    mocked_get_job_pods.return_value = [pod]
    with pytest.raises(FailedJob) as e:
        run_and_track_job(client, job, lambda x: x)
        assert e.job == job
        assert e.pod.metadata.name == pod.metadata.name


@patch("kubeluigi.k8s.get_job_pods")
@patch("kubeluigi.k8s.DEFAULT_POLL_INTERVAL", 0)
def test_run_and_track_job_success(mocked_get_job_pods):
    """
    Tests that a Success phase in a pod exits
    """
    client = MagicMock()
    labels = {"l1": "label1"}
    pod = pod_spec_from_dict("name_of_pod", dummy_pod_spec, labels=labels)
    job = job_definition("my_job", 100, pod, labels, "my_namespace")
    pod.status = MagicMock()
    pod.status.phase = "Succeeded"
    mocked_get_job_pods.return_value = [pod]
    assert run_and_track_job(client, job, lambda x: x) is None


def test_kick_off_job():
    client = MagicMock()
    labels = {"l1": "label1"}
    pod = pod_spec_from_dict("name_of_pod", dummy_pod_spec, labels=labels)
    job = job_definition("my_job", 100, pod, labels, "my_namespace")
    kick_off_job(client, job)

    client.create_namespaced_job.assert_called_with(
        body=job, namespace=job.metadata.namespace
    )


@patch("kubeluigi.k8s.get_job_pods")
def test_reduce_job_state(mocked_get_job_pods):
    labels = {"l1": "label1"}

    pod1 = pod_spec_from_dict("name_of_pod", dummy_pod_spec, labels=labels)
    pod1.status = MagicMock()
    pod1.status.phase = "Running"

    pod2 = pod_spec_from_dict("name_of_pod", dummy_pod_spec, labels=labels)
    pod2.status = MagicMock()
    pod2.status.phase = "Failed"

    job_state, error_message = reduce_job_state([pod1, pod2])
    assert job_state == "Failed"

    pod2.status.phase = "Pending"
    job_state, error_message = reduce_job_state([pod1, pod2])
    assert job_state == "Mixed"

    pod2.status.phase = "Running"
    job_state, error_message = reduce_job_state([pod1, pod2])
    assert job_state == "Running"
