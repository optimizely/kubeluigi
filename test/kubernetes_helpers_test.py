from kubeluigi.k8s import (
    has_scaling_failed,
    pod_spec_from_dict,
    job_definition,
    print_pod_logs,
    is_pod_waiting_for_scale_up,
    has_job_started,
    FailedJob,
    run_and_track_job,
    BackgroundJobLogger,
)

from kubernetes.client import V1Pod, V1PodCondition

from mock import patch, MagicMock
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


def test_job_definition():
    labels = {"l1": "label1"}
    pod_spec = pod_spec_from_dict("pod_name", dummy_pod_spec, labels=labels)
    job = job_definition("my_job", "my_uiid", 100, pod_spec, labels, "my_namespace")
    expected_labels = {
        "l1": "label1",
        "spawned_by": "luigi",
        "luigi_task_id": "my_uiid",
    }
    assert job.api_version == "batch/v1"
    assert job.kind == "Job"
    assert job.metadata.namespace == "my_namespace"
    assert job.metadata.name == "my_job"
    assert job.metadata.labels == expected_labels


@patch("kubeluigi.k8s.is_pod_running")
@patch("kubeluigi.k8s.get_pod_log_stream")
def test_print_pod_logs_pod_has_started(
    mocked_get_pod_log_stream, mocked_is_pod_running
):
    # if pod has started we should get a stream of logs
    mocked_is_pod_running.return_value = True
    labels = {"l1": "label1"}
    pod = pod_spec_from_dict("name_of_pod", dummy_pod_spec, labels=labels)
    job = job_definition("my_job", "my_uiid", 100, pod, labels, "my_namespace")
    print_pod_logs(job, pod)
    mocked_is_pod_running.assert_called_once_with(pod)
    mocked_get_pod_log_stream.assert_called_once_with(pod)


@patch("kubeluigi.k8s.is_pod_running")
@patch("kubeluigi.k8s.get_pod_log_stream")
def test_print_pod_logs_pod_has_not_started(
    mocked_get_pod_log_stream, mocked_is_pod_running
):
    # pod has not started case
    mocked_is_pod_running.side_effect = [False, False, True]
    labels = {"l1": "label1"}
    pod = pod_spec_from_dict("name_of_pod", dummy_pod_spec, labels=labels)
    job = job_definition("my_job", "my_uiid", 100, pod, labels, "my_namespace")
    print_pod_logs(job, pod)
    assert mocked_is_pod_running.call_count == 3
    mocked_get_pod_log_stream.assert_called_once_with(pod)


@patch("kubeluigi.k8s.is_pod_running")
@patch("kubeluigi.k8s.get_pod_log_stream")
def test_print_pod_logs_pod_exception(mocked_get_pod_log_stream, mocked_is_pod_running):
    mocked_is_pod_running.side_effect = Exception("eror")
    labels = {"l1": "label1"}
    pod = pod_spec_from_dict("name_of_pod", dummy_pod_spec, labels=labels)
    job = job_definition("my_job", "my_uiid", 100, pod, labels, "my_namespace")
    with pytest.raises(Exception):
        print_pod_logs(job, pod)
    mocked_is_pod_running.assert_called_once_with(pod)
    mocked_get_pod_log_stream.assert_not_called()


def test_is_pod_waiting_for_scale_up():
    scaling_up_condition = V1PodCondition(
        message="0/3 nodes are available",
        reason="Unschedulable",
        status="something",
        type="something",
    )
    assert is_pod_waiting_for_scale_up(scaling_up_condition)

    non_scaling_condition = V1PodCondition(
        message="..", reason="..", status="something", type="something"
    )
    assert not is_pod_waiting_for_scale_up(non_scaling_condition)

    non_scaling_condition = V1PodCondition(
        message="Unschedulable", reason="..", status="something", type="something"
    )
    assert not is_pod_waiting_for_scale_up(non_scaling_condition)

    
def test_has_scaling_failed():
    condition = V1PodCondition(
        message="pod didn't trigger scale-up (it wouldn't fit if a new node is added)",
        reason="Unschedulable",
        status="something",
        type="something",
    )
    assert has_scaling_failed(condition)
    condition = V1PodCondition(
        message="0/3 nodes are available",
        reason="Unschedulable",
        status="something",
        type="something",
    )
    assert not has_scaling_failed(condition)
    


@patch("kubeluigi.k8s.get_job_pods")
def test_has_job_started_pod_is_failed(mocked_get_job_pods):
    # a pod associated  with the job is in a failed state
    failed_pod = MagicMock()
    failed_pod_status = MagicMock()
    failed_pod_status.status.state.terminated = True
    failed_pod.status.container_statuses = [failed_pod_status]
    mocked_get_job_pods.return_value = [failed_pod]
    labels = {"l1": "label1"}
    pod = pod_spec_from_dict("name_of_pod", dummy_pod_spec, labels=labels)
    job = job_definition("my_job", "my_uiid", 100, pod, labels, "my_namespace")
    with pytest.raises(FailedJob):
        has_job_started(job)


@patch("kubeluigi.k8s.get_job_pods")
def test_has_job_started_scaling_up(mocked_get_job_pods):
    # a pod associated  with the job is scaling up
    scalingpod = MagicMock()
    scaling_up_condition = MagicMock()
    scaling_up_condition.reason = "Unschedulable"
    scaling_up_condition.message = "0/3 nodes are available"
    scalingpod.status.conditions = [scaling_up_condition]
    mocked_get_job_pods.return_value = [scalingpod]
    labels = {"l1": "label1"}
    pod = pod_spec_from_dict("name_of_pod", dummy_pod_spec, labels=labels)
    job = job_definition("my_job", "my_uiid", 100, pod, labels, "my_namespace")
    assert not has_job_started(job)


@patch("kubeluigi.k8s.get_job_pods")
def test_pod_is_not_ready_yet(mocked_get_job_pods):
    not_ready_pod = MagicMock()
    scaling_up_condition = MagicMock()
    scaling_up_condition.reason = "ContainersNotReady"
    not_ready_pod.status.conditions = [scaling_up_condition]
    mocked_get_job_pods.return_value = [not_ready_pod]
    labels = {"l1": "label1"}
    pod = pod_spec_from_dict("name_of_pod", dummy_pod_spec, labels=labels)
    job = job_definition("my_job", "my_uiid", 100, pod, labels, "my_namespace")
    assert not has_job_started(job)


@patch("kubeluigi.k8s.get_job_pods")
def test_pods_are_in_start_state(mocked_get_job_pods):
    # pods are in a start state
    started_pod = MagicMock()
    started_pod.status.conditions = []
    mocked_get_job_pods.return_value = [started_pod]
    labels = {"l1": "label1"}
    pod = pod_spec_from_dict("name_of_pod", dummy_pod_spec, labels=labels)
    job = job_definition("my_job", "my_uiid", 100, pod, labels, "my_namespace")
    assert has_job_started(job)


@patch("kubeluigi.k8s.has_job_started")
@patch("kubeluigi.k8s.is_job_completed")
@patch("kubeluigi.k8s.DEFAULT_POLL_INTERVAL", 0)
def test_run_and_track_job(mocked_has_job_started, mocked_is_job_completed):
    client = MagicMock()
    mocked_has_job_started.side_effect = [False, False, True]
    mocked_is_job_completed.side_effect = [False, False, True]
    labels = {"l1": "label1"}
    pod = pod_spec_from_dict("name_of_pod", dummy_pod_spec, labels=labels)
    job = job_definition("my_job", "my_uiid", 100, pod, labels, "my_namespace")
    with patch.object(BackgroundJobLogger, "__enter__", return_value=None) as m1:
        with patch.object(BackgroundJobLogger, "__exit__", return_value=None) as m2:
            run_and_track_job(client, job)
    assert mocked_has_job_started.call_count == 3
    assert mocked_is_job_completed.call_count == 3


@patch("kubeluigi.k8s.has_job_started")
@patch("kubeluigi.k8s.is_job_completed")
@patch("kubeluigi.k8s.DEFAULT_POLL_INTERVAL", 0)
def test_run_and_track_job_failure(mocked_has_job_started, mocked_is_job_completed):
    client = MagicMock()
    mocked_has_job_started.side_effect = [False, False, True]
    labels = {"l1": "label1"}
    pod = pod_spec_from_dict("name_of_pod", dummy_pod_spec, labels=labels)
    job = job_definition("my_job", "my_uiid", 100, pod, labels, "my_namespace")
    mocked_is_job_completed.side_effect = [False, False, FailedJob(job, "something")]
    with patch.object(BackgroundJobLogger, "__enter__", return_value=None) as m1:
        with patch.object(BackgroundJobLogger, "__exit__", return_value=None) as m2:
            with pytest.raises(FailedJob):
                run_and_track_job(client, job)
