from kubeluigi import KubernetesJobTask
from kubeluigi.k8s import pod_spec_from_dict, job_definition
from kubernetes.client import V1Pod

dummy_pod_spec = {
        "containers": [
            {
                "name": "container_name",
                "image": "my_image",
                "args": ["my_arg"],
                "imagePullPolicy": "Always",
                "env": [
                    {
                      "name": "my_env",
                      "value": "env"
                     }
                ]
            }
        ],
}

def test_pod_spec_from_dict():

    labels = {'l1': 'label1'}
    pod_spec = pod_spec_from_dict("name_of_pod", dummy_pod_spec, labels=labels)

    assert pod_spec.metadata.name == 'name_of_pod'
    assert pod_spec.metadata.labels == labels
    assert pod_spec.spec.restart_policy == 'Never'
    container = pod_spec.spec.containers[0]
    assert container.name == dummy_pod_spec['containers'][0]['name']
    assert container.image == dummy_pod_spec['containers'][0]['image']
    assert container.env == dummy_pod_spec['containers'][0]['env']

    
def test_job_definition():
    labels = {'l1': 'label1'}
    pod_spec = pod_spec_from_dict("pod_name", dummy_pod_spec, labels=labels)
    job = job_definition("my_job", "my_uiid", 100, pod_spec, labels, "my_namespace")
    expected_labels = {'l1': 'label1', "spawned_by": "luigi",
                       "luigi_task_id": 'my_uiid'}
    assert job.api_version == 'batch/v1'
    assert job.kind == 'Job'
    assert job.metadata.namespace == 'my_namespace'
    assert job.metadata.name == 'my_job'
    assert job.metadata.labels == expected_labels
 
def test_print_pod_logs():
    labels = {'l1': 'label1'}
    job = job_definition("my_job", "my_uiid", 100, pod_spec, labels, "my_namespace")
    pod = V1Pod()
    #print_pod_logs(job, pod)
    pass

def test_print_pod_logs_handle_error():
    pass
