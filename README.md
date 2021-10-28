# Kubeluigi

Kubeluigi lets you run Luigi tasks as Pods in a Kubernetes Cluster.  Kubeluigi is an update on Luigi's default `KubernetesTask`.

Improvements over default luigi's contrib:

- Currently default K8s task on Luigi is outdated, it does not handle some edge cases but most importantly it is not a priority in Luigi, this makes it slow to get changes merged.
- Kubeluigi uses a non-deprecated kubernetes client library
- Kubeluigi handle edge error cases not handled by the stdlib KubernetesTask
- For most cases with only minor changes you should be able to unplug Luigi's default KubernetesJobTask and use Kubeluigi instead.
- Realtime logging from Kubernetes Tasks
- Logsfrom Kubernetes Tasks include a prefix which makes it easier to track the association of Job, Task, and Pod.


## Moving from Luigi's default contrib

Just replace your imports of `KubernetesJobTask`: 

```diff
- from luigi.contrib.kubernetes import KubernetesJobTask
+ from kubeluigi import KubernetesJobTask
```



## Full Kubeluigi Example

```python
from kubeluigi import KubernetesJobTask

class Task(KubernetesJobTask):

    container_name = "ubuntu:18.04"

    @property
    def limits(self):
        r = {"requests": {
                    "memory": "50M",
                    "cpu": "1m"
                }}
        return r

    @property
    def cmd(self):
        return "echo something && sleep 2 && echo after && sleep 5 && echo again"

    @property
    def namespace(self):
        return "moussaka"

    @property
    def labels(self):
        return {"my_label": "my_label_1"}

    @property
    def spec_schema(self):
        print(self.container_name)
        print(self.container_tag)
        return {
            "containers": [
                {
                    "name": self.name,
                    "image": self.container_name + self.container_tag,
                    "args": ["/bin/bash", "-c", self.cmd],
                    "imagePullPolicy": "Always",
                    "resources": self.limits,
                    "env": [
                        {
                          "name": "my_env",
                          "value": "env"
                         }
                    ]
                }
            ],
        }

    @property
    def name(self):
        return 'dummytask'

```

## Logs

Kubeluigi's task logs include Job, Task, and Pod identifiers: 

```
INFO:luigi-interface:JOB: dummytask-20211028031913-a5eb1d7e634b43c8 POD: dummytask-20211028031913-a5eb1d7e634b43c8-9cnmt: some echo message
```


## Development

- local setup: 

```bash
# install local package
pip install -e .

# testing
python setup.py test
```

