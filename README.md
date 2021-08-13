# Kubeluigi

Kubeluigi is an update on Luigi's default Kubernetes Task.
- Kubeluigi uses a non-deprecated kubernetes client library
- Kubeluigi handle edge error cases not handled by the stdlib Kubernetes Task
- For most cases with only minor changes you should be able to unplug Luigi's default KubernetesJobTask and use Kubeluigi instead.


## Example

```python
from kubeluigi import KubernetesJobTask

class Task(KubernetesJobTask):
    """
    Runs a vanilla ubuntu docker container with minimal utilities:
       az cli, bash, wget, curl..
    Useful when you want to run a bunch of bash cmds in a container
    """
    container_name = "ubuntu:18.04"
    container_tag = "development"

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

