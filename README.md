# Kubeluigi

Kubeluigi is an update on Luigi's default Kubernetes Task.
- Currently default K8s task on Luigi is outdated, it does not handle some edge cases but most importantly it is not a priority in Luigi, this makes it slow to get changes merged.
- Kubeluigi uses a non-deprecated kubernetes client library
- Kubeluigi handle edge error cases not handled by the stdlib KubernetesTask
- For most cases with only minor changes you should be able to unplug Luigi's default KubernetesJobTask and use Kubeluigi instead.
- Please be aware that 


## Example

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

## Development

- local setup: 

```bash
# install local package
pip install -e .

# testing
python setup.py test
```
