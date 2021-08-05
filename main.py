import luigi

from kubeluigi import KubernetesJobTask

class Task(KubernetesJobTask):
    """
    Runs a vanilla ubuntu docker container with minimal utilities:
       az cli, bash, wget, curl..
    Useful when you want to run a bunch of bash cmds in a container
    """
    container_name = luigi.Parameter("fandango.azurecr.io/opti_utils:")
    container_tag = luigi.Parameter("development")

    @property
    def limits(self):
        r = {"requests": {
                    "memory": "200M",
                    "cpu": "1m"
                }}
        return r

    @property
    def cmd(self):
        return "echo david"

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
                          "name": "CLOUD",
                          "value": "azure"
                         }
                    ]
                }
            ],
        }

    @property
    def name(self):
        return 'dummytask'

print("init kubernetes..")
t = Task()
t._init_kubernetes()
t.run()
