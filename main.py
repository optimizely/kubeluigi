from kubeluigi import KubernetesJobTask

import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class Task(KubernetesJobTask):
    """
    Runs a vanilla ubuntu docker container with minimal utilities:
       az cli, bash, wget, curl..
    Useful when you want to run a bunch of bash cmds in a container
    """
    container_name = "fandango.azurecr.io/opti_utils:"
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
        return "echo david555 && sleep 2 && echo after && sleep 5 && echo again"

    @property
    def namespace(self):
        return "moussaka"

    @property
    def labels(self):
        return {"aadpodidbinding": "fandango-pod-role"}

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

if __name__ == "__main__":
    print("init kubernetes..")
    t = Task()
    t._init_kubernetes()
    t.run()
