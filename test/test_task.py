from typing import Any, Dict

import luigi

from kubeluigi import KubernetesJobTask


class DummyK8sTask(KubernetesJobTask, luigi.Task):
    @property
    def name(self) -> str:
        return "dummy-tsk"

    def spec_schema(self) -> Dict[str, Any]:
        return {}

    def run(self):
        pass


def test_task_can_be_scheduled_by_luigi():
    luigi.build([DummyK8sTask()], local_scheduler=True, log_level="WARNING")
