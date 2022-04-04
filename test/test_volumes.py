from kubeluigi.volumes import BlobStorageVolume
from test.test_kubernetes_job_task import DummyTask


def test_volumes():
    # testing default volumes
    t = DummyTask()

    # testing attaching volumes
    v = BlobStorageVolume("my_account", "my_storage")
    t.add_volume(v)
    assert t.volumes == [v]
    # augmenting pod spec

    spec = t.spec_schema()
    updated_spec = t._attach_volumes_to_spec(spec)
    # it should have two volumes:
    # (i) default cache volume  and the v
    assert len(updated_spec["volumes"]) == 1
    assert len(updated_spec["containers"][0]["volume_mounts"]) == 1
