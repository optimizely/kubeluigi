from kubeluigi.volumes import BlobStorageVolume, EphemeralVolume
from test.test_kubernetes_job_task import DummyTask
from kubeluigi.k8s import attach_volume_to_spec


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


def test_ephemeral_volumes():
    v = EphemeralVolume(10)
    v2 = EphemeralVolume(10)
    assert v.volume_hash() == v2.volume_hash()

    vol_spec = v.pod_volume_spec()
    mnt_spec = v.pod_mount_spec()
    assert vol_spec["volumes"][0]["name"] == mnt_spec["volume_mounts"][0]["name"]
    assert mnt_spec["volume_mounts"][0]["mountPath"] == "/mnt/data"
    assert (
        vol_spec["volumes"][0]["ephemeral"]["volumeClaimTemplate"]["spec"]["resources"][
            "requests"
        ]["storage"]
        == "10Gi"
    )

    v3 = EphemeralVolume(10, "/mnt/other")
    assert v3.pod_mount_spec()["volume_mounts"][0]["mountPath"] == "/mnt/other"


def test_blob_storage_volumes():
    v = BlobStorageVolume("my_account", "my_container")
    assert v.secret_name() == "storage-sas-my_account"

    v2 = BlobStorageVolume("my_account", "my_container")
    assert v.volume_hash() == v2.volume_hash()

    vol_spec = v.pod_volume_spec()
    mnt_spec = v.pod_mount_spec()
    assert vol_spec["volumes"][0]["csi"]["driver"] == "blob.csi.azure.com"
    assert (
        vol_spec["volumes"][0]["csi"]["volumeAttributes"]["containerName"]
        == "my_container"
    )

    assert mnt_spec["volume_mounts"][0]["name"] == vol_spec["volumes"][0]["name"]
    assert mnt_spec["volume_mounts"][0]["mountPath"] == "/mnt/my_account/my_container"


def test_attach_volume_to_spec():
    dummy_spec = {"containers": [{"volume_mounts": []}], "volumes": []}
    v = BlobStorageVolume("my_account", "my_container")
    updated_spec = attach_volume_to_spec(dummy_spec, v)
    assert len(updated_spec["volumes"]) == 1
    assert len(updated_spec["containers"][0]["volume_mounts"]) == 1
