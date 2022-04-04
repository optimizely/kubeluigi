class AttachableVolume:
    """
    Base Mixin for requesting extra disk resources to k8s.
    Sub classes should implement volume_spec specifiying a kubernetes PVC.
    - size can be set by overriding volume_size_in_gb
    - mount point can be set by overriding mount_location
    """

    @property
    def pvc_name(self):
        """
        The pvc name which can be used by other resources
        """
        return f"{self.volume_hash()}-moussaka-disk".lower()

    @property
    def short_pvc_name(self):
        """
        a shorter version of the pvc name used
        """
        return self.pvc_name[:60].replace("-", "")

    def pod_volume_spec(self):
        """
        specifies an item in the "Volumes" section of a pod definition
        """
        pass

    def pod_mount_spec(self):
        """
        specifies volume_mounts block in a pod definition
        """
        pass

    def volume_hash(self):
        """
        unique hash identifiying a volume within a pod
        """
        pass


class EphemeralVolume(AttachableVolume):
    """
    Mixin for requesting extra disk resources to k8s.
    A default ephemeral disk is provided.
    size can be set by overriding volume_size_in_gb
    mount point can be set by overriding mount_location

    Please be aware:
     data in these volumes will be deleted once your task has ran
    """

    def __init__(self, size_in_gb, mount_location="/mnt/data"):
        super().__init__()
        self.mount_location = mount_location
        self.size_in_gb = size_in_gb

    def volume_hash(self):
        return hash(self.mount_location)

    def pod_volume_spec(self):
        return {
            "volumes": [
                {
                    "name": self.short_pvc_name,
                    "ephemeral": {
                        "volumeClaimTemplate": {
                            "spec": {
                                "accessModes": ["ReadWriteOnce"],
                                "storageClassName": "default",
                                "resources": {
                                    "requests": {"storage": f"{self.size_in_gb}Gi"}
                                },
                            }
                        }
                    },
                }
            ]
        }

    def pod_mount_spec(self):
        return {
            "volume_mounts": [
                {"name": self.short_pvc_name, "mountPath": self.mount_location}
            ]
        }


class BlobStorageVolume(AttachableVolume):
    """
    Returns a volume which mounts an azure storage container.
    it assumes that the needed secret to access the underlying
    azure storage account is available on k8s.
    """

    def __init__(self, storage_account, storage_container):
        super().__init__()
        self.storage_account = storage_account
        self.storage_container = storage_container
        self.mount_location = f"/mnt/{storage_account}/{self.storage_container}"

    def secret_name(self):
        """
        returns the name of the k8s secret providing access to the blob storage
        """
        return f"storage-sas-{self.storage_account}"

    def volume_hash(self):
        return hash(self.storage_account + self.storage_container + self.mount_location)

    def pod_volume_spec(self):
        return {
            "volumes": [
                {
                    "name": self.short_pvc_name,
                    "csi": {
                        "driver": "blob.csi.azure.com",
                        "volumeAttributes": {
                            "containerName": self.storage_container,
                            "secretName": self.secret_name(),
                            "mountOptions": "-o allow_other --file-cache-timeout-in-seconds=120",
                        },
                    },
                }
            ]
        }

    def pod_mount_spec(self):
        return {
            "volume_mounts": [
                {"name": self.short_pvc_name, "mountPath": self.mount_location}
            ]
        }
