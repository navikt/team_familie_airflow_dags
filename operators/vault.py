import kubernetes.client as k8s
import os

VOLUME_NAME = "vault-secrets"
MOUNT_PATH = "/var/run/secrets/nais.io/vault"


def vault_volume():
    return k8s.V1Volume(
        name=VOLUME_NAME,
        empty_dir=k8s.V1EmptyDirVolumeSource(
            medium="Memory"
        )
    )

def vault_volume_mount():
    return k8s.V1VolumeMount(
        name=VOLUME_NAME, mount_path=MOUNT_PATH,
        sub_path="vault/var/run/secrets/nais.io/vault",
        read_only=False
    )