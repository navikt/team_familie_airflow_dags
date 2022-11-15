import kubernetes.client as k8s
import os

VOLUME_NAME = "vault-secrets"
MOUNT_PATH = "/var/run/secrets/nais.io/vault"

def vault_init_container(
    namespace: str,
    application_name: str = ""
):
    envs = [
        {"name": "VAULT_AUTH_METHOD", "value": "kubernetes"},
        {"name": "VAULT_SIDEKICK_ROLE", "value": namespace},
        {"name": "VAULT_K8S_LOGIN_PATH", "value": "auth/kubernetes/prod/kubeflow/login"},
    ]

    args = [
        "-v=10",
        "-logtostderr",
        "-vault=https://vault.adeo.no",
        "-one-shot",
        "-save-token=/var/run/secrets/nais.io/vault/vault_token",
        "-cn=secret:/kv/prod/kubeflow/" + namespace + ":dir=" + MOUNT_PATH + ",fmt=flatten,retries=1"
    ]
    return k8s.V1Container(
        name="vks-init",
        image="navikt/vault-sidekick:v0.3.10-26ad67d",
        volume_mounts=[vault_volume_mount()],
        env=envs,
        args=args,
    )

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