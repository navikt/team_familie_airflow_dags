import os

from datetime import timedelta
from kubernetes import client

from airflow import DAG
from airflow.models.variable import Variable
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

import kubernetes.client as k8s
from dataverk_airflow.notifications import create_email_notification, create_slack_notification
from operators.vault import vault_volume, vault_volume_mount

def kafka_consumer_kubernetes_pod_operator(
    task_id: str,
    config: str,
    dag: DAG = None,
    application_name: str = "dvh-airflow-kafka-consumer",
    data_interval_start_timestamp_milli: str = "{{ data_interval_start.int_timestamp * 1000 }}",
    data_interval_end_timestamp_milli: str = "{{ data_interval_end.int_timestamp * 1000 }}",
    kafka_consumer_image: str = "ghcr.io/navikt/dvh-kafka-airflow-consumer:0.4.5",
    namespace: str = os.getenv('NAMESPACE'),
    email: str = None,
    slack_channel: str = None,
    retries: int = 3,
    extra_envs: dict = None,
    delete_on_finish: bool = True,
    startup_timeout_seconds: int = 360,
    retry_delay: timedelta = timedelta(seconds=120),
    nls_lang: str = "NORWEGIAN_NORWAY.AL32UTF8",
    depends_on_past: bool = True,
    wait_for_downstream: bool = True,
    do_xcom_push=True,
    *args,
    **kwargs
):
    """ Factory function for creating KubernetesPodOperator for executing kafka konsumer images
    :param dag: DAG: owner DAG
    :param task_id: str: Task ID
    :param namespace: str: K8S namespace for pod. Defaults to getting the namespace from the airflow variable "NAMESPACE"
    :param kafka_consumer_image: str: The kafka consumer kubernetes/docker image.
    :param email: str: Email of owner
    :param slack_channel: Name of slack channel, default None (no slack notification)
    :param retries: int: Number of retries for task before DAG fails, default 3
    :param extra_envs: dict: dict with environment variables example: {"key": "value", "key2": "value2"}
    :param delete_on_finish: bool: Whether to delete pod on completion
    :param startup_timeout_seconds: int: pod startup timeout
    :param retry_delay: timedelta: Time inbetween retries, default 120 seconds
    :param nls_lang: str: Configure locale and character sets with NLS_LANG environment variable in k8s pod, defaults to Norwegian
    :return: KubernetesPodOperator
    """

    env_vars = {
        "TZ": os.environ["TZ"],
        "NLS_LANG": nls_lang,
        #"VKS_VAULT_ADDR": os.environ["VKS_VAULT_ADDR"],
        #"VKS_AUTH_PATH": os.environ["VKS_AUTH_PATH"],
        #"VKS_KV_PATH": os.environ["VKS_KV_PATH"],
        #"K8S_SERVICEACCOUNT_PATH": os.environ["K8S_SERVICEACCOUNT_PATH"],
        "CONSUMER_CONFIG": config,
        "KNADA_TEAM_SECRET": os.environ["KNADA_TEAM_SECRET"],
        "KAFKA_TIMESTAMP_START": data_interval_start_timestamp_milli,
        "KAFKA_TIMESTAMP_STOP": data_interval_end_timestamp_milli
    }

    if extra_envs:
        env_vars = dict(env_vars, **extra_envs)

    def on_failure(context):
        if slack_channel:
            slack_notification = create_slack_notification(slack_channel, task_id, namespace)
            slack_notification.execute(context)

    return KubernetesPodOperator(
        dag=dag,
        on_failure_callback=on_failure,
        startup_timeout_seconds=startup_timeout_seconds,
        name=task_id,
        namespace=namespace,
        task_id=task_id,
        is_delete_operator_pod=delete_on_finish,
        image=kafka_consumer_image,
        image_pull_secrets=[k8s.V1LocalObjectReference('ghcr-credentials')],
        env_vars=env_vars,
        volumes=[vault_volume()],
        volume_mounts=[vault_volume_mount()],
        service_account_name=os.getenv('TEAM'),
        annotations={"sidecar.istio.io/inject": "false"},
        resources=client.V1ResourceRequirements(
            requests={"memory": "4G"},
            limits={"memory": "4G"}
        ),
        retries=retries,
        retry_delay=retry_delay,
        do_xcom_push=do_xcom_push,
        *args,
        **kwargs
    )