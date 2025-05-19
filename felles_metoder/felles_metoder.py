import datetime, os, json, re
from os import getenv
from google.cloud import secretmanager
from pathlib import Path

def get_periode():
    """
    henter periode for the tidligere måneden eksample--> i dag er 19.04.2022, metoden vil kalkulerer periode aarMaaned eks) '202203'
    :param periode:
    :return: periode
    """
    today = datetime.date.today() # dato for idag 2022-04-19
    first = today.replace(day=1) # dato for første dag i måneden 2022-04-01
    lastMonth = first - datetime.timedelta(days=1) # dato for siste dag i tidligere måneden

    return lastMonth.strftime("%Y%m") # henter bare aar og maaned


def set_secrets_as_envs():
    secrets = secretmanager.SecretManagerServiceClient()
    resource_name = f"{os.environ['KNADA_TEAM_SECRET']}/versions/latest"
    secret = secrets.access_secret_version(name=resource_name)
    secret_str = secret.payload.data.decode('UTF-8')
    secrets = json.loads(secret_str)
    os.environ.update(secrets)

  
def oracle_secrets():
    set_secrets_as_envs()
    return dict(
    user=getenv("DBT_ORCL_USER"),
    password=getenv("DBT_ORCL_PASS"),
    host = getenv("DBT_ORCL_HOST"),
    service = getenv("DBT_ORCL_SERVICE"),
    ghcr_token = getenv("ghcr_token"),
    encoding="UTF-8",
    nencoding="UTF-8"
    )

def get_siste_dag_i_forrige_maaned():
    today = datetime.datetime.now()
    first_day_of_this_month = today.replace(day=1)
    last_day_of_last_month = first_day_of_this_month - datetime.timedelta(days=1)
    last_day = last_day_of_last_month.day
    return last_day_of_last_month.replace(day=last_day, hour=0, minute=0, second=0).strftime('%Y%m%d')

def parse_task_image(name: str) -> str:
    """This function parses the Dockerfile of a task image and returns the image reference.

    It assumes that the Dockerfile is located in /dag/task_images/{name}/Dockerfile.

    The Dockerfile must contain a single FROM statement, and the image reference is returned.

    Arguments:
        name (str): The name of the directory in dag/task_images that contains the Dockerfile.
            May contain slashes to indicate subdirectories.
    """
    project_root = Path(__file__).resolve().parents[1]

    dockerfile = project_root / "dags" / "task_images" / name / "Dockerfile"
    if not dockerfile.exists():
        raise RuntimeError(f"Could not find Dockerfile {dockerfile}")

    with dockerfile.open("r", encoding="utf-8") as f:
        matches = re.findall(r"^FROM\s+(.*)", f.read(), flags=re.MULTILINE)
        if len(matches) == 1:
            return matches[0].strip()
        else:
            rel_path = dockerfile.relative_to(project_root)
            raise RuntimeError(f"Could not find a unique image reference in Dockerfile {rel_path}")