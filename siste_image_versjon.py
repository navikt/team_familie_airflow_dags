import requests
from requests.auth import HTTPBasicAuth
from felles_metoder.felles_metoder import oracle_secrets
import re
from pathlib import Path

def get_latest_ghcr_tag(repo: str) -> str:
    
    secret = oracle_secrets() # legg til PAT (token) i gsm 
    token = secret['ghcr_token']

    if "ghp" in token:
        print("token is found!")

    url = f"https://ghcr.io/v2/navikt/{repo}/tags/list" 
    auth = HTTPBasicAuth(username='AArafaR', password=token)

    response = requests.get(url, auth=auth)
    response.raise_for_status()

    tags = response.json().get("tags", [])
    if not tags:
        raise Exception("Finner ingen tags.")

    # Sort tag navn og hent den nyeste tag
    latest_tag = sorted(tags)[-1]

    print(f"Siste tag for ghcr.io/navikt/{repo}:{latest_tag}")
    return latest_tag


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
    

