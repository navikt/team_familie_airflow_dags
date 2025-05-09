import requests
from requests.auth import HTTPBasicAuth
from felles_metoder.felles_metoder import oracle_secrets

def get_latest_ghcr_tag(repo: str) -> str:
    
    secret = oracle_secrets() # legg til PAT (token) i gsm 
    token = secret['ghcr_token']

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
    

