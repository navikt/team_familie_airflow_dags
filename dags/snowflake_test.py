import base64
import json
import urllib.request
from datetime import datetime

from airflow.decorators import dag, task

METADATA_IDENTITY_URL = (
"http://metadata.google.internal/computeMetadata/v1/instance/"
"service-accounts/default/identity"
"?audience=https://accounts.google.com&format=standard"
)

@dag(
  start_date=datetime(2026, 11, 24),
  schedule_interval=None,
  catchup=False,
  default_args={"retries": 0},
)
def test_wif_subject():
  @task
  def print_wif_subject():
    request = urllib.request.Request(
      METADATA_IDENTITY_URL, headers={"Metadata-Flavor": "Google"}
    )
    with urllib.request.urlopen(request, timeout=10) as response:
      token = response.read().decode()

      # Logger kun sub-claimet, aldri selve tokenet.
      payload = token.split(".")[1]
      claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
      print(f"WIF subject (sub): {claims['sub']}")

  print_wif_subject()

test_wif_subject()