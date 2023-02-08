import os
from typing import Final

DEPLOY_PHASE: Final[str] = os.environ.get("DEPLOY_PHASE", "dev")

S3_BUCKET_NAME: Final[str] = (
    "yewon-prod"
    if os.environ.get("DEPLOY_PHASE") == "prod"
    else "yewon-dev"
)

AWS_S3_CONN_ID: Final[str] = "S3_default"
