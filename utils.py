import boto3
import botocore
import awswrangler as wr

from awswrangler._utils import ensure_session, _get_endpoint_url, default_botocore_config, apply_configs
from typing import Optional

@apply_configs
def wr_client(
    service_name: str, session: Optional[boto3.Session] = None, botocore_config: Optional[botocore.config.Config] = None
) -> boto3.client:
    """Create a valid boto3.client."""
    endpoint_url: Optional[str] = _get_endpoint_url(service_name=service_name)
    return ensure_session(session=session).client(
        service_name=service_name,
        endpoint_url=endpoint_url,
        use_ssl=True,  # Disable SSL
        verify=False,
        config=default_botocore_config() if botocore_config is None else botocore_config,
    )