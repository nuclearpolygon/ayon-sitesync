from pydantic import Field
from ayon_server.settings import BaseSettingsModel, SettingsField

class S3RootsModel(BaseSettingsModel):
    _layout = "expanded"
    root_name: str = SettingsField("", title="Root Name")
    remote_path: str = SettingsField("", title="Remote Path")


class S3CredentialsModel(BaseSettingsModel):
    """AWS S3 credentials configuration."""
    _layout = "expanded"

    aws_access_key_id: str = Field(
        "",
        title="AWS Access Key ID",
        scope=["studio", "project", "site"],
        description="AWS Access Key ID for authentication"
    )

    aws_secret_access_key: str = Field(
        "",
        title="AWS Secret Access Key",
        scope=["studio", "project", "site"],
        description="AWS Secret Access Key for authentication"
    )

    aws_session_token: str = Field(
        "",
        title="AWS Session Token (optional)",
        scope=["studio", "project", "site"],
        description="AWS Session Token for temporary credentials"
    )


class S3Submodel(BaseSettingsModel):
    """Specific settings for S3 sites.

    credentials: AWS credentials for S3 access
    bucket: S3 bucket name
    roots: Root folders mapping on S3 bucket
    endpoint_url: Optional custom endpoint for S3-compatible services
    region_name: AWS region name
    """
    _layout = "expanded"

    enabled: bool = Field(
        True,
        title="Enabled",
        scope=["studio", "project", "site"]
    )

    credentials: S3CredentialsModel = Field(
        default_factory=S3CredentialsModel,
        title="AWS Credentials",
        scope=["studio", "project", "site"]
    )

    bucket: str = Field(
        "",
        title="S3 Bucket",
        scope=["studio", "project", "site"],
        description="S3 bucket name"
    )

    roots: list[S3RootsModel] = SettingsField(
        defalt_factory=list,
        title="S3 roots",
        description="Root folder path within the S3 bucket"
    )

    endpoint_url: str = Field(
        "",
        title="Endpoint URL (optional)",
        scope=["studio", "project", "site"],
        description="Custom endpoint URL for S3-compatible services (e.g., MinIO)"
    )

    region_name: str = Field(
        "us-east-1",
        title="AWS Region",
        scope=["studio", "project", "site"],
        description="AWS region name"
    )
