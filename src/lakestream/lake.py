"""PyIceberg REST catalog, shared by every Python component that touches Iceberg."""

from pyiceberg.catalog import Catalog, load_catalog

from lakestream.settings import IcebergSettings, S3Settings


def load_lake_catalog(iceberg: IcebergSettings, s3: S3Settings) -> Catalog:
    """Connect to the REST catalog; data files go through PyIceberg's own S3 client."""
    return load_catalog(
        iceberg.catalog,
        type="rest",
        uri=iceberg.rest_uri,
        warehouse=iceberg.warehouse,
        **{
            "s3.endpoint": s3.endpoint,
            "s3.access-key-id": s3.access_key.get_secret_value(),
            "s3.secret-access-key": s3.secret_key.get_secret_value(),
            "s3.region": s3.region,
            "s3.path-style-access": "true",
        },
    )
