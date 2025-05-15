"""
Module: sentinel3_frp_loader

Encapsulates searching and reading Sentinel-3 SLSTR FRP products using STAC
and NetCDF.
"""

from typing import Any, Iterator

import pandas as pd
import pystac_client
import stackstac
import xarray as xr

from . import config  # type: ignore


class StacSearcher:
    """
    Class to perform STAC searches for Sentinel-3 SLSTR Level 2 FRP products.
    Automatically handles authentication with Copernicus Data Space.
    """

    def __init__(self, credentials: dict[str, str] | None = None):
        """
        Initialize the STAC searcher with Copernicus credentials.

        Parameters:
        - credentials: Optional dictionary with 'endpoint_url', 'access_key',
                        'secret_key'.
                        If None, credentials will be loaded automatically.
        """
        # Get credentials and set up environment
        self.creds = credentials or config.get_s3_credentials()
        config.setup_s3_environment(self.creds)

        # Initialize STAC client
        self.catalogue_url = "https://stac.dataspace.copernicus.eu/v1"
        self.client = pystac_client.Client.open(self.catalogue_url)
        self.client.add_conforms_to("ITEM_SEARCH")

    def get_collections(self) -> list[str]:
        """
        Retrieve available collections from the STAC catalogue.

        Returns:
        - List of collection names
        """
        collections = self.client.get_collections()
        return [collection.id for collection in collections]

    def search(
        self,
        datetime_range: str,
        geom: dict[str, Any],
        max_items: int = 100,
        collections: str = "sentinel-3-sl-2-frp-ntc",
        query: dict[str, dict] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Perform a STAC item search.

        Parameters:
        - datetime_range: ISO8601 date range (e.g., "2024-07-01/2024-08-01")
        - geom: GeoJSON geometry for spatial filter
        - max_items: Maximum number of items to retrieve
        - collections: Collection name (default: "sentinel-3-sl-2-frp-ntc")
        - query: Optional additional query parameters for example:
        `{"eo:cloud_cover": {"gte": 0, "lte": 20}}`
        Returns:
        - List of STAC item dictionaries
        """
        params = {
            "collections": collections,
            "datetime": datetime_range,
            "intersects": geom,
            "max_items": max_items,
            "fields": {"exclude": ["geometry"]},
        }
        items = list(self.client.search(**params).items_as_dicts())
        return items


class Sentinel3FRPLoader:
    """
    Class to convert Sentinel-3 SLSTR FRP NetCDF assets to a pandas DataFrame.
    Supports streaming and joining multiple assets.
    """

    def __init__(
        self,
        items: list[dict[str, Any]],
        credentials: dict[str, str] | None = None,
    ):
        """
        Initialize loader with STAC items and credentials.

        Parameters:
        - items: List of STAC item dictionaries
        - credentials: Optional dictionary with credentials. If None,
                        credentials will be loaded automatically.
        """
        self.items = items

        # Get credentials
        self.creds = credentials or config.get_s3_credentials()

        # Create filesystem
        self.fs = config.create_fsspec_filesystem(self.creds)

    def stream_asset(
        self, item: dict[str, Any], asset_key: str
    ) -> Iterator[dict[str, Any]]:
        """
        Lazily stream fire-level records from one NetCDF asset.

        Parameters:
        - item: STAC item dictionary
        - asset_key: Asset name (e.g., "FRP_an")

        Yields:
        - Fire-level record as dictionary
        """
        time = pd.to_datetime(item["properties"]["datetime"])
        item_id = item["id"]
        url = item["assets"][asset_key]["href"]

        with self.fs.open(url) as f:
            ds = xr.open_dataset(f)
            fire_vars = [v for v in ds.data_vars if "fires" in ds[v].dims]
            n_fires = ds.dims.get("fires", 0)

            for i in range(n_fires):
                record = {
                    str(var): ds[var].isel(fires=i).item()
                    for var in fire_vars
                }
                record["acquisition_time"] = time
                record["item_id"] = item_id
                yield record

    def load_asset(self, asset_key: str) -> pd.DataFrame:
        """
        Load a single asset across all items into a DataFrame.

        Parameters:
        - asset_key: Asset name to load (e.g., "FRP_an")

        Returns:
        - DataFrame of fire records
        """
        records = []
        for item in self.items:
            records.extend(self.stream_asset(item, asset_key))
        return pd.DataFrame(records)

    def load_all_assets(self, asset_keys: list[str]) -> pd.DataFrame:
        """
        Load and merge multiple assets on the "fires" dimension.
        Currently performs an outer join on index.

        Parameters:
        - asset_keys: List of asset keys to merge

        Returns:
        - Merged DataFrame with suffixes for overlapping column names
        """
        dfs = []
        for key in asset_keys:
            df = self.load_asset(key)
            dfs.append(df.add_suffix(f"_{key}"))

        # Join on index (row-wise merge)
        df_merged = pd.concat(dfs, axis=1)
        return df_merged


class Sentinel2Loader:
    def __init__(
        self,
        items: list[dict[str, Any]],
        epsg: int,
        bounds_latlon: tuple[float, float, float, float] | None = None,
        credentials: dict[str, str] | None = None,
        resolution: int = 10,
    ):
        """
        Initialize loader with STAC items and credentials.

        Parameters:
        - items: List of STAC item dictionaries
        - credentials: Optional dictionary with credentials. If None,
                        credentials will be loaded automatically.
        """
        self.items = items

        # Get credentials
        self.creds = credentials or config.get_s3_credentials()

        # Create filesystem
        self.fs = config.create_fsspec_filesystem(self.creds)

        return stackstac.stack(
            items=items,
            resolution=(resolution, resolution),
            bounds_latlon=bounds_latlon,
            chunksize=98304,
            epsg=epsg,
            gdal_env=stackstac.DEFAULT_GDAL_ENV.updated(
                {
                    "GDAL_NUM_THREADS": -1,
                    "GDAL_HTTP_UNSAFESSL": "YES",
                    "GDAL_HTTP_TCP_KEEPALIVE": "YES",
                    "AWS_VIRTUAL_HOSTING": "FALSE",
                    "AWS_HTTPS": "YES",
                }
            ),
        )
