from __future__ import annotations
# Import libraries
import io
# from io import StringIO, BytesIO
from datetime import datetime as dt
import tempfile
import zipfile
import os
import urllib.parse
import pandas as pd
import numpy as np
import pendulum
# import time
# from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv
from sklearn.neighbors import BallTree
import json
import re
import unicodedata
import polars as pl
from uuid import uuid4
pl.enable_string_cache()
from typing import Dict, List, Optional, Tuple, Union, Iterable
import sqlalchemy as sa
from sqlalchemy import text as T
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine


import pyarrow as pa
import pyarrow.dataset as pads
import pyarrow.fs as pafs
import pyarrow.csv as pacsv


## Airflow
from airflow.sdk import dag, task, get_current_context, Variable, Connection
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowNotFoundException
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from botocore.config import Config as BotoConfig
from sqlalchemy.engine import URL as _SA_URL

# Load environment variables from .env file
load_dotenv()

LOCAL_TZ = pendulum.timezone("America/Mexico_City")

# Utilities (to be defined into another module)

## Constants
EARTH_RADIUS_KM = 6371.0

## Lists
homologated_mapping = {
    # Casas
    "Casa": "Casa",
    "Casa en condominio": "Casa en Condominio",
    "Casa en Fraccionamiento": "Casa en Condominio",
    "Condominio Horizontal": "Casa en Condominio",
    "Desarrollo-Casa": "Casa",
    "Desarrollo-Casa en condominio": "Casa en Condominio",
    "Casa - Comercial": "Casa Comercial",

    # Departamentos
    "Departamento": "Departamento",
    "Penthouse": "Departamento",
    "Loft": "Departamento",
    "Estudio": "Departamento",
    "Dúplex": "Dúplex",
    "Desarrollo-Duplex": "Dúplex",
    "Desarrollo-Apartamento": "Departamento",

    # Terrenos
    "Lote / Terreno": "Terreno",
    "Terreno": "Terreno",
    "Desarrollo-Terreno": "Terreno",
    "Terreno habitacional": "Terreno",
    "Terreno - Comercial": "Terreno Comercial",
    "Terreno comercial": "Terreno Comercial",
    "Terreno - Industrial": "Terreno Industrial",
    "Terreno industrial": "Terreno Industrial",

    # Comerciales y Oficinas
    "Tienda / Local Comercial": "Local Comercial",
    "Local - Comercial": "Local Comercial",
    "Local": "Local Comercial",
    "Lote Comercial": "Terreno Comercial",
    "Local en Centro Comercial": "Centro Comercial",
    "Desarrollo-Local Comercial": "Local Comercial",
    "Desarrollo-Local en centro comercial": "Centro Comercial",
    "Plaza - Comercial": "Centro Comercial",
    "Centro - Comercial": "Centro Comercial",
    "Oficina": "Oficina",
    "Oficina - Comercial": "Oficina",
    "Desarrollo-Oficina comercial": "Oficina",
    "Consultorio Médico": "Consultorio",
    "Consultorio - Comercial": "Consultorio",
    "Hospital - Comercial": "Hospital",

    # Bodegas y Naves Industriales
    "Nave Industrial / Bodega": "Bodega",
    "Bodega - Comercial": "Bodega",
    "Bodega comercial": "Bodega",
    "Bodega - Industrial": "Bodega Industrial",
    "Nave - Industrial": "Bodega Industrial",
    "Parque - Industrial": "Parque Industrial",
    "Fábrica - Industrial": "Fábrica Industrial",

    # Edificios
    "Edificio": "Edificio",
    "Edificio - Comercial": "Edificio Comercial",
    "Desarrollo-Edificio": "Edificio",

    # Hoteles y Entretenimiento
    "Entretenimiento / Hotel": "Hotel",
    "Hotel - Comercial": "Hotel",

    # Villas y Quintas
    "Quinta": "Quinta",
    "Rancho": "Rancho",
    "Quinta / Hacienda": "Quinta",
    "Rancho / Huerta": "Rancho",
    "Finca/Rancho": "Rancho",
    "Finca/Rancho - Comercial": "Rancho Comercial",
    "Desarrollo-Villa": "Villa",
    "Villa": "Villa",

    # Misceláneos
    "Desarrollo": "Desarrollo",
    "Cuarto": "Cuarto",
}

ordered_columns = [
    "id",
    "id_ext",
    "id_dev",
    "title",
    "description",
    "approximate_address",
    "street_name",
    "ext_number",
    "int_number",
    "neighborhood",
    "del_mun",
    "city",
    "city_area",
    "region",
    "state",
    "country",
    "zip_code",
    "lat",
    "lon",
    "property_type",
    "property_type_raw",
    "operation_type",
    "no_bedrooms",
    "no_full_bathrooms",
    "no_half_bathrooms",
    "no_parking_spaces",
    "year_built",
    "asking_price",
    "currency",
    "maintenance_fee",  # "asking_price_pesos",
    "m2_built",
    "m2_terrain",
    "m2_balconies",
    "m2_terraces",
    "m2_rooftop",
    "amenidades",
    "jacuzzi",
    "alberca",
    "gimnasio",
    "salonusosmultiples",
    "elevador",
    "asador",
    "publisher_name",
    "publisher_type",
    "source_type",  # "source",
    "source_listing_url",
    "main_development_url",
    "publication_date",
    "date_scraped",
    # "potential_duplicates", "num_pot_duplicates",
    "is_development",
    "is_remate",
]

common_columns = [
    "id",
    "title",
    "amenidades",
    "approximate_address",
    "apt_floor",
    "asking_price",
    "city",
    "city_area",
    "content",
    "country",
    "currency",
    "date_scraped",
    "del_mun",
    "ext_number",
    "int_number",
    "lat",
    "lon",
    "m2_balconies",
    "m2_built",
    "m2_rooftop",
    "m2_terraces",
    "m2_terrain",
    "main_development_url",
    "maintenance_fee",
    "neighborhood",
    "no_bedrooms",
    "no_full_bathrooms",
    "no_half_bathrooms",
    "no_parking_spaces",
    "operation_type",
    "phone_number",
    "property_type",
    "publication_date",
    "publisher_name",
    "publisher_type",
    "region",
    "source_listing_url",
    "source_type",
    "state",
    "street_name",
    "year_built",
    "zip_code",
]

tracked_fields = [
    'asking_price', 'currency', 'm2_built', 'm2_terrain',
    'lat', 'lon', 'no_bedrooms', 'no_full_bathrooms',
    'no_half_bathrooms', 'no_parking_spaces', 'year_built'
]

## Paths?

## Regex

## Functions
def _required_variable(key: str) -> str:
    """Fetch a required Airflow Variable and ensure it is not empty."""
    try:
        value = Variable.get(key)
    except KeyError as exc:
        raise ValueError(f"Airflow Variable '{key}' is not set.") from exc
    value_str = str(value).strip()
    if not value_str:
        raise ValueError(f"Airflow Variable '{key}' is empty.")
    return value_str

def _infer_source_and_operation(zip_basename: str) -> tuple[str, Optional[str]]:
    """
    Each csv file is a different source and have different types of operations.
    This function generates a mapping of sourcers and operations.
    """
    file_name = zip_basename.lower()
    if "i24" in file_name or "inmuebles24" in file_name:
        return "i24", "SALE"
    if "lamudi" in file_name:
        return "lamudi", "VENTA"
    if "remax" in file_name:
        return "remax", "VENTA"
    if "propiedades" in file_name or "propiedades.com" in file_name:
        return "propiedades", "VENTA"
    return "unknown", None


def _clean_text_expr(col: str) -> pl.Expr:
    # Use Unicode property + joiners; replace with " " (space), not ""
    EMOJI_RE = r"(?:\p{Extended_Pictographic}|\uFE0F|\u200D|\u20E3)"
    
    return (
        pl.col(col).cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.replace_all(EMOJI_RE, " ")     # changes for a space, due to same words getting fusioned (preventapreventa)
        .str.normalize("NFKD")
        .str.replace_all(r"\p{M}+", "")
        .str.to_uppercase()
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
    )

# Used in dict, prefer this structure because polars cannot (or is hard to) clean dictionaries
def clean_dict_keys(d):
    if not isinstance(d, dict):
        return {}
    cleaned_dict = {}
    for key, value in d.items():
        clean_key = (
            unicodedata.normalize("NFKD", key).encode("ascii", "ignore").decode("utf-8")
        )
        clean_key = re.sub(r"[^a-zA-Z0-9\s]", "", clean_key).lower().replace(" ", "_")
        cleaned_dict[clean_key] = value
    return cleaned_dict

# 2) tiny normalizer: JSON string -> normalized JSON string (same semantics as pandas)
## Function similar to pandas code
### all_sources['amenidades'] = all_sources['amenidades'].apply(lambda x: clean_dict_keys(json.loads(x)) if isinstance(x, str) and x.startswith('{') else {})
### lstrip removes leading spaces characters
def _normalize_amenidades_json(string: str) -> str:
    if isinstance(string, str) and string.lstrip().startswith("{"):
        try:
            d = json.loads(string)
            d = clean_dict_keys(d)  # <- identical normalization to pandas
            def v2s(v):
                if v is None:
                    return ""
                if isinstance(v, bool):
                    return "true" if v else "false"
                if isinstance(v, (int, float)):
                    return str(int(v)) if isinstance(v, float) and v.is_integer() else str(v)
                return str(v)
            return json.dumps({k: v2s(v) for k, v in d.items()}, ensure_ascii=False)
        except Exception:
            return "{}"
    return "{}"

def extract_development_id(col: str) -> pl.Expr:
    url = pl.col(col).cast(pl.Utf8, strict=False)

    return (
        pl.when(url.is_not_null()) # simulates `isinstance(url, str)`
        # If it a string, then extract desarrollo id base on regex expression
        .then(
            pl.when(url.str.contains(r"inmuebles24\.com"))
            .then(url.str.extract(r"desarrollo/([^/]+)-\d+\.html", group_index=1))
            .otherwise(
                pl.when(url.str.contains(r"lamudi\.com\.mx"))
                .then(url.str.extract(r"desarrollo/([a-zA-Z0-9\-]+)$", group_index=1))
                .otherwise(pl.lit(None, dtype=pl.Utf8))
            )
        )
        # If is not string, returns None
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )

def _align_schema(
    lf: pl.LazyFrame, cols: list[str], dmap: dict[str, pl.DataType]
) -> pl.LazyFrame:
    TEXT = pl.Utf8
    schema_set = set(lf.collect_schema().names())


    # Add missing columns as None
    missing = [column for column in cols if column not in schema_set]
    if missing:
        lf = lf.with_columns(
            [pl.lit(None, dtype=dmap.get(column, TEXT)).alias(column) for column in missing]
        )

    # Cast existing columns to mapped polar types
    casts = []
    for column in cols:
        if column in schema_set and column in dmap:
            casts.append(pl.col(column).cast(dmap[column], strict=False).alias(column))
    if casts:
        lf = lf.with_columns(casts)
    return lf.select(cols)

def find_potential_duplicates(
    dataframe: pl.DataFrame,
    radius_km: float = 0.2,
    batch_size: int = 25_000,
    show_progress: bool = False,
) -> pl.DataFrame:
    """
    Same results (as sets) as a fully vectorized version, but uses a streaming,
    batch-by-batch neighbor query to avoid building giant arrays.
    *_global are DataFrame row indices
    *_local are BallTree indices
    """
    # Why?
    df = dataframe.with_row_index(name="__pos")

    # --- Prepare arrays (NumPy) ---
    lat = df.get_column("lat").cast(pl.Float64).to_numpy()
    lon = df.get_column("lon").cast(pl.Float64).to_numpy()

    has_valid_coordinates = np.isfinite(lat) & np.isfinite(lon)

    # local index (ball tree) to global index (dataframe)
    has_valid_coordinates_idx_all = np.flatnonzero(has_valid_coordinates)

    id_inner = df.schema.get("id", pl.Utf8)

    print(f"id_inner type is {id_inner}")

    list_dtype = pl.List(id_inner)

    if has_valid_coordinates_idx_all.size == 0:
        # TODO modify return to raise exception
        print(f"No valid indexes to calculate: valid ids size is {has_valid_coordinates_idx_all}")
        return df.with_columns(
            pl.lit([], dtype=list_dtype).alias("potential_duplicates")
        )

    # Take radians coordinates to BallTree
    coords_tree = np.column_stack([np.radians(lat[has_valid_coordinates]), np.radians(lon[has_valid_coordinates])])
    tree = BallTree(coords_tree, metric="haversine")

    # Calculate distance threshold in radians
    radius_rad = radius_km / EARTH_RADIUS_KM

    # Filter active flag
    ## TODO check 
    ## Centers (active ∧ valid)
    if "is_active" in df.columns:
        is_active = (
            df.get_column("is_active").fill_null(True).cast(pl.Boolean).to_numpy()
        )
    else:
        is_active = np.ones(df.height, dtype=bool)

    # Filter properties for active and valid properties
    ## centers_mask
    active_and_valid_properties = is_active & has_valid_coordinates

    # Global indices of filtered properties => array of integer positions
    # centers_global
    filtered_properties = np.flatnonzero(active_and_valid_properties)

    # set all array to -1 (invalid) => lookup table
    ## BallTree needs a local index to form its queries
    ## It does this by querying inside its coordinates tree
    ## Mapping global to local
    lookup_to_balltree = np.full(df.height, -1, dtype=int)

    # For valid properties, match their position inside the ball tree
    lookup_to_balltree[has_valid_coordinates_idx_all] = np.arange(has_valid_coordinates_idx_all.size)

    # centers_local
    # one to one with centers global, but local indices
    queried_properties_balltree = lookup_to_balltree[filtered_properties]

    # Cleaning variables
    price = (
        df.get_column("asking_price").cast(pl.Float64).to_numpy()
        if "asking_price" in df.columns
        else np.full(df.height, np.nan)
    )
    m2 = (
        df.get_column("m2_built").cast(pl.Float64).to_numpy()
        if "m2_built" in df.columns
        else np.full(df.height, np.nan)
    )
    beds = (
        df.get_column("no_bedrooms").cast(pl.Float64).to_numpy()
        if "no_bedrooms" in df.columns
        else np.full(df.height, np.nan)
    )
    baths = (
        df.get_column("no_full_bathrooms").cast(pl.Float64).to_numpy()
        if "no_full_bathrooms" in df.columns
        else np.full(df.height, np.nan)
    )
    id_dev_list = (
        df.get_column("id_dev").to_list()
        if "id_dev" in df.columns
        else [None] * df.height
    )
    ids_out = (
        df.get_column("id").to_list() if "id" in df.columns else list(range(df.height))
    )

    def _is_null(x):
        return x is None or (isinstance(x, float) and np.isnan(x))

    # list of rows?
    results: list[list] = [[] for _ in range(df.height)]

    # tqdm does not work
    batch_starts = range(0, len(filtered_properties), batch_size)

    for start in batch_starts:
        end = min(start + batch_size, len(filtered_properties))
        filtered_properties_batch = filtered_properties[start:end]
        queried_properties_balltree_batch = queried_properties_balltree[start:end]

        # neighbors_local_by_center
        neighbors_list = tree.query_radius(
            coords_tree[queried_properties_balltree_batch], r=radius_rad
        )

        for index, neighbors in enumerate(neighbors_list):
            property_row = filtered_properties_batch[index]
            # price[property], m2[property], beds[property],  baths[property] = price[property], m2[property], beds[property], baths[property]
            # id_dev_list[property] = id_dev_list[property]

            for neighbor_index in neighbors:
                neighbor_property = has_valid_coordinates_idx_all[neighbor_index]
                if neighbor_property == property_row:
                    continue

                # Asking price
                # price[neighbor_property] = price[neighbor_property]
                if not (
                    np.isnan(price[property_row])
                    or np.isnan(price[neighbor_property])
                    or (
                        price[neighbor_property] >= price[property_row] * 0.85
                        and price[neighbor_property] <= price[property_row] * 1.15
                    )
                ):
                    continue

                # Built area
                # m2[neighbor_property] = m2[neighbor_property]
                if not (
                    np.isnan(m2[property_row])
                    or np.isnan(m2[neighbor_property])
                    or (
                        m2[neighbor_property] >= m2[property_row] * 0.95
                        and m2[neighbor_property] <= m2[property_row] * 1.05
                    )
                ):
                    continue

                # Bedrooms
                # beds[neighbor_property] = beds[neighbor_property]
                if not (
                    np.isnan(beds[property_row])
                    or np.isnan(beds[neighbor_property])
                    or (beds[property_row] == beds[neighbor_property])
                ):
                    continue

                # Bathrooms
                # baths[neighbor_property] = baths[neighbor_property]
                if not (
                    np.isnan(baths[property_row])
                    or np.isnan(baths[neighbor_property])
                    or (baths[property_row] == baths[neighbor_property])
                ):
                    continue

                # Ensure id_dev differs; otherwise skip
                # id_dev_list[neighbor_property] = id_dev_list[neighbor_property]
                if not (
                    _is_null(id_dev_list[property_row])
                    or _is_null(id_dev_list[neighbor_property])
                    or (id_dev_list[property_row] != id_dev_list[neighbor_property])
                ):
                    continue

                results[property_row].append(ids_out[neighbor_property])

    out = df.with_columns(pl.Series("potential_duplicates", results, dtype=list_dtype))
    return out

def _sniff_delimiter(sample: bytes) -> str:
    """
    Very light heuristic: pick ',' or ';' by which appears more in the sample's first lines.
    Falls back to ','.
    """
    try:
        head = sample.splitlines()[:50]
        commas = sum(line.count(b",") for line in head)
        semis  = sum(line.count(b";") for line in head)
        return ";" if semis > commas else ","
    except Exception:
        return ","


def _read_csv_lazy_from_zip(
    zip_file: zipfile.ZipFile,
    file_name: str,
    *,
    dtypes: dict[str, pl.DataType] | None = None,
    null_values: list[str] | None = None,
) -> pl.LazyFrame:
    """
    Stream a CSV file out of a ZipFile and return a Polars LazyFrame
    WITHOUT loading the whole file to disk. Robust to ragged lines and delimiter variation.
    # WITHOUT loading the whole file into RAM.
    """
    dtypes = dtypes or {}
    null_values = null_values or ["None", "NULL", ""]

    with zip_file.open(file_name) as file_sniff:
        # data = file.read()
        head = file_sniff.peek(256*1024)
    
    sep = _sniff_delimiter(head)

    # sep = _sniff_delimiter(data)

    def _try_read(truncate_ragged: bool) -> pl.DataFrame:
        with zip_file.open(file_name) as file:
            return pl.read_csv(
                # BytesIO(data),
                file,
                dtypes=dtypes,                 # partial dict is fine
                null_values=null_values,
                ignore_errors=True,            # skip bad rows instead of raising
                low_memory=True,
                infer_schema_length=0,         # we supplied dtypes; skip long inference
                has_header=True,
                separator=sep,
                encoding="utf8-lossy",
                truncate_ragged_lines=truncate_ragged,
            )

    try:
        df = _try_read(truncate_ragged=False)
    except Exception as e1:
        # Retry for files like lamudi_1.csv with extra fields per row
        try:
            print("Retrying csv read with ragged rows (extra fields)")
            df = _try_read(truncate_ragged=True)
        except Exception as e2:
            raise RuntimeError(
                f"Failed to parse CSV '{file_name}' (sep='{sep}'). "
                f"Original: {e1}; Retry with truncate_ragged_lines: {e2}"
            )

    return df.lazy()


def _standardize_one_source_lazy(
    lf: pl.LazyFrame,
    source_name: str,
    *,
    operation_type_upper: Optional[str],
    common_columns: list[str],
    log_shape: bool = False,
) -> pl.LazyFrame:
    """
    Same logic as _standardize_one_source, but starts from an existing LazyFrame
    (so we don't re-read from disk).
    """
    # subset unique IDs (if present)
    schema_names = set(lf.collect_schema().names())

    if "id" in schema_names:
        lf = lf.with_columns(pl.col("id").cast(pl.Utf8)).unique(subset=["id"])

    # drop nulls for key numeric columns
    need_non_null = [c for c in ["asking_price", "lat", "lon"] if c in schema_names]
    if need_non_null:
        lf = lf.drop_nulls(subset=need_non_null)

    # zip_code -> strip + Utf8
    if "zip_code" in schema_names:
        lf = lf.with_columns(pl.col("zip_code").cast(pl.Utf8).str.strip_chars())

    # filter operation_type if requested
    if operation_type_upper and "operation_type" in schema_names:
        lf = lf.filter(
            pl.col("operation_type").cast(pl.Utf8).str.to_uppercase() == operation_type_upper
        )

    # filter by currency/price
    if "currency" in schema_names and "asking_price" in schema_names:
        lf = lf.filter(
            (
                pl.col("currency").is_in(["MXN", "MXP"]) & (pl.col("asking_price") >= 500_000)
            )
            | ((pl.col("currency") == "USD") & (pl.col("asking_price") >= 25_000))
        )

    # keep just the common columns that exist
    keep_cols = [c for c in common_columns if c in schema_names]
    if keep_cols:
        lf = lf.select(keep_cols)

    # add source
    lf = lf.with_columns(pl.lit(source_name).alias("source"))

    if log_shape:
        try:
            df_dbg = lf.collect()
            print(f"[{source_name}] shape: {df_dbg.shape}")
        except Exception as e:
            print(f"[{source_name}] could not compute shape: {e}")

    return lf

## Export data types to pandas so then posgresql can use it
def convert_to_python_native(obj):
    if isinstance(obj, dict):
        return {convert_to_python_native(k): convert_to_python_native(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_python_native(i) for i in obj]
    elif isinstance(obj, (np.integer, np.floating)):
        return obj.item()
    elif isinstance(obj, (np.ndarray,)):
        return obj.tolist()
    else:
        return obj

# -----------------------------------------------------------
# Postgres connection resolution (Airflow Conn → Hook)
# -----------------------------------------------------------
def get_pg_engine(conn_id: str | None = None) -> Engine:
    """
    Resolve a Postgres SQLAlchemy Engine using an Airflow Connection id.
    Resolution order:
      1) explicit conn_id argument (if provided)
      2) Airflow Variable 'SODACAPITAL_PG' (if defined)
      3) literal 'SODACAPITAL_PG'
    Then:
      - Try BaseHook.get_connection(resolved) -> create_engine(uri)
      - Fallback to PostgresHook: get_sqlalchemy_engine() or get_uri()
    """
    from sqlalchemy.engine import URL

    conn_id = "SODACAPITAL_PG"
    conn = Connection.get(conn_id)
    pg_url = URL.create(
        "postgresql+psycopg2",
        username=conn.login,
        password=conn.password,
        host=conn.host,
        port=int(conn.port) if conn.port else None,
        database=conn.schema,
    )
    return create_engine(pg_url)

    # try:
    #     from airflow.hooks.base import BaseHook
    #     conn = BaseHook.get_connection(conn_id)
    #     eng = create_engine(conn.get_uri())
    #     # Optional: safe debug
    #     try:
    #         url_obj = getattr(eng, "url", None)
    #         dbname = getattr(url_obj, "database", None)
    #         print(f"[get_pg_engine] using Airflow BaseHook connection id='{conn_id}' for database='{dbname or '?'}'")
    #     except Exception:
    #         pass
    #     return eng
    # except Exception as e:
    #     print(f"[get_pg_engine] BaseHook lookup failed for '{conn_id}': {e!r}")

    # # Final fallback: PostgresHook
    # try:
    #     from airflow.providers.postgres.hooks.postgres import PostgresHook
    #     hook = PostgresHook(postgres_conn_id=conn_id)
    #     try:
    #         eng = hook.get_sqlalchemy_engine()
    #         print(f"[get_pg_engine] using PostgresHook.get_sqlalchemy_engine() for '{conn_id}'")
    #         return eng
    #     except Exception:
    #         # Some provider versions expose only a DSN/URI—build an Engine from it
    #         try:
    #             dsn = hook.get_uri()  # Airflow ≥ 2.9
    #         except Exception as ue:
    #             raise RuntimeError("Could not derive a DSN from PostgresHook.") from ue
    #         eng = create_engine(dsn)
    #         print(f"[get_pg_engine] using PostgresHook.get_uri() for '{conn_id}'")
    #         return eng
    # except Exception as e:
    #     print(f"[get_pg_engine] PostgresHook failed for '{conn_id}': {e!r}")

    # # If we reach here, we failed every path
    # raise RuntimeError("Could not obtain a Postgres connection/engine")


# -----------------------------------------------------------
# Small helpers
# -----------------------------------------------------------
def get_table_columns(engine: Engine, schema: str, table: str) -> List[str]:
    with engine.begin() as c:
        rows = c.execute(T("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=:s AND table_name=:t
            ORDER BY ordinal_position
        """), {"s": schema, "t": table}).all()
    return [r[0] for r in rows]

def ensure_table_exists(engine: Engine, schema: str, table: str) -> None:
    with engine.begin() as c:
        ok = c.execute(T("""
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema=:s AND table_name=:t
            )
        """), {"s": schema, "t": table}).scalar()
        if not ok:
            raise RuntimeError(f"Destination table not found: {schema}.{table}")

def ensure_tables_exist_or_clone(
    engine: Engine,
    *,
    staging_schema: str,
    table_pairs: List[Tuple[str, str]],
    template_schema: Optional[str] = None,
) -> None:
    template_schema = template_schema or staging_schema
    for staging_table, template_table in table_pairs:
        if staging_table == template_table:
            ensure_table_exists(engine, staging_schema, staging_table)
            continue
        with engine.connect() as c:
            exists = c.execute(T("""
                SELECT EXISTS (
                  SELECT 1 FROM information_schema.tables
                  WHERE table_schema=:s AND table_name=:t
                )
            """), {"s": staging_schema, "t": staging_table}).scalar()
        if exists:
            continue
        ensure_table_exists(engine, template_schema, template_table)
        print(
            f"Creating staging table {staging_schema}.{staging_table} LIKE {template_schema}.{template_table}"
        )
        with engine.begin() as c:
            c.execute(T(
                f'CREATE TABLE "{staging_schema}"."{staging_table}" '
                f'(LIKE "{template_schema}"."{template_table}" INCLUDING ALL)'
            ))
            
def parse_fqn(fqn: str) -> Tuple[str, str]:
    if "." in fqn:
        s, t = fqn.split(".", 1)
        return s, t
    return "public", fqn


# -----------------------------------------------------------
# Bounded-memory streamer from Parquet(S3) → Postgres COPY
# -----------------------------------------------------------
def upload_large_parquet_streaming(
    s3_url: str,                 # "s3://bucket/path/to/file.parquet" or a prefix with many files
    target_fqn: str,             # "schema.table"
    *,
    engine: Engine,
    aws_conn_id: Optional[str] = None,
    batch_rows: int = 128_000,   # Arrow batch; tune 64k–256k
    csv_chunk_bytes: int,  # ~4MB chunks over the COPY socket
    include_header: bool = False,
) -> int:
    """
    Streams Parquet from S3 in Arrow record batches and pipes small CSV chunks
    into Postgres COPY. Memory stays ~O(batch) + O(csv_chunk_bytes).
    Returns -1 (rowcount unknown).
    """
    # Build S3 filesystem (use Airflow creds if available)
    s3fs_kwargs = {}
    if aws_conn_id and S3Hook is not None:
        hook = S3Hook(aws_conn_id=aws_conn_id)
        sess = hook.get_session()
        creds = hook.get_credentials()
        s3fs_kwargs = dict(
            access_key=creds.access_key,
            secret_key=creds.secret_key,
            session_token=getattr(creds, "token", None),
            region=sess.region_name or "us-east-1",
        )
    s3fs_kwargs.setdefault("connect_timeout", 60.0)
    s3fs_kwargs.setdefault("request_timeout", 600.0)
    s3fs = pafs.S3FileSystem(**s3fs_kwargs)

    schema, table = parse_fqn(target_fqn)
    ensure_table_exists(engine, schema, table)
    cols = get_table_columns(engine, schema, table)
    if not cols:
        raise RuntimeError(f"No columns in destination table: {schema}.{table}")

    # Generator: Arrow → CSV text chunks
    def csv_chunks() -> Iterable[bytes]:
        dataset_path = s3_url
        parsed = urllib.parse.urlparse(s3_url)
        if parsed.scheme == "s3":
            key = parsed.path.lstrip("/")
            dataset_path = parsed.netloc if not key else f"{parsed.netloc}/{key}"
        ds = pads.dataset(dataset_path, filesystem=s3fs, format="parquet")
        scanner = ds.scanner(batch_size=batch_rows)

        first = True
        for rb in scanner.to_batches():
            tbl = pa.Table.from_batches([rb])

            # add missing cols as nulls, order exactly like destination
            missing = [c for c in cols if c not in tbl.column_names]
            for m in missing:
                tbl = tbl.append_column(m, pa.nulls(len(tbl)))
            tbl = tbl.select(cols)

            sink = io.BytesIO()
            pacsv.write_csv(
                tbl,
                sink,
                write_options=pacsv.WriteOptions(include_header=(include_header and first)),
            )
            first = False
            # buf = sink.getvalue().decode("utf-8")
            buf = sink.getvalue()

            mv = memoryview(buf)    # zero-copy slicing
            
            # yield in steady, smaller chunks
            for i in range(0, len(buf), csv_chunk_bytes):
                # yield buf[i:i+csv_chunk_bytes]
                yield mv[i:i+csv_chunk_bytes]

    # class IterableTextIO(io.TextIOBase):
    class IterableBytesIO(io.RawIOBase):
        """
        A file-like object that serves bytes produced by an iterator.
        Psycopg2's copy_expert() will call read() repeatedly until EOF.
        """
        # def __init__(self, it: Iterable[str]):
        #     self._it = iter(it)
        #     self._cache = ""
        def __init__(self, it):
            self._it = iter(it)   # iterator of bytes-like chunks
            self._buf = bytearray()
        
        def readable(self):
            return True

        # def read(self, n: Optional[int] = None) -> str:
        #     # psycopg2 calls read(None) or read(small_n); serve from a small cache
        #     if n is None:
        #         # drain the iterator fully (COPY completes on EOF)
        #         buf = [self._cache]
        #         self._cache = ""
        #         buf.extend(list(self._it))
        #         return "".join(buf)
        #     # Serve exactly up to n bytes
        #     while len(self._cache) < n:
        #         try:
        #             self._cache += next(self._it)
        #         except StopIteration:
        #             break
        #     out, self._cache = self._cache[:n], self._cache[n:]
        #     return out

        def read(self, n=-1):
            # drain all if n < 0
            if n is None or n < 0:
                for chunk in self._it:
                    self._buf += chunk
                out = bytes(self._buf)
                self._buf.clear()
                return out

            # ensure we have >= n bytes buffered (or hit EOF)
            while len(self._buf) < n:
                try:
                    self._buf += next(self._it)
                except StopIteration:
                    break
            out = bytes(self._buf[:n])
            del self._buf[:n]
            return out


        def readinto(self, b):
            # fill the provided writable bytes-like buffer 'b'
            n = len(b)
            if n == 0:
                return 0
            # ensure we have at least n bytes buffered
            while len(self._buf) < n:
                try:
                    self._buf += next(self._it)
                except StopIteration:
                    break
            to_copy = min(n, len(self._buf))
            b[:to_copy] = self._buf[:to_copy]
            del self._buf[:to_copy]
            return to_copy or 0  # 0 signals EOF to callers

    collist = ", ".join(f'"{c}"' for c in cols)
    copy_sql = f'COPY "{schema}"."{table}" ({collist}) FROM STDIN WITH (FORMAT csv, HEADER false)' # TODO 

    with engine.begin() as conn:
        # defensive knobs for long COPY
        conn.execute(T("SET LOCAL statement_timeout='0'"))
        conn.execute(T("SET LOCAL lock_timeout='5min'"))

        raw = conn.connection if hasattr(conn, "connection") else conn
        cur = raw.cursor()
        try:
            # cur.copy_expert(copy_sql, file=IterableTextIO(csv_chunks()))
            cur.copy_expert(copy_sql, file=IterableBytesIO(csv_chunks()))
        finally:
            cur.close()
    return -1


# -----------------------------------------------------------
# Staging helpers (truncate, promote)
# -----------------------------------------------------------
def truncate_tables_restart_identity(engine: Engine, tables: List[str], schema: str) -> None:
    if not tables:
        return
    joined = ", ".join(f'"{schema}"."{t}"' for t in tables)
    with engine.begin() as c:
        c.execute(T(f"TRUNCATE {joined} RESTART IDENTITY CASCADE"))

def promote_staging_to_public(
    engine: Engine,
    *,
    src_schema: str,
    dst_schema: str,
    tables: List[Tuple[str, str]],
) -> None:
    """
    Promote data from src_schema → dst_schema in order.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine connected to the target Postgres.
    src_schema : str
        Schema to read from (typically your staging schema).
    dst_schema : str
        Schema to write to (typically your public/prod schema).
    tables : list[tuple[str, str]]
        Ordered list of (src_table, dst_table). The list order is the promotion order.
        Example:
            [("dev_mkt_listings_main_stg", "dev_mkt_listings_main"),
             ("dev_mkt_listings_log_stg",  "dev_mkt_listings_log")]

    Notes
    -----
    - For each (src, dst), this will:
        1) LOCK dst IN SHARE MODE
        2) TRUNCATE dst RESTART IDENTITY CASCADE
        3) INSERT INTO dst SELECT * FROM src
    - Safety: if src_schema == dst_schema AND src == dst, we raise to avoid truncating
      the same table we’re copying from.
    """
    if not tables:
        raise ValueError("`tables` must be a non-empty list of (src_table, dst_table) tuples.")
    
    # Validate all specs up-front for clearer errors before any truncation occurs
    for spec in tables:
        if not (isinstance(spec, tuple) and len(spec) == 2):
            raise ValueError(f"Table specification must be a (src_table, dst_table) tuple, got: {spec!r}")
        src, dst = spec
        if not (isinstance(src, str) and isinstance(dst, str) and src and dst):
            raise ValueError(f"Invalid table names in spec {spec!r}. Both must be non-empty strings.")
        if src_schema == dst_schema and src == dst:
            raise ValueError(
                f"Unsafe spec {spec!r}: source and destination are identical in the same schema "
                f"({src_schema}). This would TRUNCATE the source table."
            )

    with engine.begin() as c:
        for src, dst in tables:
            # Lock the destination to avoid concurrent readers seeing partial states
            c.execute(T(f'LOCK TABLE "{dst_schema}"."{dst}" IN SHARE MODE'))
            # Reset destination
            c.execute(T(f'TRUNCATE TABLE "{dst_schema}"."{dst}" RESTART IDENTITY CASCADE'))
            # Copy from source
            c.execute(T(
                f'INSERT INTO "{dst_schema}"."{dst}" SELECT * FROM "{src_schema}"."{src}"'
            ))



## Apply conversion to postgres
def convert_for_postgres(obj, as_string=False):
    obj = convert_to_python_native(obj)
    return json.dumps(obj) if as_string else obj

@dag(
    # Manual run
    schedule=None,
    start_date=pendulum.datetime(2025, 8, 1, tz=LOCAL_TZ),
    catchup=False,
    tags=["S3", "CSV", "simple"],
    max_active_runs=1
)
def dag_clean_update_listings_data():

    @task(multiple_outputs=False)
    def get_runtime_cfg() -> dict:
        """
        Resolve runtime config and validate S3 access (head_bucket).
        Returns a JSON-serializable dict; do NOT return the boto3 client.
        """

        # --- Required Airflow Variables ---
        bucket         = _required_variable("S3_BUCKET")
        data_folder    = _required_variable("S3_DATA_FOLDER")
        backup_folder  = _required_variable("S3_BACKUP_FOLDER")
        export_folder  = _required_variable("S3_EXPORT_FOLDER")

        # Normalize folder slashes
        if not backup_folder.endswith("/"):
            backup_folder += "/"
        if not export_folder.endswith("/"):
            export_folder += "/"

        # Detect a Connection named "AWS_CONN_ID" (don’t read secrets here)
        try:
            Connection.get("AWS_CONN_ID")
            aws_conn_id = "AWS_CONN_ID"
            auth_mode = "connection"
        except AirflowNotFoundException:
            aws_conn_id = None
            auth_mode = "env"

        region = "us-east-1"
        boto_cfg = BotoConfig(
            retries={"max_attempts": 10, "mode": "standard"},
            connect_timeout=10,
            read_timeout=120,
            s3={"addressing_style": "path", "use_dualstack_endpoint": False, "use_accelerate_endpoint": False},
        )

        # --- Build S3 client from Connection
        if auth_mode == "connection" and aws_conn_id:
            try:
                aws_conn = Connection.get("AWS_CONN_ID")
                extra = json.loads(aws_conn.extra or "{}")
                region = extra.get("region_name") or region
                s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=aws_conn.login,
                    aws_secret_access_key=aws_conn.password,
                    region_name=region,
                    config=boto_cfg
                )
                auth_mode = "connection"
                print("[get_runtime_cfg] Using AWS_CONN_ID credentials.")

            except (AirflowNotFoundException, NoCredentialsError, PartialCredentialsError) as e:
                print(f"[get_runtime_cfg] Fallback to default AWS credentials (IAM/env). Cause: {e}")
                s3_client = boto3.client("s3", region_name=region, config=boto_cfg)
        else:
            print(f"[get_runtime_cfg] Fallback because aws_conn_id is not set. Value is {aws_conn_id}")
            s3_client = boto3.client("s3", region_name=region, config=boto_cfg)

        # --- Validate bucket reachability ---
        try:
            s3_client.head_bucket(Bucket=bucket)
            print(f"[get_runtime_cfg] S3 connection OK. Bucket reachable: s3://{bucket}/ (auth={auth_mode}, region={region})")
        except ClientError as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code")
            if code in {"404", "NoSuchBucket"}:
                raise ValueError(f"S3 bucket not found: '{bucket}'.") from e
            if code in {"403", "AccessDenied"}:
                raise PermissionError(
                    f"Access denied to bucket '{bucket}'. Check IAM permissions / bucket policy."
                ) from e
            raise RuntimeError(
                f"Could not access bucket '{bucket}'. AWS error code: {code or 'unknown'}"
            ) from e

        # --- Postgres Connection (treat as Connection ID) ---
        pg_conn_id = "SODACAPITAL_PG"
        # pg_uri = None

        # try:
        #     c = Connection.get(pg_conn_id)  # SDK connection (works in normal tasks)
        #     pg_uri = str(_SA_URL.create(
        #         "postgresql+psycopg2",
        #         username=c.login,
        #         password=c.password,        # URL.create will handle quoting safely
        #         host=c.host,
        #         port=int(c.port) if c.port else None,
        #         database=c.schema,
        #     ))
        #     print("[get_runtime_cfg] Injecting pg_uri for downstream (masked).")
        # except Exception as e:
        #     print(f"[get_runtime_cfg] Could not resolve '{pg_conn_id}': {e}")

        # --- Return JSON-serializable config for downstream tasks ---
        cfg = {
            "bucket": bucket,
            "region": region,
            "auth": auth_mode,
            "data_folder": data_folder,
            "backup_folder": backup_folder,
            "export_folder": export_folder,
            "aws_conn_id": aws_conn_id,
            "pg_conn_id": pg_conn_id,
            # "pg_uri": pg_uri,
        }
        # Drop None values to avoid any SDK validation weirdness
        cfg = {k: v for k, v in cfg.items() if v is not None}

        print("[get_runtime_cfg] returning keys:", list(cfg.keys()))
        return cfg

    @task()
    def list_and_read_all(_runtime_cfg: dict | None = None):
        """
        List all folders and files in the bucket (optionally under a prefix)
        and read CSV files to validate accessibility.

        - Use DAG param 'prefix' to scope listing.
        - Prints summaries; avoids large XCom payloads.
        """
        context = get_current_context()
        cfg = _runtime_cfg
        bucket = cfg.get("bucket")
        region = cfg.get("region")
        auth_mode = cfg.get("auth")

        param_prefix = context.get("params", {}).get("prefix")
        prefix = (
            param_prefix.strip()
            if isinstance(param_prefix, str) and param_prefix.strip()
            else None
        )

        client = None
        if auth_mode == "connection":
            try:
                conn = Connection.get("AWS_CONN_ID")
                client = boto3.client(
                    "s3",
                    aws_access_key_id=conn.login,
                    aws_secret_access_key=conn.password,
                    region_name=region,
                )
            except (AirflowNotFoundException, NoCredentialsError, PartialCredentialsError) as e:
                print(f"[list_and_read_all] Fallback to default AWS credentials (IAM/env). Cause: {e}")
                client = boto3.client("s3", region_name=region)
        else:
            client = boto3.client("s3", region_name=region)

        paginator = client.get_paginator("list_objects_v2")
        kwargs = {"Bucket": bucket}
        if prefix:
            kwargs["Prefix"] = prefix
        page_iteration = paginator.paginate(**kwargs)

        keys = []
        folders = set()
        total_bytes = 0
        for page in page_iteration:
            contents = page.get("Contents", []) or []
            for object in contents:
                key = object.get("Key")
                if not key:
                    continue
                keys.append(key)
                total_bytes += int(object.get("Size") or 0)
                if "/" in key:
                    folders.add(key.rsplit("/", 1)[0])
        
        total_gb = round(total_bytes / 1_000_000_000, 2)

        print(f"Bucket: s3://{bucket}/")
        if prefix:
            print(f"Prefix: {prefix}")
        print(f"Discovered {len(folders)} folders and {len(keys)} files, total ~{total_gb} GB")

        # Prepare sorted samples and pretty printing
        sample_folders = sorted(list(folders))[:10]
        sample_files = sorted(keys)[:10]

        if sample_folders:
            print("Sample folders (max 10):")
            for f in sample_folders:
                print(f"- {f}")
        else:
            print("Sample folders (max 10): <none>")

        if sample_files:
            print("Sample files (max 10):")
            for f in sample_files:
                print(f"- {f}")
        else:
            print("Sample files (max 10): <none>")

        return {
            "bucket": bucket,
            "prefix": prefix or "",
            "folders_count": len(folders),
            "files_count": len(keys),
            "sample_folders": sample_folders,
            "sample_files": sample_files,
        }

    @task()
    def get_data_folders(_runtime_cfg: dict | None = None):
        """
        List all date-named folders under base prefix and the files inside each.
        Collect all dates, determine the most recent (max) and the previous (lagged, max-1),
        and return both S3 prefixes.
        Folder names must be 'YYYY-MM-DD'.
        """

        cfg = _runtime_cfg
        bucket = cfg.get("bucket")
        region = cfg.get("region")
        auth_mode = cfg.get("auth")
        data_folder = cfg.get("data_folder")

        # normalize to end with exactly one slash
        if not data_folder.endswith("/"):
            data_folder = data_folder + "/"

        client = None
        if auth_mode == "connection":
            try:
                conn = Connection.get("AWS_CONN_ID")
                client = boto3.client(
                    "s3",
                    aws_access_key_id=conn.login,
                    aws_secret_access_key=conn.password,
                    region_name=region,
                )
            except (AirflowNotFoundException, NoCredentialsError, PartialCredentialsError) as e:
                print(f"[list_and_read_all] Fallback to default AWS credentials (IAM/env). Cause: {e}")
                client = boto3.client("s3", region_name=region)
        else:
            client = boto3.client("s3", region_name=region)
            
        # Discover immediate subfolders under data_folder
        paginator = client.get_paginator("list_objects_v2")
        page_iteration = paginator.paginate(Bucket=bucket, Prefix=data_folder, Delimiter="/")

        date_folders: list[tuple[dt, str]] = []
        for page in page_iteration:
            for prefix in page.get("CommonPrefixes", []) or []:
                pfx = prefix.get("Prefix") or ""
                if not pfx.startswith(data_folder):
                    continue
                name = pfx[len(data_folder):].rstrip("/")
                try:
                    parsed = dt.strptime(name, "%Y-%m-%d")
                    date_folders.append((parsed, name))
                except Exception:
                    # skip non-date folders
                    pass

        if not date_folders:
            raise RuntimeError(f"No date folders found under s3://{bucket}/{data_folder}")

        # List all folders and their files => collect all dates
        print(f"Discovered {len(date_folders)} date folder(s) under s3://{bucket}/{data_folder}")
        all_dates = [date for date, _ in date_folders]

        # Sort by date for readable output
        for parsed_dt, folder_name in sorted(date_folders, key=lambda t: t[0]):
            folder_prefix = f"{data_folder}{folder_name}/"
            print(f"Folder: {folder_name} (folder=s3://{bucket}/{folder_prefix})")

            # List files inside this folder
            files: list[str] = []
            total_bytes = 0
            for page in paginator.paginate(Bucket=bucket, Prefix=folder_prefix):
                for obj in page.get("Contents", []) or []:
                    key = obj.get("Key")
                    if not key or key.endswith("/"):
                        continue
                    files.append(key)
                    total_bytes += int(obj.get("Size") or 0)

            files_sorted = sorted(files)
            total_gb = round(total_bytes / 1_000_000_000, 2)
            print(f"- {len(files_sorted)} file(s), ~{total_gb} GB")
            for key in files_sorted:
                print(f"  - {key}")

        # Choose most recent date and its predecessor
        all_dates_sorted = sorted(all_dates)
        latest_dt = all_dates_sorted[-1]
        # prev_dt = all_dates_sorted[-2] if len(all_dates_sorted) > 1 else None # TODO => exclude
        if len(all_dates_sorted) > 1:
            prev_dt = all_dates_sorted[-2]
        else:
            prev_dt = None
            print("⚠️ Fallback: no previous date found, setting prev_dt = None")

        latest_data_folder = f"{data_folder}{latest_dt.strftime('%Y-%m-%d')}/"
        lagged_data_folder = f"{data_folder}{prev_dt.strftime('%Y-%m-%d')}/" if prev_dt else ""

        print(f"Latest data folder: s3://{bucket}/{latest_data_folder}")
        if lagged_data_folder:
            print(f"Lagged data folder: s3://{bucket}/{lagged_data_folder}")
        else:
            print("Lagged data folder: <none>")

        return {"latest_data_folder": latest_data_folder, "lagged_data_folder": lagged_data_folder}

    @task()
    def load_dataframes(
        _runtime_cfg: dict,
        folders: dict,
        _backup_done: dict | None = None):
        """
        List ZIPs under S3 prefix -> download -> read each CSV with Polars
        -> standardize (cast/unique/drop-nulls/zip_code clean + column subset)
        -> union per source -> align schema -> write Parquet per source.
        Returns {source_name: parquet_path}.
        """
        # Map to expected polars datatype
        # Integer was quitted due to value differences in amenities
        TEXT, F64, I64 = pl.Utf8, pl.Float64, pl.Int64
        DTYPE_MAP = {
            "id": TEXT,
            "title": TEXT,
            "amenidades": TEXT,
            "approximate_address": TEXT,
            "apt_floor": I64, # changed to I64 from F64
            "city": TEXT,
            "city_area": TEXT,
            "content": TEXT,
            "country": TEXT,
            "currency": TEXT, # parse later
            "date_scraped": TEXT,
            "del_mun": TEXT,
            "ext_number": TEXT,
            "int_number": TEXT,
            "main_development_url": TEXT,
            "neighborhood": TEXT,
            "operation_type": TEXT,
            "phone_number": TEXT,
            "property_type": TEXT,
            "publication_date": TEXT,
            "publisher_name": TEXT,
            "publisher_type": TEXT,
            "region": TEXT,
            "source_listing_url": TEXT,
            "source_type": TEXT,
            "state": TEXT,
            "street_name": TEXT,
            "zip_code": TEXT, # parse later
            "source": TEXT,
            # numeric
            "asking_price": F64,
            "maintenance_fee": F64,
            "lat": F64,
            "lon": F64,
            "m2_balconies": F64,
            "m2_built": TEXT,  # parse later
            "m2_rooftop": F64,
            "m2_terraces": F64,
            "m2_terrain": TEXT,  # parse later
            "no_bedrooms": F64,
            "no_full_bathrooms": F64,
            "no_half_bathrooms": F64,
            "no_parking_spaces": F64,
            "year_built": F64,
        }

        cfg = _runtime_cfg
        bucket = cfg.get("bucket")
        auth_mode = cfg.get("auth")

        latest_data_folder = folders.get("latest_data_folder") or ""

        if not latest_data_folder or not latest_data_folder.endswith('/'):
            raise ValueError("Parameter 'latest_data_folder' must be a non-empty S3 folder ending with '/'.")

        client = None
        if auth_mode == "connection":
            try:
                conn = Connection.get("AWS_CONN_ID")

                # Create boto3 client with explicit credentials
                client = boto3.client(
                    's3',
                    aws_access_key_id=conn.login,
                    aws_secret_access_key=conn.password,
                    region_name='us-east-1'
                )
            except (AirflowNotFoundException, NoCredentialsError, PartialCredentialsError) as e:
                print(f"Fallback to default credentials due to exception {e}")
                # Fall back to default credentials (IAM role, env vars)
                client = boto3.client('s3')

            try:
                client.head_bucket(Bucket=bucket)

            except ClientError as e:
                code = getattr(e, "response", {}).get("Error", {}).get("Code")
                if code in {"404", "NoSuchBucket"}:
                    raise ValueError(f"S3 bucket not found: '{bucket}'.") from e
                elif code in {"403", "AccessDenied"}:
                    raise PermissionError(
                        f"Access denied to bucket '{bucket}'. Check IAM permissions / bucket policy."
                    ) from e
                else:
                    raise RuntimeError(
                        f"Could not access bucket '{bucket}'. AWS error code: {code or 'unknown'}"
                    ) from e

        # Log bucket and latest prefix up front
        print(f"Bucket: {bucket}")
        print(f"Latest data folder: {latest_data_folder}")

        # List all zip objects under the provided prefix
        paginator = client.get_paginator("list_objects_v2")
        page_iteration = paginator.paginate(Bucket=bucket, Prefix=latest_data_folder)

        # Take zip files under latest data folder
        zip_keys: list[str] = []
        for page in page_iteration:
            for object in page.get("Contents", []) or []:
                key = object.get("Key") or ""
                if key.lower().endswith('.zip'):
                    zip_keys.append(key)

        if not zip_keys:
            print(f"No .zip files found under s3://{bucket}/{latest_data_folder}")
            return {}

        # Accumulate lazyframes per source; we’ll concat each group once
        by_source: Dict[str, List[pl.LazyFrame]] = {}

        # Define download config to multitask zip files
        transfer_config = TransferConfig(
            multipart_threshold = 8 * 1024 * 1024,   # files > 8MB use multipart
            multipart_chunksize = 16 * 1024 * 1024,  # 16MB per part
            max_concurrency    = 8,                  # 8 threads per file
            use_threads        = True,
        )


        with tempfile.TemporaryDirectory(prefix="latest_data_zip_dl_") as tmpdir:
            print(f"Processing {len(zip_keys)} ZIP archive(s); temp dir: {tmpdir}")

            for key in sorted(zip_keys):
                s3_path = f"s3://{bucket}/{key}"
                base = os.path.basename(key)                # lamudi.zip (ejemplo)
                local_zip = os.path.join(tmpdir, base)
                source_name, op_upper = _infer_source_and_operation(base)

                # Download ZIP to local temp path
                try:
                    client.download_file(bucket, key, local_zip, Config=transfer_config)
                except Exception as e:
                    print(f"Warning: failed to download {s3_path}: {e}")
                    continue

                # Log the ZIP path, then read CSVs using Polars
                print(f"ZIP path: {s3_path}")

                try:
                    # Extract all CSV files and build one Polars DF per CSV
                    with zipfile.ZipFile(local_zip) as zf:
                        csv_files = [n for n in zf.namelist() if n.lower().endswith('.csv')]
                        if not csv_files:
                            print(f"Warning: no CSV files found in {s3_path}")
                            continue

                        for file in csv_files:
                            csv_base = os.path.basename(file)
                            print(f"[{source_name}] reading CSV file from ZIP: {csv_base}")

                            # Read directly from the zipfile into a LazyFrame
                            lf_raw = _read_csv_lazy_from_zip(
                                zf,
                                file,
                                dtypes=DTYPE_MAP,                 # use your known dtypes to avoid inference
                                null_values=["None", "NULL", ""],
                            )

                            # Apply your standardization logic on the existing LazyFrame
                            lf = _standardize_one_source_lazy(
                                lf_raw,
                                source_name=source_name,
                                operation_type_upper=op_upper,
                                common_columns=common_columns,
                            )

                            by_source.setdefault(source_name, []).append(lf)


                except Exception as e:
                    print(f"Warning: failed to read {local_zip} with Polars: {e}")
            
            # Now union per source, align schema, and persist as Parquet
            outs: Dict[str, str] = {}
            targets = set(common_columns) | {"source"}

            for source_name, lfs in by_source.items():
                if not lfs:
                    continue
                lf = pl.concat(lfs, how="vertical")

                # Align schema to uniform dtypes you use downstream
                lf = _align_schema(lf, list(targets), DTYPE_MAP)

                # Persist (small XCom: return only file path)
                tmp = tempfile.NamedTemporaryFile(prefix=f"{source_name}_std_", suffix=".parquet", delete=False)
                tmp.close()
                df = lf.collect(streaming=True)
                df.write_parquet(tmp.name)
                outs[source_name] = tmp.name

                print(f"[{source_name}] standardized rows: {df.height:,} -> {tmp.name}")

            if not outs:
                print("No sources produced output parquet.")
        return outs


    @task()
    # small helper to pass context into the isolated venv task
    def _get_run_date() -> str:
        """
        Get the actual run data from dag context.
        """
        context_task_date = get_current_context()["logical_date"]
        run_date_str = context_task_date.strftime("%Y-%m-%d")
        return run_date_str

    @task()
    def backup(_runtime_cfg: dict, run_date_str: str, _loaded_names: list[str] | None = None) -> dict:
        import os
        import io
        import gzip
        import pyarrow as pa
        import pyarrow.csv as pacsv
        import pyarrow.parquet as pq
        import tempfile
        import pendulum as _pendulum
        from airflow.sdk import get_current_context, Variable, Connection
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook as _S3Hook
        import sqlalchemy as sa
        from sqlalchemy import text as _sa_text
        from sqlalchemy import create_engine
        import os
        import json
        import logging
        import boto3
        import polars as pl
        from airflow.hooks.base import BaseHook
        from airflow.providers.postgres.hooks.postgres import PostgresHook as _PostgresHook
        from botocore.config import Config as BotoConfig
        from airflow.exceptions import AirflowNotFoundException
        from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError


        # Get config data
        cfg = _runtime_cfg
        bucket = cfg.get("bucket")
        region = cfg.get("region")
        backup_folder = cfg.get("backup_folder")
        auth_mode = cfg.get("auth")
        pg_conn_id = cfg.get("pg_conn_id")
        aws_conn_id = cfg.get("aws_conn_id")
        # pg_uri_cfg    = cfg.get("pg_uri")

        # normalize to end with exactly one slash
        if not backup_folder.endswith("/"):
            backup_folder = backup_folder + "/"

        # Debug: print variables one per line (as requested)
        print(f"[backup] bucket: {bucket}")
        print(f"[backup] backup_folder: {backup_folder}")
        print(f"[backup] Using Posgres connection ID: {pg_conn_id}")

        # New hook and configuration to deal with DNS and internet issues
        boto_cfg = BotoConfig(
            retries={"max_attempts": 10, "mode": "standard"},
            connect_timeout=10,
            read_timeout=120,
            s3={
                "addressing_style": "path",
                "use_dualstack_endpoint": False,
                "use_accelerate_endpoint": False,
            },
        )

        # --- S3 client
        if auth_mode == "connection" and aws_conn_id:
            try:
                aws_conn = BaseHook.get_connection(aws_conn_id)
                extra = json.loads(aws_conn.extra or "{}")
                resolved_region = extra.get("region_name") or region
                client_kwargs = {"region_name": resolved_region, "config": boto_cfg}
                if aws_conn.login:
                    client_kwargs["aws_access_key_id"] = aws_conn.login
                if aws_conn.password:
                    client_kwargs["aws_secret_access_key"] = aws_conn.password
                if extra.get("aws_session_token"):
                    client_kwargs["aws_session_token"] = extra["aws_session_token"]
                s3_client = boto3.client("s3", **client_kwargs)
                region = resolved_region
                print(f"[backup] Using AWS connection '{aws_conn_id}' (region={region}).")
            except (AirflowNotFoundException, NoCredentialsError, PartialCredentialsError) as e:
                print(f"[backup] Fallback to default AWS credentials (IAM/env). Cause: {e}")
                s3_client = boto3.client("s3", region_name=region, config=boto_cfg)
        else:
            s3_client = boto3.client("s3", region_name=region, config=boto_cfg)

        # --- Postgres Engine resolution
        # 1) ENV VAR connection (safest; no code/DB secrets)
        # env_key = f"AIRFLOW_CONN_{pg_conn_id}".upper()
        # pg_uri_env = os.getenv(env_key)
        # if pg_uri_env:
        #     try:
        #         engine = create_engine(pg_uri_env)
        #         print(f"[backup] Using {env_key} from environment.")
        #     except Exception as e:
        #         print(f"[backup] ENV DSN failed: {e!r}")

        # 2) DSN passed via XCom (JSON); never log it
        # if engine is None and pg_uri_cfg:
        #     try:
        #         engine = create_engine(pg_uri_cfg)
        #         print("[backup] Using DSN from upstream runtime config.")
        #     except Exception as e:
        #         print(f"[backup] DSN from runtime cfg failed: {e!r}")

        # 3) Traditional hooks (may fail in venv; harmless if above worked)
        # if engine is None:
        #     try:
        #         conn = BaseHook.get_connection(pg_conn_id)
        #         engine = create_engine(conn.get_uri())
        #         print("[backup] Using BaseHook connection.")
        #     except Exception as e:
        #         print(f"[backup] BaseHook failed: {e!r}. Trying PostgresHook…")
        #         try:
        #             engine = _PostgresHook(postgres_conn_id=pg_conn_id).get_sqlalchemy_engine()
        #             print("[backup] Using PostgresHook.get_sqlalchemy_engine().")
        #         except Exception:
        #             try:
        #                 engine = _PostgresHook(postgres_conn_id=pg_conn_id).get_conn()
        #                 print("[backup] Using PostgresHook.get_conn().")
        #             except Exception as e2:
        #                 print(f"[backup] PostgresHook failed: {e2!r}")

        engine = None
        try:
            c = Connection.get(pg_conn_id)  # SDK connection (works in normal tasks)
            pg_uri = str(_SA_URL.create(
                "postgresql+psycopg2",
                username=c.login,
                password=c.password,        # URL.create will handle quoting safely
                host=c.host,
                port=int(c.port) if c.port else None,
                database=c.schema,
            ))
            print("[get_runtime_cfg] Got pg_uri from pg_conn_id.")
        except Exception as e:
            print(f"[get_runtime_cfg] Could not resolve '{pg_conn_id}': {e}")

        try:
            engine = create_engine(pg_uri)
            print("[backup] Using DSN from upstream runtime config.")
        except Exception as e:
            print(f"[backup] DSN from runtime cfg failed: {e!r}")


        if engine is None:
            raise RuntimeError("Could not obtain a Postgres connection/engine.")

        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        print("[backup] Postgres connectivity OK")

        # Announce backup source
        host = database = None
        try:
            host = getattr(engine.url, "host", None)
            database = getattr(engine.url, "database", None)
        except Exception as _e:
            print(f"[backup] Could not introspect Engine URL: {_e!r}")
        
        MAIN_SCHEMA = "public"
        MAIN_TABLE = "mkt_listings_main"

        LOGS_SCHEMA = "public"
        LOGS_TABLE = "mkt_listings_log"

        MAIN_FQN = f"{MAIN_SCHEMA}.{MAIN_TABLE}"
        LOGS_FQN = f"{LOGS_SCHEMA}.{LOGS_TABLE}"

        print(
            f"[backup] source: Postgres {host or '?'} / {database or '?'}"
        )

        print(f"[backup] client: {s3_client}")

        # Optional: preflight connectivity (clearer error, earlier)
        s3_client.head_bucket(Bucket=bucket)

        # S3 keys
        key_main = f"{backup_folder}{MAIN_TABLE}_{run_date_str}.parquet"
        key_logs = f"{backup_folder}{LOGS_TABLE}_{run_date_str}.parquet"

        # Helpers
        def _pg_arrow_schema(engine, schema: str, table: str) -> pa.Schema:
            with engine.connect() as c:
                rows = c.execute(_sa_text("""
                    SELECT column_name, data_type, numeric_precision, numeric_scale, udt_name
                    FROM information_schema.columns
                    WHERE table_schema = :schema AND table_name = :table
                    ORDER BY ordinal_position
                """), {"schema": schema, "table": table}).fetchall()

            fields = []
            for col, dt, prec, scale, udt in rows:
                dt  = (dt or "").lower()
                udt = (udt or "").lower()

                if dt in {"smallint", "integer", "bigint"}: at = pa.int64()
                elif dt == "real":                           at = pa.float32()
                elif dt == "double precision":               at = pa.float64()
                elif dt in {"numeric", "decimal"}:
                    p = int(prec) if prec is not None else 38
                    s = int(scale) if scale is not None else 6
                    at = pa.decimal128(p, s) if p <= 38 else pa.string()
                elif dt.startswith("timestamp"):
                    at = pa.timestamp("us", tz="UTC") if ("with time zone" in dt or udt=="timestamptz") else pa.timestamp("us")
                elif dt == "date":                           at = pa.date32()
                elif dt == "boolean":                        at = pa.bool_()
                else:                                        at = pa.string()   # text/varchar/json/uuid/arrays→string
                fields.append(pa.field(col, at))
            return pa.schema(fields)

        def _copy_to_gz_csv(conn, table_fqn: str, out_gz_path: str):
            #   TODO Optional clarity improvement
            # - Make _copy_to_gz_csv return the output path and pass that to _csv_gz_to_parquet.
            raw = conn.connection if hasattr(conn, "connection") else conn  # SA or psycopg2
            with raw.cursor() as cur, open(out_gz_path, "wb") as f:
                with gzip.GzipFile(fileobj=f, mode="wb") as gz:
                    cur.copy_expert(
                        f"COPY (SELECT * FROM {table_fqn}) TO STDOUT "
                        f"WITH (FORMAT CSV, HEADER TRUE, FORCE_QUOTE *, ENCODING 'UTF8')",
                        gz
                    )

        def _csv_gz_to_parquet(csv_gz_path: str, parquet_path: str, schema:pa.Schema) -> int:
            # stream-convert gzipped CSV -> Parquet (no pandas/polars)
            with open(csv_gz_path, "rb") as fh:
                gz_stream = pa.CompressedInputStream(pa.input_stream(fh), "gzip")
                reader = pacsv.open_csv(
                    gz_stream,
                    read_options=pacsv.ReadOptions(block_size=1 << 20),
                    parse_options=pacsv.ParseOptions(
                        delimiter=",",
                        quote_char='"',
                        double_quote=True,         # Postgres doubles quotes inside quoted fields
                        escape_char=None,          # default is fine for standard Postgres CSV
                        newlines_in_values=True,   # allow embedded newlines inside quoted fields
                    ),
                    convert_options=pacsv.ConvertOptions(
                        column_types={f.name: f.type for f in schema},
                        strings_can_be_null=True,
                        null_values=["", "NULL", "None"],
                        true_values=["t", "true", "1", "y", "yes", "on", "True", "TRUE"],
                        false_values=["f", "false", "0", "n", "no", "off", "False", "FALSE"],
                    ),
                )
                writer = pq.ParquetWriter(parquet_path , schema, compression="snappy", use_dictionary=True)
                total = 0
                try:
                    for batch in reader:
                        if batch is None or batch.num_rows == 0:
                            continue
                        batch = batch.cast(schema, safe=False)   # ← enforce exact types
                        writer.write_batch(batch)
                        total += batch.num_rows
                finally:
                    writer.close()
            return total

        def _upload_to_s3(local_path: str, bucket: str, key: str):
            s3_client.upload_file(
                local_path, bucket, key,
                ExtraArgs={"ServerSideEncryption": "AES256", "ContentType": "application/octet-stream"}
            )

        # snapshot + export
        with engine.begin() as conn:  # repeatable-read snapshot when supported
            try:
                # Guarantess to freeze the database in time
                conn.execute(sa.text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            except Exception as e:
                print(f"[SET TRANSACTION] ran an exception clause: {e}")
                pass

            with tempfile.TemporaryDirectory(prefix="pg_backup_") as tmp:

                # MAIN
                schema_main = _pg_arrow_schema(engine, MAIN_SCHEMA, MAIN_TABLE)
                csv_main = os.path.join(tmp, f"{MAIN_FQN.replace('.', '_')}.csv.gz")
                pq_main  = os.path.join(tmp, f"{MAIN_FQN.replace('.', '_')}.parquet")
                _copy_to_gz_csv(conn, MAIN_FQN, csv_main)
                rows_main = _csv_gz_to_parquet(csv_main, pq_main, schema_main)
                _upload_to_s3(pq_main, bucket, key_main)

                # LOGS
                schema_logs = _pg_arrow_schema(engine, LOGS_SCHEMA, LOGS_TABLE)
                csv_logs = os.path.join(tmp, f"{LOGS_FQN.replace('.', '_')}.csv.gz")
                pq_logs  = os.path.join(tmp, f"{LOGS_FQN.replace('.', '_')}.parquet")
                _copy_to_gz_csv(conn, f"{LOGS_FQN}", csv_logs)
                rows_logs = _csv_gz_to_parquet(csv_logs, pq_logs, schema_logs)
                _upload_to_s3(pq_logs, bucket, key_logs)

            results = {
                "bucket": bucket,
                "keys": {
                    "main": key_main,
                    "logs": key_logs,
                },
                "rows": {
                    "main": rows_main,
                    "logs": rows_logs,
                },
                "format": "parquet",
                "run_date_str": run_date_str,
            }

        print(
            f"[backup] Backed up to:"
            f"\n -> s3://{bucket}/{key_main}"
            f"\n -> s3://{bucket}/{key_logs}"
            f"\n rows: main={rows_main}, logs={rows_logs}"
            )

        return results

    @task()
    def clean_and_concatenate(std_paths: Dict[str, str]) -> str:
        """
        Read standardized per-source Parquets -> concat -> full cleaning pipeline
        -> write final Parquet and return its path.
        """
        # 1) Read lazily and concat
        lfs = [pl.scan_parquet(path) for path in std_paths.values() if path]
        if not lfs:
            print("No standardized inputs provided to clean_and_concatenate.")
            return ""
        lf = pl.concat(lfs, how="vertical")

        schema_names = set(lf.collect_schema().names())

        # content → description
        if "content" in schema_names:
            lf = lf.rename({"content": "description"})
            schema_names.discard("content")
            schema_names.add("description")

        # Homologate property type to the mapping defined
        if "property_type" in schema_names:
            lf = lf.with_columns(
                pl.coalesce(
                    [
                        pl.col("property_type").replace(
                            homologated_mapping
                        ),  # case-sensitive
                        pl.col("property_type"),
                    ]
                )
                .str.to_uppercase()
                .alias("property_type_homo")
            )
        else:
            lf = lf.with_columns(pl.lit(None, dtype=pl.Utf8).alias("property_type_homo"))

        # Ensure string columns used later exist
        ## If they don't exist, mark as None
        ## Then update the schema (missing columns are present know)
        need_utf8 = [
            "property_type",
            "description",
            "main_development_url",
            "title",
            "source_listing_url",
            "amenidades",
            "publisher_name",
        ]
        missing = [column for column in need_utf8 if column not in schema_names]
        if missing:
            lf = lf.with_columns([pl.lit(None, dtype=pl.Utf8).alias(column) for column in missing])
            schema_names.update(missing)

        # Clean text columns
        ## (?i) == case=False
        ## fill.null(False) == na=false
        preventa_re = r"(?i)\bpreventa(?:s)?\b|\bpre venta(?:s)?\b|\bpre-venta(?:s)?\b"

        lf = lf.with_columns(
            [
                _clean_text_expr("title").alias("title_clean"),
                _clean_text_expr("description").alias(
                    "description_clean"
                ),
                _clean_text_expr("property_type").alias("property_type_clean"),
                _clean_text_expr("source_listing_url").alias(
                    "source_listing_url_clean"
                ),
            ]
        )
        
        # Identify developments
        lf = lf.with_columns(
            (
                pl.col("property_type_clean")
                .cast(pl.Utf8)
                .str.contains(r"(?i)desarrollo", literal=False)
                .fill_null(False)
                | pl.col("description_clean")
                .cast(pl.Utf8)
                .str.contains(preventa_re, literal=False)
                .fill_null(False)
                | (pl.col("year_built").cast(pl.Float64, strict=False) >= 2025).fill_null(
                    False
                )
                | pl.col("main_development_url")
                .cast(pl.Utf8)
                .is_not_null()
                .fill_null(False)
            ).alias("is_development")
        )

        # Extract development id from main_development_url and creates "id_dev"
        lf = lf.with_columns(
            extract_development_id("main_development_url")
            .alias("id_dev")
        )

        # Assumes `property_type_homo` already exists (from your earlier mapping step)
        terreno_re = r"(?i)\bterreno(?:s)?\b|\blote(?:s)?\b|\bmacrolote(?:s)?\b"
        casa_re    = r"(?i)\bcasa(?:s)?\b"
        is_des     = (pl.col("property_type").cast(pl.Utf8) == "DESARROLLO")
        
        # Identifies types of desarrollo
        ## DESARROLLO & (title has terreno/lote/macrolote) -> TERRENO,  otherwise maintains property type
        lf = lf.with_columns(
            pl.when(
                is_des &
                pl.col("title").cast(pl.Utf8).str.contains(terreno_re, literal=False).fill_null(False)
            )
            .then(pl.lit("TERRENO"))
            .otherwise(pl.col("property_type_homo"))
            .alias("property_type_homo")
        )

        ## DESARROLLO & (title has casa) -> CASA, otherwise maintains property type
        lf = lf.with_columns(
            pl.when(
                is_des &
                pl.col("title").cast(pl.Utf8).str.contains(casa_re, literal=False).fill_null(False)
            )
            .then(pl.lit("CASA"))
            .otherwise(pl.col("property_type_homo"))
            .alias("property_type_homo")
        )

        ## DESARROLLO -> DEPARTAMENTO, otherwise maintains property type
        lf = lf.with_columns(
            pl.when(is_des)
            .then(pl.lit("DEPARTAMENTO"))
            .otherwise(pl.col("property_type_homo"))
            .alias("property_type_homo")
        )

        # Filter out "remates" but keep "preventa"
        str_filter_remates  = (
            r"REMATE|REMATO|PRECIO INMEJORABLE|ATENCION INVERSIONISTA|BAJO CONTRATO|BAJO COSTO|CONTADO|"
            r"COSTO COMERCIAL|COSTO LIQUIDACION|COSTO TOTAL|DE AHORRO|HIPOTECARIA|LIQUIDACION|NO CRED|NO SE ACEPTAN CR|"
            r"NOTARIADO|POR DEBAJO|PRECIO DEBAJO|PRIMER PAGO|RECUPERACION|REMAT|SOLO EFECTIVO|STATUS JURIDICO|TRAMITE|"
            r"UNICA OPORTUNIDAD|INIGUALABLE|OPORTUNIDAD DE INVERS|OPORTUNIDAD DE ADQUIRIR|VALOR AVALUO|VALOR COMERCIAL|"
            r"RECURSOS PROP|SENTENCIA|INVALIDO|JUICIO|ADJUDICA|DESCUENT|RENTA|MITAD DE PRECIO|MITAD-DE-PRECIO|REBAJ|"
            r"PRECIO-ABAJO-DEL|POR DEBAJO DE|POR-DEBAJO|VALOR-CATASTRAL|AL COSTO|AL-COSTO|BAJO DE SU COSTO|% DE SU VALOR COMERCIAL|"
            r"SOBRE SU VALOR COMERCIAL|NO-CRED"
        )
        str_filter_preventa = r"PREVENTA|PRE VENTA|PRE-VENTA|LANZAMIENTO"

        def is_remate(col: str) -> pl.Expr:
            # literal=False -> interpret pattern as regex
            """
            Returns boolean or null for the regex expression.
            Then it fills the nulls into False with fill_null.
            POlars use three-valued logic
            """
            text = pl.col(col).cast(pl.Utf8)
            has_remate = text.str.contains(str_filter_remates, literal=False).fill_null(False)
            has_preventa = text.str.contains(str_filter_preventa, literal=False).fill_null(False)
            is_remate = (
                pl.when(text.is_not_null())
                .then(has_remate & ~has_preventa)
                .otherwise(False)
                )
            return is_remate

        lf = lf.with_columns(
            (
                is_remate("description_clean") |
                is_remate("title_clean") |
                is_remate("source_listing_url_clean")
            ).cast(pl.Int8).alias("is_remate") # 1 or 0

        )

        # Exclude remates
        lf = lf.filter(pl.col("is_remate") == 0)


        # Exclude specific publishers
        if "publisher_name" in schema_names:
            lf = lf.filter(
                ~pl.col("publisher_name").is_in(["Dimensión Asesores, S.A. de C.V."])
            )

        # Defining amenidades
        ## Normalize to JSON text (or "{}"), then decode ONLY the canonical fields as booleans
        lf = lf.with_columns(
            pl.when(pl.col("amenidades").is_not_null())
            .then(pl.col("amenidades").map_elements(_normalize_amenidades_json, return_dtype=pl.Utf8))
            .otherwise(pl.lit("{}"))
            .alias("amenidades_normalized")
        )

        # Decode ONLY the 6 canonical keys to a Struct of Utf8 (so empty strings remain visible)
        amen_dtype = pl.Struct({
            "jacuzzi": pl.Utf8,
            "alberca": pl.Utf8,
            "gimnasio": pl.Utf8,
            "salonusosmultiples": pl.Utf8,
            "elevador": pl.Utf8,
            "asador": pl.Utf8,
        })

        lf = lf.with_columns(
            pl.col("amenidades_normalized").str.json_decode(dtype=amen_dtype).alias("amen_raw")
        )

        # 2) Helper: extract JSON key via JSONPath and coerce to boolean with your truthy semantics
        _truthy = {"1","true","t","yes","y","si","sí"}
        _falsey = {"0","false","f","no","n"}

        def amen_bool(k: str) -> pl.Expr:
            raw = pl.col("amen_raw").struct.field(k)
            s = (raw.cast(pl.Utf8, strict=False)
                    .str.strip_chars()
                    .str.strip_chars('"')
                    .str.to_lowercase())
            # num = s.cast(pl.Float64, strict=True) # strict=True raises error
            # Extracts integer/decimal (with . or ,), normalizes to ".", and converts to float
            num = (
                s.str.extract(r"(-?\d+(?:[.,]\d+)?)")
                .str.replace(",", ".")
                .cast(pl.Float64)
            )

            return (
                pl.when(raw.is_null()).then(None)                 # missing key
                .when(num.is_not_null()).then(num > 0)            # Verify position: here or below? "4"/"4.0"/"-1" -> True, "0" -> False
                .when(s == "").then(None)                         # empty string -> NULL
                .when(s.is_in(list(_truthy))).then(pl.lit(True))  # textual truthy
                .when(s.is_in(list(_falsey))).then(pl.lit(False)) # textual falsey
                .otherwise(None)
            ).alias(k)

        # 3) Expand desired keys to columns
        lf = lf.with_columns([
            amen_bool("jacuzzi"),
            amen_bool("alberca"),
            amen_bool("gimnasio"),
            amen_bool("salonusosmultiples"),
            amen_bool("elevador"),
            amen_bool("asador"),
        ])

        # 3) Elevador rule (pandas): any non-null value not exactly False -> True; else keep (False/null)
        elevador = pl.col("elevador")
        lf = lf.with_columns(
            pl.when(elevador.is_not_null() & (elevador != pl.lit(False)))
            .then(pl.lit(True))
            .otherwise(elevador)
            .alias("elevador")
        )

        # 12) Final cleaning / renames
        lf = lf.filter(
            ~pl.col("property_type_homo").is_in(["QUINTA", "RANCHO", "RANCHO COMERCIAL"])
        )
        rename_map = {}
        if "id" in schema_names:
            rename_map["id"] = "id_ext"
        if "property_type" in schema_names:
            rename_map["property_type"] = "property_type_raw"
        rename_map["property_type_homo"] = "property_type"

        lf = lf.rename(rename_map)

        # keep cache in sync
        for old, new in rename_map.items():
            schema_names.discard(old)
            schema_names.add(new)

        # 13) Numeric cleaning for areas
        for metr_col in ["m2_terrain", "m2_built"]:
            if metr_col not in schema_names:
                lf = lf.with_columns(pl.lit(None, dtype=pl.Utf8).alias(metr_col))
                schema_names.add(metr_col)
        num_pat = r"(-?\d+(?:\.\d+)?)"
        lf = lf.with_columns(
            [
                pl.col("m2_terrain")
                .cast(pl.Utf8)
                .str.replace(",", "")
                .str.extract(num_pat)
                .cast(pl.Float64, strict=False)
                .alias("m2_terrain"),
                pl.col("m2_built")
                .cast(pl.Utf8)
                .str.replace(",", "")
                .str.extract(num_pat)
                .cast(pl.Float64, strict=False)
                .alias("m2_built"),
            ]
        )

        # 14) Adjust terrain for DEPARTAMENTO
        lf = lf.with_columns(
            pl.when(
                (pl.col("m2_terrain") < pl.col("m2_built"))
                | (
                    pl.col("m2_terrain").is_null()
                    & (pl.col("property_type") == "DEPARTAMENTO")
                )
            )
            .then(pl.col("m2_built"))
            .otherwise(pl.col("m2_terrain"))
            .alias("m2_terrain")
        )

        # 15) Sequential id starting at 1
        lf = lf.with_row_index(name="id", offset=1)

        # 16) Final column ordering (append debug columns if present)
        current_cols = set(lf.collect_schema().names())
        final_cols = [c for c in ordered_columns if c in current_cols]

        # Extra cols for debugging amenidades
        # for extra in ("amenidades_normalized", "amen_raw"):
        #     if extra in current_cols and extra not in final_cols:
        #         final_cols.append(extra)

        if final_cols:
            lf = lf.select(final_cols)

        # 13) Materialize + write
        df = lf.collect(engine="streaming")
        out_path = tempfile.NamedTemporaryFile(prefix="listings_clean_", suffix=".parquet", delete=False).name
        df.write_parquet(out_path)

        print(f"[clean_and_concatenate] rows={df.height:,}, cols={len(df.columns)}")
        print(f"[clean_and_concatenate] wrote: {out_path}")

        return out_path

    @task()
    def generate_historical_data(_runtime_cfg: dict, backup_result: dict, cleaned_listing_path: str, folders: dict) -> str:
        """
        Merge prior backup data with the latest cleaned listings to build and export
        a historical snapshot, track changes, detect duplicates, and upload the result to S3.

        Parameters
        ----------
        backup_result : dict
            Output from `backup()`, must include:
            - `bucket` (str): S3 bucket with the previous snapshot.
            - `keys.main` (str): S3 key to the main parquet.
        cleaned_listing_path : str
            Local path to the cleaned listings parquet.
        folders : dict
            Must contain `latest_data_folder` (wave identifier, e.g. 'YYYY-MM-DD').

        Steps
        -----
        1. Load new and old data (from local parquet and S3).
        2. Normalize wave identifier (`new_wave`).
        3. Classify listings as published, unpublished, or kept; update `id`, wave markers, and `is_active`.
        4. For tracked fields, update values and set `*_last_updated_wave` when changes occur.
        5. Cast numeric fields, normalize list-like columns, and detect duplicates.
        6. Export merged parquet to `s3://{S3_BUCKET}/{S3_EXPORT_FOLDER}historical_{new_wave}.parquet`.

        Returns
        -------
        str
        S3 URL of the exported historical parquet.
        """

        # Get config data
        cfg = _runtime_cfg
        bucket = cfg.get("bucket")
        export_folder = cfg.get("export_folder")
        region = cfg.get("region")
        auth_mode = cfg.get("auth")

        # Guarantee the export folder ends with /
        if not export_folder.endswith("/"):
            export_folder += "/"



        # Loaded cleaned dataframe
        if not cleaned_listing_path:
            print("[new_data] cleaned_listing_path is empty; skip loading cleaned data")
        else:
            try:
                new_data = pl.scan_parquet(cleaned_listing_path).collect(streaming=True)
                print(
                    f"[new_data] cleaned data loaded: rows={new_data.height:,} "
                    f"cols={len(new_data.columns)} from {cleaned_listing_path}"
                )
            except Exception as e:
                raise RuntimeError(f"[new_data] failed to load cleaned data: {e}") from e
        
        # Load Backuped dataframe
        print(f"[backup_result] Backup result is {backup_result}")
        if not backup_result or not isinstance(backup_result, dict):
            raise ValueError("[backup_result] generate_historical_data requires backup_result from backup()")

        bucket = backup_result.get("bucket")
        keys = backup_result.get("keys") or {}
        key_main = keys.get("main")
        if not bucket or not key_main:
            raise ValueError("backup_result missing 'bucket' or 'keys.main'")

        # Get S3 client
        client = None
        if auth_mode == "connection":
            try:
                conn = Connection.get("AWS_CONN_ID")
                client = boto3.client(
                    "s3",
                    aws_access_key_id=conn.login,
                    aws_secret_access_key=conn.password,
                    region_name=region,
                )
            except (AirflowNotFoundException, NoCredentialsError, PartialCredentialsError) as e:
                print(f"[list_and_read_all] Fallback to default AWS credentials (IAM/env). Cause: {e}")
                client = boto3.client("s3", region_name=region)
        else:
            client = boto3.client("s3", region_name=region)

        with tempfile.NamedTemporaryFile(prefix="hist_main_", suffix=".parquet", delete=False) as tmp:
            local_path = tmp.name

        client.download_file(bucket, key_main, local_path)

        try:
            lf_backuped_main_data = pl.scan_parquet(local_path)
            old_data = lf_backuped_main_data.collect(streaming=True)

            with tempfile.NamedTemporaryFile(prefix="hist_main_full_", suffix=".parquet", delete=False) as out:
                out_path = out.name
            old_data.write_parquet(out_path)

            print(
                f"[backuped_main_data] loaded full backup: rows={old_data.height:,}, cols={len(old_data.columns)} "
                f"from s3://{bucket}/{key_main} -> {out_path}"
            )
        except Exception as e:
            print(f"[backuped_main_data] selection failed; returning raw parquet: {e}")
            out_path = local_path  # return the raw download if selection failed

        print("[dtype check] new_data id_ext:", new_data.schema.get("id_ext"))
        print("[dtype check] old_data id_ext:", old_data.schema.get("id_ext"))
        
        # Pure Polars: derive unique ids and compute intersections/differences via joins
        new_ids_df = new_data.select("id_ext").drop_nulls().unique()
        old_ids_df = old_data.select("id_ext").drop_nulls().unique()

        print(f" -> Unique new ids: {new_ids_df.height}")
        print(f" -> Unique old ids: {old_ids_df.height}")

        keep_published = new_data.join(old_ids_df, on="id_ext", how="semi")  # in both
        published = new_data.join(old_ids_df, on="id_ext", how="anti")        # new only
        unpublished = old_data.join(new_ids_df, on="id_ext", how="anti")      # old only


        # 1) Prove the overlap is real
        ## sanity check
        print("[debug] sample ids in both (keep_published):",
            keep_published.select("id_ext").unique().head(10).to_series().to_list())

        print("[debug] sample ids NEW ONLY (published):",
            published.select("id_ext").unique().head(10).to_series().to_list())


        # Compare ids
        n_new = new_data.height # equivalent to len() function, but returns int
        n_old = old_data.height

        pct_new = (published.height / n_new) if n_new else 0.0
        pct_old = (unpublished.height / n_old) if n_old else 0.0

        print(f" -> Total properties in new data: {n_new}")
        print(f" -> Total properties in old data: {n_old}")
        print(f" -> Common properties: {keep_published.height}")
        print(f" -> New properties: {published.height} ({pct_new:.1%} of new data)")
        print(f" -> Removed properties: {unpublished.height} ({pct_old:.1%} of old data)")

        # Normalize wave identifier: wave_str (raw) -> new_wave (final)
        wave_str = folders.get("latest_data_folder") or ""
        print(f"Wave string is {wave_str}")

        try:
            new_wave_raw = wave_str or ""
            wave_norm = new_wave_raw.strip("/")
            last_seg = wave_norm.split("/")[-1] if wave_norm else wave_norm
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", last_seg or ""):
                new_wave = last_seg
            else:
                new_wave = (last_seg or wave_norm).replace("/", "_")
            print(f"[wave] normalized: input='{new_wave_raw}' -> '{new_wave}'")
        except Exception as _e:
            new_wave = str(wave_str)
            print(f"[wave] fallback; using raw new_wave='{new_wave}' ({_e})")
        
        # Create export key for new wave
        export_key = f"{export_folder}historical_{new_wave}.parquet"

        # Make the new published dataframe
        ## Add wave metadata to published using provided new_wave
        published = published.with_columns([
            pl.lit(new_wave).alias('published_wave'),
            pl.lit(None, dtype=pl.Utf8).alias('unpublished_wave'),
            pl.lit(True).alias('is_active'),
        ])

        ## Get last id from backuped_main
        last_id_opt = old_data.select(pl.col("id").cast(pl.Int64).max()).item()
        last_id = 0 if last_id_opt is None else int(last_id_opt)
        
        ## Set published ids bases on the last id of backuped main
        published = published.with_columns(
            (pl.int_range(0, pl.len()) + pl.lit(last_id + 1))
            .cast(pl.Int64)
            .alias("id")
        )

        ## Create historical data
        # Build historical dataframe by vertically concatenating old_data and newly published
        # Equivalent to: pd.concat([old_data, published], ignore_index=True)
        # In Polars, there is no index to reset; rows are inherently positional.
        historical_data = pl.concat([old_data, published], how="diagonal_relaxed")

        # extract ids for updating historical data
        unpublished_ids = unpublished.select("id_ext").drop_nulls().unique().get_column("id_ext")
        unpublish_wave = new_wave

        # ## Update unpublished wave and is_active
        historical_data = historical_data.with_columns([
            pl.when(
                (pl.col("id_ext").is_in(unpublished_ids)) & (pl.col("is_active"))
            ).then(pl.lit(unpublish_wave, dtype=pl.Utf8))
            .otherwise(pl.col("unpublished_wave"))
            .alias("unpublished_wave"),

            pl.when(
                (pl.col("id_ext").is_in(unpublished_ids)) & (pl.col("is_active"))
            ).then(False)
            .otherwise(pl.col("is_active"))
            .alias("is_active")
        ])

        ## Add tracked columns if they are not present
        for field in tracked_fields:
            if f"{field}_last_updated_wave" not in historical_data.columns:
                historical_data = historical_data.with_columns(
                    pl.lit(None, dtype=pl.Utf8).alias(f"{field}_last_updated_wave")
                )

        ## Transform numeric columns
        numeric_fields = ['asking_price', 'm2_built', 'no_bedrooms', 'no_full_bathrooms', 'lat', 'lon']
        for col in numeric_fields:
            historical_data = historical_data.with_columns(
                pl.col(col).cast(pl.Float64, strict=False).alias(col)
            )
            new_data = new_data.with_columns(
                pl.col(col).cast(pl.Float64, strict=False).alias(col)
            )

        # Controlled merge
        # Align -> Compare -> Find diff -> Drop NAs -> collect Ids -> update -> add wave marker 
        # Drop rows with null identifiers before aligning/comparing
        # new_data = new_data.drop_nulls(subset=["id_ext"])  # ensure key presence
        # historical_data = historical_data.drop_nulls(subset=["id_ext"])  # ensure key presence
        # published = published.drop_nulls(subset=["id_ext"])  # keep only valid published keys
        # IDs for listings that exist in both old and new dataframes
        keep_published_ids = (
            keep_published
            .select("id_ext")
            .drop_nulls()
            .unique()
            .get_column("id_ext")
        )

        for field in tracked_fields:
            new_vals = (
                new_data
                .filter(pl.col("id_ext").is_in(keep_published_ids))
                .select(["id_ext", field])
                .rename({field: "new_val"})
            )

            old_vals = (
                historical_data
                .filter(pl.col("id_ext").is_in(keep_published_ids))
                .select(["id_ext", field])
                .rename({field: "old_val"})
            )

            ## Join to compare old vs new
            comparison = old_vals.join(new_vals, on="id_ext", how="left")

            # Rows where values differ (note: null-safe inequality as in pandas' !=—this excludes equal values)
            changed = comparison.filter(
                (pl.col("old_val") != pl.col("new_val")) |
                (pl.col("old_val").is_null() & pl.col("new_val").is_not_null()) |
                (pl.col("old_val").is_not_null() & pl.col("new_val").is_null())
                )

            ## Only keep rows where the new value is not null (mimics ~new_vals.loc[changed.index].isna())
            valid_changes = changed.filter(pl.col("new_val").is_not_null())

            ## List of ids to actually update
            changed_ids = valid_changes.select("id_ext").to_series().to_list()

            ## Update field values
            historical_data = (
                historical_data
                .join(valid_changes.select(["id_ext", "new_val"]), on="id_ext", how="left")
                .with_columns(
                    pl.when(pl.col("id_ext").is_in(changed_ids))
                        .then(pl.col("new_val"))
                        .otherwise(pl.col(field))
                        .alias(field)
                )
                .drop("new_val")
            )

            ## Updated wave marker
            historical_data = historical_data.with_columns(
                pl.when(pl.col("id_ext").is_in(changed_ids))
                    .then(pl.lit(new_wave, dtype=pl.Utf8))
                    .otherwise(pl.col(f"{field}_last_updated_wave"))
                    .alias(f"{field}_last_updated_wave")
            )

        # Identify duplicates BEFORE writing (keep same historical_data; no mutation)
        try:
            # 1) Compute the Polars streaming result (input can be pandas or polars)
            historical_data = find_potential_duplicates(
                dataframe =  historical_data,
                radius_km=0.2,       
                # maximum effective batch size is 738,312 for this data (query centers)
                # consumes up to 5GB in RAM      
                batch_size=100_000,
            )

        except Exception as e:
            print(f"[identify_duplicates] failed to compute duplicates: {e}")

        # Pretty breakdown of unpublished_wave counts (Polars-based, pandas-free)
        try:
            counts_df = (
                historical_data
                .select(pl.col("unpublished_wave").cast(pl.Utf8))
                .with_columns(
                    pl.col("unpublished_wave")
                    .fill_null("(no unpublish)")
                    .alias("unpublished_wave")
                )
                .group_by("unpublished_wave")
                .agg(pl.len().alias("count"))
                .sort("count", descending=True)
                .with_columns(
                    (pl.col("count") / pl.lit(historical_data.height) * 100)
                    .round(2)
                    .alias("pct")
                )
            )

            print("[Stats for unpublished_wave]")
            for row in counts_df.to_dicts():
                label = row.get("unpublished_wave", "<unknown>")
                cnt = row.get("count", 0)
                pct = row.get("pct", 0.0)
                print(f" - {label:>12}: {cnt:,} ({pct:.2f}%)")
        except Exception as e:
            print(f"[Stats for unpublished_wave] failed to compute: {e}")

        print(
            f"[historical_data] rows={historical_data.height:,}, cols={len(historical_data.columns)}"
        )

        numeric_columns = [
            'lat', 'lon', 'no_bedrooms', 'no_full_bathrooms', 'no_half_bathrooms',
            'no_parking_spaces', 'year_built', 'asking_price', 'maintenance_fee',
            'm2_built', 'm2_terrain', 'm2_balconies', 'm2_terraces', 'm2_rooftop'
        ]

        # Cast the selected columns to numeric (Float64 is the safe default)
        historical_data = historical_data.with_columns([
            pl.col(col).cast(pl.Float64, strict=False) for col in numeric_columns
        ])

        # Transform to pandas to make the data types familiar to postgresql
        ## next task will read S3 and copy it to postgres database
        df_historical_pandas = historical_data.to_pandas()

        df_historical_pandas["amenidades"] = df_historical_pandas["amenidades"].apply(convert_to_python_native)
        df_historical_pandas["potential_duplicates"] = df_historical_pandas["potential_duplicates"].apply(convert_to_python_native)

        df_historical_pandas["amenidades"] = df_historical_pandas["amenidades"].apply(lambda x: convert_for_postgres(x, as_string=True))
        df_historical_pandas["potential_duplicates"] = df_historical_pandas["potential_duplicates"].apply(lambda x: convert_for_postgres(x, as_string=True))

        # Stream Parquet bytes to S3 (no temp file, no local path)
        # Change to a temporary file and multipart upload if memory constrains are high
        # Estimated filesize is  in disk is 759MB
        # Tried to avoid I/O operation using BytesIO
        # but upload broke due to the lost of TLS connection // Its my bad internet or BytesIO?
        # Using pandas estimated memory consume can be 2-5x bigger (~1.5-3.5GB RM when working)

        # 1) Write parquet to a temp file (no big RAM spike)
        with tempfile.NamedTemporaryFile(prefix="hist_upload_", suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name

        df_historical_pandas.to_parquet(tmp_path, index=False, engine="pyarrow", compression="snappy")
        
        # 2) Conservative multipart settings to avoid TLS drops
        tconfig = TransferConfig(
            multipart_threshold=16 * 1024 * 1024,  # 16 MB
            multipart_chunksize=16 * 1024 * 1024,  # 16 MB parts
            max_concurrency=4,                     # fewer parallel sockets
            use_threads=True,
        )
        try:
            client.upload_file(tmp_path, bucket, export_key, Config=tconfig)
            s3_url = f"s3://{bucket}/{export_key}"
            print(f"[historical_data] exported to {s3_url}")
            return s3_url
        except Exception as e:
                # Fail the task if we couldn't put the file in S3
                raise RuntimeError(f"[historical_data] S3 export failed: {e}") from e
        finally:
            try:
                os.remove(tmp_path)
            except OSError as e:
                print(f"OSError was {e}")
                pass


    @task()
    def generate_logs_table(
        _runtime_cfg: dict,
        backup_result: dict,
        cleaned_listing_path: str,
        folders: dict,
        _historical_data_done: str | None = None,
    ) -> str:
        """
        Build a change log table comparing logs backuped date (from backup) vs new data (cleaned parquet),
        for the tracked_fields and the intersection of id_ext values. Exports a Parquet to S3
        and returns its s3:// URL.

        Mirrors the data-loading approach in generate_historical_data to ensure consistency.
        """

        # Helpers
        def _normalize_wave(prefix:str) -> str:
            raw_wave_str = (prefix or "")
            try:
                wave_norm = raw_wave_str.strip("/")
                last_segment = wave_norm.split("/")[-1] if wave_norm else ""
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", last_segment or ""):
                    wave = last_segment
                else:
                    print(f"[_normalize_wave_match] fallback of '{prefix}': last_segment {last_segment} doesn't have the correct format for a full match. Please, verify data folder naming.")
                    wave = (last_segment or wave_norm).replace("/", "_")
                print(f"[_normalize_wave_match] normalized: prefix='{prefix}', input='{raw_wave_str}' -> '{wave}'")
            except Exception as _e:
                wave = str(raw_wave_str)
                print(f"[_normalize_wave_match] fallback of '{prefix}'; using raw wave str ='{wave}' due to the following exception: ({_e})")

            return wave

        def _read_s3_parquet(bucket: str, key:str, prefix:str) -> pl.DataFrame:
            """
            Download a parquet from S3 to a temp file, select existing columns, and return a DataFrame.
            - Logs any missing columns from `wanted_cols`.
            - Cleans up the temporary file in all cases.
            """
            with tempfile.NamedTemporaryFile(prefix=prefix, suffix=".parquet", delete=False) as tmp:
                local_path = tmp.name
            try:
                client.download_file(bucket, key, local_path)
                lazyframe = pl.scan_parquet(local_path)
                schema = lazyframe.collect_schema().names()
                present = [column for column in wanted_cols if column in schema]
                missing = [column for column in wanted_cols if column not in schema]
                if missing:
                    print(f"[backuped_logs_data] missing columns (info only): {missing}")
                materialized_dataframe = lazyframe.select(present).collect(streaming=True)

                print(f"[_read_s3_parquet] For {prefix} it was selected {len(present)} cols, rows={materialized_dataframe.height:,}")

                for column in present:
                    print(f" -> Loaded column: {column}")

                return materialized_dataframe

            except Exception as e:
                raise RuntimeError(f"[_read_s3_parquet] selection failed as Exception: {e}") from e

            finally:
                try:
                    os.remove(local_path)
                except OSError as e:
                    print(f"[_read_s3_parquet] Could not remove temp parquet '{local_path}': {e}")

        def _norm_numeric(df: pl.DataFrame) -> pl.DataFrame:
            return df.with_columns([
                pl.col(c).cast(pl.Float64, strict=False).fill_nan(None).alias(c)
                for c in numeric_tracked
                if c in df.columns
            ])

        def _round_cols(df: pl.DataFrame) -> pl.DataFrame:
            return df.with_columns([
                pl.col(c).cast(pl.Float64, strict=False).round(6).alias(c)
                for c in numeric_tracked
                if c in df.columns
            ])
        
        # Get config data
        cfg = _runtime_cfg
        region = cfg.get("region")
        auth_mode = cfg.get("auth")
        bucket = cfg.get("bucket")
        export_folder = cfg.get("export_folder")

        if not export_folder.endswith("/"):
            export_folder += "/"

        # Validate inputs
        print(f"Cleaned listing path is {cleaned_listing_path}")
        if not cleaned_listing_path:
            raise ValueError("Requires cleaned_listing_path from clean_and_concatenate()")

        print(f"Backup result is {backup_result}")
        if not backup_result or not isinstance(backup_result, dict):
            raise ValueError("Requires backup_result from backup()")

        bucket = backup_result.get("bucket")
        keys = backup_result.get("keys") or {}
        key_logs = keys.get("logs")
        if not bucket or not key_logs:
            raise ValueError("[generate_logs_table] backup_result missing 'bucket' or 'keys.logs'")

        new_wave = _normalize_wave(folders.get("latest_data_folder"))
        old_wave = _normalize_wave(folders.get("lagged_data_folder"))
        
        # Get S3 client
        client = None
        if auth_mode == "connection":
            try:
                conn = Connection.get("AWS_CONN_ID")
                client = boto3.client(
                    "s3",
                    aws_access_key_id=conn.login,
                    aws_secret_access_key=conn.password,
                    region_name=region,
                )
            except (AirflowNotFoundException, NoCredentialsError, PartialCredentialsError) as e:
                print(f"[list_and_read_all] Fallback to default AWS credentials (IAM/env). Cause: {e}")
                client = boto3.client("s3", region_name=region)
        else:
            client = boto3.client("s3", region_name=region)


        # Define variables for tracking dataframes variables
        wanted_cols = ["id_ext", "source_type"] + tracked_fields + ["wave_date"]

        numeric_tracked = [
            "asking_price",
            "m2_built",
            "m2_terrain",
            "lat",
            "lon",
            "no_bedrooms",
            "no_full_bathrooms",
            "no_half_bathrooms",
            "no_parking_spaces",
            "year_built"
        ]

        fields_for_audit = [
            "asking_price",
            "currency",
            "m2_built",
            "m2_terrain",
            "lat",
            "lon",
            "no_bedrooms",
            "no_full_bathrooms",
            "no_half_bathrooms",
            "no_parking_spaces",
            "year_built"
        ]

        # Load data
        ## new data
        try:
            new_data = pl.scan_parquet(cleaned_listing_path).collect(streaming=True)
            print(
                f"[new_data] cleaned data loaded: rows={new_data.height:,}"
                f"cols={len(new_data.columns)} from {cleaned_listing_path}"
            )
        except Exception as e:
            raise RuntimeError(f"[new_data] failed to load cleaned data: {e}") from e


        ## old data
        df_old_full = _read_s3_parquet(bucket, key_logs, "mkt_listings_logs")

        # Select only the last wave to compare
        ## Cast to polars date to ensure right comparison
        if "wave_date" not in df_old_full.columns:
            raise ValueError("[backuped_logs_data] 'wave_date' is required but not present in backup logs.")

        df_old_full = df_old_full.with_columns(
            pl.col("wave_date").cast(pl.Utf8).str.strptime(pl.Date, strict=False)
        )

        ## Keep only rows at or before old_wave
        _old_cutoff = dt.fromisoformat(old_wave)
        old_filtered = (
            df_old_full
            # Keep dates that are equal or more old than the cutoff
            .filter(pl.col("wave_date") <= pl.lit(_old_cutoff, dtype=pl.Date))
        )

        snapshot_old = (
            old_filtered
            # id_ext = ascending
            # wave_date = descending
            # For each id_ext, its most recent row appears first
            # Push null values to the end
            .sort(by=["id_ext", "wave_date"], descending=[False, True], nulls_last=True)
            # keep the first row, that is the most recent because we sort it was descending
            .unique(subset=["id_ext"], keep="first")
            .drop("wave_date")
        )

        # Debug
        ## Print schema to diagnose dtype mismatches
        ## Print rows to see changs between waves 
        print(
            f"[logs] full_backup (all log waves)\n -> rows={df_old_full.height}, unique ids={df_old_full.select(pl.col('id_ext').n_unique()).item()}"
        )
        print(
            f"[logs] snapshot_old (last comparable wave)\n -> rows={snapshot_old.height}, unique ids={snapshot_old.select(pl.col('id_ext').n_unique()).item()}"
        )
        print(f"[logs] snapshot_old schema: {snapshot_old.schema}")

        # Establish comparable id sets
        ## 1) ids that are still published between waves
        old_ids_df = snapshot_old.select("id_ext").drop_nulls().unique()
        keep_published = new_data.join(old_ids_df, on="id_ext", how="semi")  # in both
        keep_published_ids = (
            keep_published
            .select("id_ext")
            .drop_nulls()
            .unique()
            .get_column("id_ext")
        )

        
        ## 2) ids that are only in the new wave
        published = new_data.join(old_ids_df, on="id_ext", how="anti")        # new only ids
        published_ids = (
            published
            .select("id_ext")
            .drop_nulls()
            .unique()
            .get_column("id_ext")
        )

        ## 3)
        ### ids that are in the new and old snapshot (continuing listings)
        ### ids that are in the new snapshpot (newly published listings)
        new_ids = (
            pl.concat([keep_published_ids, published_ids])
                .unique(maintain_order=True)
        )

        # Build aligned snapshots for comparison
        ## SNAPSHOT NEW
        snapshot_new = (
            new_data.filter(pl.col("id_ext").is_in(new_ids.implode()))
            .select(["id_ext", "source_type"] + tracked_fields)
        )

        ## Create wave_date as new snapshot is only the cleaning new data without wave
        snapshot_new = snapshot_new.with_columns(pl.lit(new_wave).alias("wave_date"))
        
        # Normalize and round numeric values to guarantee a confident comparison
        snapshot_old = _norm_numeric(snapshot_old)
        snapshot_old = _round_cols(snapshot_old)

        snapshot_new = _norm_numeric(snapshot_new)
        snapshot_new = _round_cols(snapshot_new)

        # Debug: print schema to diagnose dtype mismatches
        print(f"[logs] snapshot_new schema: {snapshot_new.schema}")

        # Debug: verify wave settings
        print("[wave] settings:")
        print(f"   new_wave = '{new_wave}'")
        print(f"   old_wave = '{old_wave}'")

        print(snapshot_old.select([pl.all_horizontal([pl.col(c).is_nan() for c in numeric_tracked]).any()]).to_dicts())
        print(snapshot_new.select([pl.all_horizontal([pl.col(c).is_nan() for c in numeric_tracked]).any()]).to_dicts())

        # --- 1) Find changed ids (null-safe, join on id only) ---
        selected_old_to_compare = (
            snapshot_old
            .select(["id_ext"] + tracked_fields)
            .rename({c: f"{c}__old" for c in tracked_fields})
        )

        selected_new_to_compare = (
            snapshot_new
            .select(["id_ext"] + tracked_fields + ["source_type"])  # keep source_type for output
            .rename({c: f"{c}__new" for c in tracked_fields} | {"source_type": "source_type__new"})
        )

        # keep properties present in both
        properties_to_compare = selected_old_to_compare.join(selected_new_to_compare, on="id_ext", how="inner")

        tracking_fields_bool = [
            (
                # XOR =  exclusive OR
                # True if exactly one of the values is null.
                (pl.col(f"{field}__old").is_null() ^ pl.col(f"{field}__new").is_null()) |
                
                # True if values both values are not null and have changed
                ((pl.col(f"{field}__old") != pl.col(f"{field}__new")) &
                pl.col(f"{field}__old").is_not_null() & pl.col(f"{field}__new").is_not_null())
            )
            for field in tracked_fields
        ]

        diff_any = pl.any_horizontal(tracking_fields_bool) if tracking_fields_bool else pl.lit(False)

        changed_ids = (
            properties_to_compare
            .filter(diff_any)
            .select("id_ext")
            .unique()
            .get_column("id_ext")
        )

        # Define columns to shape log rows
        # * Observe prints when debugging
        base_present_new = [c for c in wanted_cols if c in snapshot_new.columns]
        print(f"base_present_new = {base_present_new}\n")


        expected_cols_final = base_present_new + ["change_type"]
        print(f"expected_cols_final = {expected_cols_final}\n")

        string_columns = {"id_ext", "source_type", "currency", "wave_date", "change_type"}
        print(f"string_columns = {string_columns}\n")
        
        # Create placeholders
        def _empty_logs_df():
            schema = {}
            for col in expected_cols_final:
                if col in string_columns:
                    schema[col] = pl.Utf8
                elif col in numeric_tracked:
                    schema[col] = pl.Float64
                else:
                    # Default to Utf8 for unknown non-numeric tracked fields
                    schema[col] = pl.Utf8
            return pl.DataFrame(schema=schema)

        logs_rows_changed = _empty_logs_df()
        if changed_ids.len() > 0:
            logs_rows_changed = (
                snapshot_new
                .filter(pl.col("id_ext").is_in(changed_ids.implode()))
                .select(base_present_new)
                .with_columns(pl.lit("changed").alias("change_type"))
            )

        logs_rows_new = _empty_logs_df()
        if published_ids.len() > 0: # new only ids
            logs_rows_new = (
                snapshot_new
                .filter(pl.col("id_ext").is_in(published_ids.implode()))
                .select(base_present_new)
                .with_columns(pl.lit("new").alias("change_type"))
            )

        # Align schemas and concat
        for column in expected_cols_final:
            if column not in logs_rows_changed.columns:
                logs_rows_changed = logs_rows_changed.with_columns(pl.lit(None).alias(column))
            if column not in logs_rows_new.columns:
                logs_rows_new = logs_rows_new.with_columns(pl.lit(None).alias(column))

        logs_rows_changed = logs_rows_changed.select(expected_cols_final)
        logs_rows_new = logs_rows_new.select(expected_cols_final)

        snapshot_all = pl.concat([logs_rows_changed, logs_rows_new], how="vertical")

        # Audits
        print(
            f"[logs] published_ids (new_only_ids): {published_ids.len()}, changed_ids: {changed_ids.len()}, total_rows: {snapshot_all.height}"
        )

        fields_for_audit = [field for field in fields_for_audit if field in snapshot_all.columns]
        print(f" -> Auditing fields: {fields_for_audit}")
        
        # Compare duplicated ids inside snapshot_all for safety
        # * in theory there would never be because it is only new and changed rows
        if fields_for_audit:
            dup_ids = (
                snapshot_all.group_by("id_ext")
                .agg(
                    [pl.col("wave_date").n_unique().alias("unique_waves")]
                    + [pl.col(field).n_unique().alias(f"unique_{field}") for field in fields_for_audit]
                )
                .filter(pl.col("unique_waves") > 1) # more than one wave
                # filter unique values between waves of same id
                .filter(pl.all_horizontal(
                    [pl.col(f"unique_{field}") == 1 for field in fields_for_audit]))
                .select("id_ext")
            )
            
            if dup_ids.height != 0:
                print("[audit] ids duplicated across waves but identical fields:", dup_ids.height)
        
                # 1) How many ids are in both waves?
        print("[logs] keep_published_ids:", keep_published_ids.len())

        # 2) How many ids your null-safe diff flagged?
        print("[logs] changed_ids:", changed_ids.len())

        # double check
        n_rows = snapshot_all.height
        n_ids = snapshot_all.select(pl.col("id_ext")).n_unique()
        dup_rows = n_rows - n_ids
        print(f"[logs] rows={n_rows:,}, unique_ids={n_ids:,}, duplicate_rows={dup_rows:,}")

        # Comparison by column
        def track_change_per_column(column:str) -> pl.Expr:
            a = pl.col(f"{column}__old")
            b = pl.col(f"{column}__new")
            
            return (
                (a.is_null() ^ b.is_null())                       # one side null, the other not
                | ((a != b) & a.is_not_null() & b.is_not_null())  # both not null but different
            ).alias(f"__changed_{column}")

        comparison_per_column = properties_to_compare.with_columns(
            [track_change_per_column(column) for column in tracked_fields]
            ).select(
                [pl.col(f"__changed_{column}").sum().alias(column) for column in tracked_fields]
                )
        per_column_changes = comparison_per_column.row(0, named=True)
        print("[audits] Changes per field:")
        for column, total in per_column_changes.items():
            print(f"\t{column}: {total}")
        
        print(f"[logs] snapshot_all rows={snapshot_all.height:,} cols={snapshot_all.columns}")

        # Guardrail =  ensure one id_ext by wave_date
        # snapshot_all = snapshot_all.unique(subset=["id_ext", "wave_date"], keep="last")

        # Convert to pandas to upload
        snapshot_all_pandas = snapshot_all.to_pandas()


        # Construct export key for new wave
        export_key = f"{export_folder}snapshot_{new_wave}.parquet"

        # Stream Parquet bytes to S3 (no temp file, no local path)
        # Change to a temporary file and multipart upload if memory constrains are high
        # Estimated filesize is  in disk is 759MB
        # Tried to avoid I/O operation using BytesIO
        # but upload broke due to the lost of TLS connection // Its my bad internet or BytesIO?
        # Using pandas estimated memory consume can be 2-5x bigger (~1.5-3.5GB RM when working)

        # 1) Write parquet to a temp file (no big RAM spike)
        with tempfile.NamedTemporaryFile(prefix="snapshot_upload_", suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name

        # ? Do we really need pandas?
        snapshot_all_pandas.to_parquet(tmp_path, index=False, engine="pyarrow", compression="snappy")
        
        # 2) Conservative multipart settings to avoid TLS drops
        tconfig = TransferConfig(
            multipart_threshold=16 * 1024 * 1024,  # 16 MB
            multipart_chunksize=16 * 1024 * 1024,  # 16 MB parts
            max_concurrency=4,                     # fewer parallel sockets
            use_threads=True,
        )
        try:
            client.upload_file(tmp_path, bucket, export_key, Config=tconfig)
            s3_url = f"s3://{bucket}/{export_key}"
            print(f"[snapshot_data] exported to {s3_url}")
            return s3_url
        except Exception as e:
                # Fail the task if we couldn't put the file in S3
                raise RuntimeError(f"[snapshot_data] S3 export failed: {e}") from e
        finally:
            try:
                os.remove(tmp_path)
            except OSError as e:
                print(f"OSError was {e}")
                pass

    @task()
    def upload_to_db(
        s3_url_main: str,
        s3_url_logs: str,
        *,
        public_schema: str,
        main_table: str,
        logs_table: str,
        _runtime_cfg: dict,
    ) -> dict:
        """
        Truncate → Load → Validate.

        - Truncates the target tables in public_schema up front
        - Streams Parquet from S3 directly into production via COPY
        - Validates nonzero counts after the load
        """
        import os
        import sqlalchemy as sa
        import boto3
        import psycopg2

        cfg = _runtime_cfg
        aws_conn_id = cfg.get("aws_conn_id")

        # 1) connect
        engine = get_pg_engine()
        print("[engine] Engine acquired")
        with engine.connect() as c:
            c.execute(sa.text("SELECT 1"))
        print("[connection] Database connectivity verified")

        ensure_table_exists(engine, public_schema, main_table)
        print(f"[ensure_table_exists] Ensured table {public_schema}.{main_table}")
        ensure_table_exists(engine, public_schema, logs_table)
        print(f"[ensure_table_exists] Ensured table {public_schema}.{logs_table}")

        # 2) clean production tables up front
        truncate_tables_restart_identity(engine, [main_table], schema=public_schema)
        print(f"[truncate] Truncated {public_schema}.{main_table}; leaving {public_schema}.{logs_table} intact for append")

        # 3) load directly into production  (IMPORTANT: no CSV headers; COPY uses HEADER false)
        upload_large_parquet_streaming(
            s3_url_main,
            f"{public_schema}.{main_table}",
            engine=engine,
            aws_conn_id=aws_conn_id,
            include_header=False,
            batch_rows=128_000,
            csv_chunk_bytes=32 * 1024 * 1024,
        )
        print(f"[upload_large_parquet_streaming] Loaded main table from {s3_url_main}")
        
        upload_large_parquet_streaming(
            s3_url_logs,
            f"{public_schema}.{logs_table}",
            engine=engine,
            aws_conn_id=aws_conn_id,
            include_header=False,
            batch_rows=128_000,
            csv_chunk_bytes=32 * 1024 * 1024,
        )
        print(f"[upload_large_parquet_streaming] Appended logs from {s3_url_logs}")

        # 4) validate counts in production
        with engine.connect() as c:
            cnt_main = c.execute(sa.text(f'SELECT COUNT(*) FROM "{public_schema}"."{main_table}"')).scalar() or 0
            cnt_logs = c.execute(sa.text(f'SELECT COUNT(*) FROM "{public_schema}"."{logs_table}"')).scalar() or 0
        if cnt_main == 0:
            raise RuntimeError("Production main table is empty after load.")
        if cnt_logs == 0:
            raise RuntimeError("Production logs table is empty after load.")
        print(f"[upload_to_db] Row counts → main: {cnt_main}, logs: {cnt_logs}")

        return {"production_counts": {"main": int(cnt_main), "logs": int(cnt_logs)}}

    @task()
    def cleanup_exports(
        _runtime_cfg: dict,
        export_urls: List[str],
        _load_summary: dict | None = None
    ) -> None:
        """
        Delete exported Parquet files under the configured S3 export folder once the load finishes.
        """
        
        # Get runtime config data
        cfg = _runtime_cfg
        export_folder = cfg.get("export_folder")
        region = cfg.get("region")
        auth_mode = cfg.get("auth")


        normalized_prefix = export_folder.strip("/")
        if not normalized_prefix:
            raise ValueError("Airflow Variable 'S3_EXPORT_FOLDER' must point to a non-empty folder.")
        normalized_prefix = f"{normalized_prefix}/"
        
        # Get export urls
        urls = [url for url in (export_urls or []) if url]
        if not urls:
            print("[cleanup_exports] No export URLs provided; nothing to delete.")
            return
        print(urls)


        client = None
        if auth_mode == "connection":
            try:
                conn = Connection.get("AWS_CONN_ID")
                client = boto3.client(
                    "s3",
                    aws_access_key_id=conn.login,
                    aws_secret_access_key=conn.password,
                    region_name=region,
                )
            except (AirflowNotFoundException, NoCredentialsError, PartialCredentialsError) as e:
                print(f"[list_and_read_all] Fallback to default AWS credentials (IAM/env). Cause: {e}")
                client = boto3.client("s3", region_name=region)
        else:
            client = boto3.client("s3", region_name=region)

        seen: set[tuple[str, str]] = set()
        for url in urls:
            parsed = urllib.parse.urlparse(url)
            if parsed.scheme != "s3":
                print(f"[cleanup_exports] Skipping non-S3 URL: {url}")
                continue

            bucket = parsed.netloc
            key = parsed.path.lstrip("/")
            if not key:
                print(f"[cleanup_exports] Skipping URL without key: {url}")
                continue

            if not key.startswith(normalized_prefix):
                print(f"[cleanup_exports] Skipping key outside export folder: s3://{bucket}/{key}")
                continue

            if (bucket, key) in seen:
                continue

            try:
                client.delete_object(Bucket=bucket, Key=key)
                print(f"[cleanup_exports] Deleted s3://{bucket}/{key}")
            except ClientError as e:
                raise RuntimeError(f"[cleanup_exports] Failed to delete s3://{bucket}/{key}: {e}") from e

            seen.add((bucket, key))

    # Chaining
    ## First three parallel tasks

    ### Get variables and connections
    runtime_cfg = get_runtime_cfg()

    ### Get run date of the dag
    run_date_str = _get_run_date()

    ### See folder structure
    list_and_read_all(runtime_cfg)

    ## Next two parallel tasks
    ### Get data folders
    folders = get_data_folders(runtime_cfg)
    
    ### Generate backup
    backup_info = backup(runtime_cfg, run_date_str)
    
    # Load zips
    std_map = load_dataframes(runtime_cfg, folders, backup_info)

    # Generate new data
    cleaned_listing = clean_and_concatenate(std_map)

    # Generate historical data
    historical_data = generate_historical_data(runtime_cfg, backup_info, cleaned_listing, folders)
    
    # Generate logs data
    logs_table = generate_logs_table(runtime_cfg, backup_info, cleaned_listing, folders, historical_data)

    # Send data to postgres database
    load_summary = upload_to_db(
        s3_url_main=historical_data,
        s3_url_logs=logs_table,
        public_schema="public",
        main_table="mkt_listings_main",
        logs_table="mkt_listings_log",
        _runtime_cfg=runtime_cfg
    )
    
    # Last task, clean up exports
    cleanup_exports(
        runtime_cfg,
        export_urls=[historical_data, logs_table],
        _load_summary=load_summary,
    )
    

    
    
    
dag = dag_clean_update_listings_data()
