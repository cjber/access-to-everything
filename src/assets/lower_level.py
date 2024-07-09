import json
import urllib.request
from io import BytesIO
from zipfile import ZipFile

import polars as pl
from dagster import asset
from pyproj import Transformer

from src.common.utils import NHSEng, NHSScot, NHSWales, Paths


@asset
def postcodes():
    return (
        pl.read_csv(Paths.POSTCODES, columns=["PCD", "OSEAST1M", "OSNRTH1M", "DOTERM"])
        .rename({"PCD": "postcode", "OSEAST1M": "easting", "OSNRTH1M": "northing"})
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .filter(pl.col("DOTERM").is_null())
        .drop(["DOTERM"])
    )


def _read_zip_from_url(file_url, filename):
    r = urllib.request.urlopen(file_url).read()
    file = ZipFile(BytesIO(r))
    return file.open(filename)


def _fetch_scot_records(resource_id, limit=100):
    base_url = "https://www.opendata.nhs.scot/api/3/action/datastore_search"
    initial_url = f"{base_url}?resource_id={resource_id}&limit={limit}"
    response = urllib.request.urlopen(initial_url)
    data = response.read().decode()
    data_dict = json.loads(data)
    total_records = data_dict["result"]["total"]
    records = data_dict["result"]["records"]

    offset = limit
    while offset < total_records:
        paginated_url = (
            f"{base_url}?resource_id={resource_id}&limit={limit}&offset={offset}"
        )
        response = urllib.request.urlopen(paginated_url)
        data = response.read().decode()
        data_dict = json.loads(data)
        records.extend(data_dict["result"]["records"])
        offset += limit

    return pl.DataFrame(records)


@asset
def hospitals(postcodes):
    eng_csv = _read_zip_from_url(*NHSEng.HOSPITALS_URL)
    eng = (
        pl.read_csv(eng_csv, has_header=False)
        .select(["column_1", "column_10"])
        .rename({"column_1": "code", "column_10": "postcode"})
    )
    scot = (
        _fetch_scot_records(NHSScot.HOSPITAL_ID)
        .select(["HospitalCode", "Postcode"])
        .rename({"HospitalCode": "code", "Postcode": "postcode"})
    )
    return (
        pl.concat([eng, scot])
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
    )


@asset
def gpprac(postcodes):
    eng_csv = _read_zip_from_url(*NHSEng.GP_URL)
    eng = (
        pl.read_csv(eng_csv, has_header=False)
        .select(["column_1", "column_10"])
        .rename({"column_1": "code", "column_10": "postcode"})
    )
    scot = (
        _fetch_scot_records(NHSScot.GP_ID)
        .select(["PracticeCode", "Postcode"])
        .rename({"PracticeCode": "code", "Postcode": "postcode"})
        .with_columns(pl.col("code").cast(pl.String))
    )
    return (
        pl.concat([eng, scot])
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
    )


@asset
def dentists(postcodes):
    eng_csv = _read_zip_from_url(*NHSEng.DENTISTS_URL)
    eng = (
        pl.read_csv(eng_csv, has_header=False)
        .select(["column_1", "column_10"])
        .rename({"column_1": "code", "column_10": "postcode"})
    )
    scot = (
        _fetch_scot_records(NHSScot.DENTISTS_ID)
        .select(["Dental_Practice_Code", "pc7"])
        .rename({"Dental_Practice_Code": "code", "pc7": "postcode"})
        .with_columns(pl.col("code").cast(pl.String))
    )
    return (
        pl.concat([eng, scot])
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
    )


@asset
def pharmacies(postcodes):
    eng_csv = _read_zip_from_url(*NHSEng.PHARMACIES_URL)
    eng = (
        pl.read_csv(eng_csv, has_header=False)
        .select(["column_1", "column_10"])
        .rename({"column_1": "code", "column_10": "postcode"})
    )
    scot = (
        pl.read_csv(NHSScot.PHARMACIES_CSV)
        .select(["DispCode", "DispLocationPostcode"])
        .rename({"DispCode": "code", "DispLocationPostcode": "postcode"})
        .with_columns(pl.col("code").cast(pl.String))
    )
    wales = (
        pl.read_excel(NHSWales.PHARMACIES_EXCEL)
        .select(["Account Number", "Post Code"])
        .rename({"Account Number": "code", "Post Code": "postcode"})
    )
    return (
        pl.concat([eng, scot, wales])
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
    )


@asset
def overture():
    transformer = Transformer.from_crs("epsg:4326", "epsg:27700")
    return (
        pl.read_parquet(Paths.OVERTURE, columns=["id", "lat", "long", "high_category"])
        .with_columns(
            pl.struct(pl.col("lat"), pl.col("long"))
            .map_elements(
                lambda row: transformer.transform(row["long"], row["lat"]),
                return_dtype=pl.List(pl.Float64),
            )
            .alias("coords")
        )
        .with_columns(
            pl.col("coords").list[0].alias("easting"),
            pl.col("coords").list[1].alias("northing"),
        )
        .select(["id", "easting", "northing", "high_category"])
    )
