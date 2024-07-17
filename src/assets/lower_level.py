import json
import urllib.request
from io import BytesIO
from zipfile import ZipFile

import polars as pl
from pyproj import Transformer

from src.common.utils import NHSEng, NHSScot, NHSWales, Paths


def process_postcodes():
    return (
        pl.read_csv(
            Paths.POSTCODES, columns=["PCD", "OSEAST1M", "OSNRTH1M", "DOTERM", "CTRY"]
        )
        .rename({"PCD": "postcode", "OSEAST1M": "easting", "OSNRTH1M": "northing"})
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .filter(
            (pl.col("DOTERM").is_null())
            & (pl.col("CTRY").is_in(["N92000002", "L93000001", "M83000003"]).not_())
        )
        .drop(["DOTERM", "CTRY"])
        .drop_nulls()
    )

postcodes = process_postcodes()
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


def process_hospitals(postcodes):
    eng_csv_path = Paths.RAW / "hospitals_england.csv"
    if not eng_csv_path.exists():
        eng_csv = _read_zip_from_url(*NHSEng.HOSPITALS_URL)
        (
            pl.read_csv(eng_csv, has_header=False)
            .select(["column_1", "column_10"])
            .rename({"column_1": "code", "column_10": "postcode"})
            .write_csv(eng_csv_path)
        )
    eng = pl.read_csv(eng_csv_path)

    scot_csv_path = Paths.RAW / "hospitals_scotland.csv"
    if not scot_csv_path.exists():
        (
            _fetch_scot_records(NHSScot.HOSPITAL_ID)
            .select(["HospitalCode", "Postcode"])
            .rename({"HospitalCode": "code", "Postcode": "postcode"})
            .write_csv(scot_csv_path)
        )
    scot = pl.read_csv(scot_csv_path)
    return (
        pl.concat([eng, scot])
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
    )


def process_gppracs(postcodes):
    eng_csv_path = Paths.RAW / "gppracs_england.csv"
    if not eng_csv_path.exists():
        eng_csv = _read_zip_from_url(*NHSEng.GP_URL)
        (
            pl.read_csv(eng_csv, has_header=False)
            .select(["column_1", "column_10"])
            .rename({"column_1": "code", "column_10": "postcode"})
            .write_csv(eng_csv_path)
        )
    eng = pl.read_csv(eng_csv_path)

    scot_csv_path = Paths.RAW / "gppracs_england.csv"
    if not scot_csv_path.exists():
        (
            _fetch_scot_records(NHSScot.GP_ID)
            .select(["PracticeCode", "Postcode"])
            .rename({"PracticeCode": "code", "Postcode": "postcode"})
            .with_columns(pl.col("code").cast(pl.String))
            .write_csv(scot_csv_path)
        )
    scot = pl.read_csv(scot_csv_path)
    return (
        pl.concat([eng, scot])
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
    )


def process_dentists(postcodes):
    eng_csv_path = Paths.RAW / "dentists_england.csv"
    if not eng_csv_path.exists():
        eng_csv = _read_zip_from_url(*NHSEng.DENTISTS_URL)
        (
            pl.read_csv(eng_csv, has_header=False)
            .select(["column_1", "column_10"])
            .rename({"column_1": "code", "column_10": "postcode"})
            .write_csv(eng_csv_path)
        )
    eng = pl.read_csv(eng_csv_path)

    scot_csv_path = Paths.RAW / "dentists_scotland.csv"
    if not scot_csv_path.exists():
        (
            _fetch_scot_records(NHSScot.DENTISTS_ID)
            .select(["Dental_Practice_Code", "pc7"])
            .rename({"Dental_Practice_Code": "code", "pc7": "postcode"})
            .with_columns(("c" + pl.col("code").cast(pl.String)).alias("code"))
            .write_csv(scot_csv_path)
        )
    scot = pl.read_csv(scot_csv_path)
    return (
        pl.concat([eng, scot])
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
    )


def process_pharmacies(postcodes):
    eng_csv_path = Paths.RAW / "pharmacies_england.csv"
    if not eng_csv_path.exists():
        eng_csv = _read_zip_from_url(*NHSEng.PHARMACIES_URL)
        eng = (
            pl.read_csv(eng_csv, has_header=False)
            .select(["column_1", "column_10"])
            .rename({"column_1": "code", "column_10": "postcode"})
            .write_csv(eng_csv_path)
        )
    eng = pl.read_csv(eng_csv_path)

    scot_csv_path = Paths.RAW / "pharmacies_scotland.csv"
    if not scot_csv_path.exists():
        (
            pl.read_csv(NHSScot.PHARMACIES_CSV)
            .select(["DispCode", "DispLocationPostcode"])
            .rename({"DispCode": "code", "DispLocationPostcode": "postcode"})
            .with_columns(pl.col("code").cast(pl.String))
            .write_csv(scot_csv_path)
        )
    scot = pl.read_csv(scot_csv_path)

    wales_csv_path = Paths.RAW / "pharmacies_wales.csv"
    if not wales_csv_path.exists():
        (
            pl.read_excel(NHSWales.PHARMACIES_EXCEL)
            .select(["Account Number", "Post Code"])
            .rename({"Account Number": "code", "Post Code": "postcode"})
            .write_csv(wales_csv_path)
        )
    wales = pl.read_csv(wales_csv_path)

    return (
        pl.concat([eng, scot, wales])
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
    )


def process_overture():
    transformer = Transformer.from_crs("epsg:4326", "epsg:27700")
    overture = (
        pl.read_parquet(
            Paths.OVERTURE,
            columns=["id", "lat", "long", "high_category", "low_category"],
        )
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
        .with_columns(pl.col("high_category").str.strip_chars())
        .select(["id", "easting", "northing", "high_category", "low_category"])
        .unique()
        .drop_nulls()
    )
    (
        overture.group_by(["high_category", "low_category"])
        .len()
        .sort("len", descending=True)
        .group_by("high_category")
        .head(25)
        .write_csv("./data/example_cats.csv")
    )
