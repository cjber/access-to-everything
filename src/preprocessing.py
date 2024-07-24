import json
import logging
import urllib.request
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import geopandas as gpd
import polars as pl
from pyproj import Transformer
from rich.logging import RichHandler
from ukroutes.oproad.utils import process_oproad

from src.common.utils import Config, Paths

FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)

logger = logging.getLogger("rich")


def _read_zip_from_url(filename: str):
    filename = Config.NHS_ENG_URL + filename
    r = urllib.request.urlopen(filename).read()
    file = ZipFile(BytesIO(r))
    return file.open(f"{Path(filename).stem}.csv")


def _fetch_scot_records(resource_id: str, limit: int = 100) -> pl.DataFrame:
    initial_url = f"{Config.NHS_SCOT_URL}?resource_id={resource_id}&limit={limit}"
    response = urllib.request.urlopen(initial_url)
    data = response.read().decode()
    data_dict = json.loads(data)
    total_records = data_dict["result"]["total"]
    records = data_dict["result"]["records"]

    offset = limit
    while offset < total_records:
        paginated_url = f"{Config.NHS_SCOT_URL}?resource_id={resource_id}&limit={limit}&offset={offset}"
        response = urllib.request.urlopen(paginated_url)
        data = response.read().decode()
        data_dict = json.loads(data)
        records.extend(data_dict["result"]["records"])
        offset += limit
    return pl.DataFrame(records)


def process_postcodes():
    logger.info("Processing postcodes...")
    (
        pl.read_csv(
            Paths.RAW / "onspd" / "ONSPD_FEB_2024.csv",
            columns=["PCD", "OSEAST1M", "OSNRTH1M", "DOTERM", "CTRY"],
        )
        .rename({"PCD": "postcode", "OSEAST1M": "easting", "OSNRTH1M": "northing"})
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .filter(
            (pl.col("DOTERM").is_null())
            & (pl.col("CTRY").is_in(["N92000002", "L93000001", "M83000003"]).not_())
        )
        .drop(["DOTERM", "CTRY"])
        .drop_nulls()
        .write_parquet(Paths.PROCESSED / "onspd" / "postcodes.parquet")
    )


def process_hospitals(postcodes):
    logger.info("Processing hospitals...")
    eng_csv_path = Paths.RAW / "nhs" / "hospitals_england.csv"
    if not eng_csv_path.exists():
        eng_csv = _read_zip_from_url(Config.NHS_ENG_FILES["hospitals"])
        (
            pl.read_csv(eng_csv, has_header=False)
            .select(["column_1", "column_10", "column_12"])
            .rename({"column_1": "code", "column_10": "postcode", "column_12": "close"})
            .filter(pl.col("close") != "")
            .drop("close")
            .write_csv(eng_csv_path)
        )
    eng = pl.read_csv(eng_csv_path)

    scot_csv_path = Paths.RAW / "nhs" / "hospitals_scotland.csv"
    if not scot_csv_path.exists():
        (
            _fetch_scot_records(Config.NHS_SCOT_FILES["hospitals"])
            .select(["HospitalCode", "Postcode"])
            .rename({"HospitalCode": "code", "Postcode": "postcode"})
            .write_csv(scot_csv_path)
        )
    scot = pl.read_csv(scot_csv_path)
    (
        pl.concat([eng, scot])
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
        .write_parquet(Paths.PROCESSED / "hospitals.parquet")
    )


def process_gppracs(postcodes):
    logger.info("Processing GP practices...")
    eng_csv_path = Paths.RAW / "nhs" / "gppracs_england.csv"
    if not eng_csv_path.exists():
        eng_csv = _read_zip_from_url(Config.NHS_ENG_FILES["gppracs"])
        (
            pl.read_csv(eng_csv, has_header=False)
            .select(["column_1", "column_10", "column_12"])
            .rename({"column_1": "code", "column_10": "postcode", "column_12": "close"})
            .filter(pl.col("close") != "")
            .drop("close")
            .write_csv(eng_csv_path)
        )
    eng = pl.read_csv(eng_csv_path)

    scot_csv_path = Paths.RAW / "nhs" / "gppracs_scotland.csv"
    if not scot_csv_path.exists():
        (
            _fetch_scot_records(Config.NHS_SCOT_FILES["gppracs"])
            .select(["PracticeCode", "Postcode"])
            .rename({"PracticeCode": "code", "Postcode": "postcode"})
            .with_columns(("c" + pl.col("code").cast(pl.String)).alias("code"))
            .write_csv(scot_csv_path)
        )
    scot = pl.read_csv(scot_csv_path)
    (
        pl.concat([eng, scot])
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
        .write_parquet(Paths.PROCESSED / "gppracs.parquet")
    )


def process_dentists(postcodes):
    logger.info("Processing dentists...")
    eng_csv_path = Paths.RAW / "nhs" / "dentists_england.csv"
    if not eng_csv_path.exists():
        eng_csv = _read_zip_from_url(Config.NHS_ENG_FILES["dentists"])
        (
            pl.read_csv(eng_csv, has_header=False)
            .select(["column_1", "column_10", "column_12"])
            .rename({"column_1": "code", "column_10": "postcode", "column_12": "close"})
            .filter(pl.col("close") != "")
            .drop("close")
            .write_csv(eng_csv_path)
        )
    eng = pl.read_csv(eng_csv_path)

    scot_csv_path = Paths.RAW / "nhs" / "dentists_scotland.csv"
    if not scot_csv_path.exists():
        (
            _fetch_scot_records(Config.NHS_SCOT_FILES["dentists"])
            .select(["Dental_Practice_Code", "pc7"])
            .rename({"Dental_Practice_Code": "code", "pc7": "postcode"})
            .with_columns(("c" + pl.col("code").cast(pl.String)).alias("code"))
            .write_csv(scot_csv_path)
        )
    scot = pl.read_csv(scot_csv_path)
    (
        pl.concat([eng, scot])
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
        .write_parquet(Paths.PROCESSED / "dentists.parquet")
    )


def process_pharmacies(postcodes):
    logger.info("Processing pharmacies...")
    eng_csv_path = Paths.RAW / "nhs" / "pharmacies_england.csv"
    if not eng_csv_path.exists():
        eng_csv = _read_zip_from_url(Config.NHS_ENG_FILES["pharmacies"])
        eng = (
            pl.read_csv(eng_csv, has_header=False)
            .select(["column_1", "column_10", "column_12"])
            .rename({"column_1": "code", "column_10": "postcode", "column_12": "close"})
            .filter(pl.col("close") != "")
            .drop("close")
            .write_csv(eng_csv_path)
        )
    eng = pl.read_csv(eng_csv_path)

    scot_csv_path = Paths.RAW / "nhs" / "pharmacies_scotland.csv"
    if not scot_csv_path.exists():
        (
            pl.read_csv(Config.NHS_SCOT_FILES["pharmacies"])
            .select(["DispCode", "DispLocationPostcode"])
            .rename({"DispCode": "code", "DispLocationPostcode": "postcode"})
            .with_columns(("c" + pl.col("code").cast(pl.String)).alias("code"))
            .write_csv(scot_csv_path)
        )
    scot = pl.read_csv(scot_csv_path)

    wales_csv_path = Paths.RAW / "nhs" / "pharmacies_wales.csv"
    if not wales_csv_path.exists():
        (
            pl.read_excel(Config.NHS_WALES_URL + Config.NHS_WALES_FILES["pharmacies"])
            .select(["Account Number", "Post Code"])
            .rename({"Account Number": "code", "Post Code": "postcode"})
            .write_csv(wales_csv_path)
        )
    wales = pl.read_csv(wales_csv_path)

    (
        pl.concat([eng, scot, wales])
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
        .write_parquet(Paths.PROCESSED / "pharmacies.parquet")
    )


def process_greenspace():
    logger.info("Processing greenspace...")
    gs = gpd.read_file(
        Paths.RAW / "greenspace" / "opgrsp_gb.gpkg", layer="access_point"
    )
    gs["easting"], gs["northing"] = gs.geometry.x, gs.geometry.y
    pl.from_pandas(gs[["id", "easting", "northing"]]).write_parquet(
        Paths.PROCESSED / "greenspace.parquet"
    )


def process_education(postcodes, typ="Primary"):
    logger.info(f"Processing {typ} schools...")
    scot = pl.from_pandas(
        gpd.read_file(
            Paths.RAW / "education" / "SG_SchoolRoll_2023/SG_SchoolRoll_2023.shp"
        )[["SchUID", "SchoolType", "GridRefEas", "GridRefNor"]].rename(
            columns={
                "SchUID": "code",
                "SchoolType": "type",
                "GridRefEas": "easting",
                "GridRefNor": "northing",
            }
        )
    ).with_columns(
        pl.col("easting").cast(pl.Int64),
        pl.col("northing").cast(pl.Int64),
    )
    eng = (
        pl.read_csv(
            Paths.RAW / "education" / "schools.csv",
            encoding="latin1",
            truncate_ragged_lines=True,
            columns=["URN", "Postcode", "PhaseOfEducation (name)"],
        )
        .drop_nulls(subset=["Postcode"])
        .rename(
            {"URN": "code", "Postcode": "postcode", "PhaseOfEducation (name)": "type"}
        )
        .with_columns(
            pl.col("code").cast(pl.String),
            pl.col("postcode").str.replace(" ", ""),
        )
        .join(postcodes, on="postcode")
        .select(["code", "type", "easting", "northing"])
        .filter(pl.col("type") != "Not applicable")
    )

    wales = (
        pl.read_csv(
            Paths.RAW / "education" / f"state_{typ.lower()}_schools_wales.csv",
            columns=["FID", "easting", "northing"],
        )
        .with_columns(
            pl.lit(typ).alias("type"),
            pl.col("easting").cast(pl.Int64),
            pl.col("northing").cast(pl.Int64),
        )
        .rename({"FID": "code"})
        .select(["code", "type", "easting", "northing"])
    )
    schools = pl.concat(
        [
            scot.filter(pl.col("type") == typ),
            eng.filter(pl.col("type") == typ),
            wales,
        ]
    )
    schools.write_parquet(Paths.PROCESSED / f"{typ.lower()}_schools.parquet")


def process_busstops():
    logger.info("Processing bus stops...")
    pl.read_csv(
        Paths.RAW / "transport" / "Stops.csv",
        infer_schema_length=100_000,
        columns=["ATCOCode", "Easting", "Northing"],
    ).rename(
        {"ATCOCode": "code", "Easting": "easting", "Northing": "northing"}
    ).write_parquet(
        Paths.PROCESSED / "busstops.parquet"
    )


def process_evpoints():
    logger.info("Processing EV points...")
    transformer = Transformer.from_crs("epsg:4326", "epsg:27700")
    (
        pl.read_csv(
            Paths.RAW / "transport" / "national-charge-point-registry.csv",
            columns=["chargeDeviceID", "latitude", "longitude"],
        )
        .with_columns(
            pl.struct(pl.col("latitude"), pl.col("longitude"))
            .map_elements(
                lambda row: transformer.transform(row["latitude"], row["longitude"]),
                return_dtype=pl.List(pl.Float64),
            )
            .alias("coords")
        )
        .with_columns(
            pl.col("coords").list[0].alias("easting"),
            pl.col("coords").list[1].alias("northing"),
        )
        .select(["chargeDeviceID", "easting", "northing"])
        .drop_nulls(["easting", "northing"])
        .write_parquet(Paths.PROCESSED / "evpoints.parquet")
    )


def process_trainstations():
    logger.info("Processing train stations...")
    transformer = Transformer.from_crs("epsg:4326", "epsg:27700")
    (
        pl.read_csv(
            Paths.RAW / "transport" / "stations.csv",
            columns=["stationName", "lat", "long"],
        )
        .with_columns(
            pl.struct(pl.col("lat"), pl.col("long"))
            .map_elements(
                lambda row: transformer.transform(row["lat"], row["long"]),
                return_dtype=pl.List(pl.Float64),
            )
            .alias("coords")
        )
        .with_columns(
            pl.col("coords").list[0].alias("easting"),
            pl.col("coords").list[1].alias("northing"),
        )
        .select(["stationName", "easting", "northing"])
        .write_parquet(Paths.PROCESSED / "trainstations.parquet")
    )


def main():
    process_postcodes()
    postcodes = pl.read_parquet(Paths.PROCESSED / "onspd" / "postcodes.parquet")

    process_hospitals(postcodes)
    process_gppracs(postcodes)
    process_dentists(postcodes)
    process_pharmacies(postcodes)
    process_greenspace()
    process_education(postcodes, "Primary")
    process_education(postcodes, "Secondary")
    process_busstops()
    process_evpoints()
    process_trainstations()
    _ = process_oproad(outdir=Paths.PROCESSED / "oproad")


if __name__ == "__main__":
    main()
