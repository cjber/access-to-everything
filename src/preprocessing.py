import json
import logging
import urllib.request
from io import BytesIO
from zipfile import ZipFile

import geopandas as gpd
import polars as pl
from pyproj import Transformer
from rich.logging import RichHandler
from ukroutes.oproad.utils import process_oproad

from src.common.utils import NHSEng, NHSScot, NHSWales, Paths

FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)

logger = logging.getLogger("rich")


def _read_zip_from_url(file_url: str, filename: str) -> BytesIO:
    r = urllib.request.urlopen(file_url).read()
    file = ZipFile(BytesIO(r))
    return file.open(filename)


def _fetch_scot_records(resource_id: int, limit: int = 100) -> pl.DataFrame:
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


def process_postcodes():
    (
        pl.read_csv(
            Paths.RAW / "ONSPD_FEB_2024.csv",
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
    eng_csv_path = Paths.RAW / "hospitals_england.csv"
    if not eng_csv_path.exists():
        eng_csv = _read_zip_from_url(*NHSEng.HOSPITALS_URL)
        (
            pl.read_csv(eng_csv, has_header=False)
            .select(["column_1", "column_10", "column_12"])
            .rename({"column_1": "code", "column_10": "postcode", "column_12": "close"})
            .filter(pl.col("close") != "")
            .drop("close")
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
    (
        pl.concat([eng, scot])
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
        .write_parquet(Paths.PROCESSED / "hospitals.parquet")
    )


def process_gppracs(postcodes):
    eng_csv_path = Paths.RAW / "gppracs_england.csv"
    if not eng_csv_path.exists():
        eng_csv = _read_zip_from_url(*NHSEng.GP_URL)
        (
            pl.read_csv(eng_csv, has_header=False)
            .select(["column_1", "column_10", "column_12"])
            .rename({"column_1": "code", "column_10": "postcode", "column_12": "close"})
            .filter(pl.col("close") != "")
            .drop("close")
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
    (
        pl.concat([eng, scot])
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
        .write_parquet(Paths.PROCESSED / "gppracs.parquet")
    )


def process_dentists(postcodes):
    eng_csv_path = Paths.RAW / "dentists_england.csv"
    if not eng_csv_path.exists():
        eng_csv = _read_zip_from_url(*NHSEng.DENTISTS_URL)
        (
            pl.read_csv(eng_csv, has_header=False)
            .select(["column_1", "column_10", "column_12"])
            .rename({"column_1": "code", "column_10": "postcode", "column_12": "close"})
            .filter(pl.col("close") != "")
            .drop("close")
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
    (
        pl.concat([eng, scot])
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
        .write_parquet(Paths.PROCESSED / "dentists.parquet")
    )


def process_pharmacies(postcodes):
    eng_csv_path = Paths.RAW / "pharmacies_england.csv"
    if not eng_csv_path.exists():
        eng_csv = _read_zip_from_url(*NHSEng.PHARMACIES_URL)
        eng = (
            pl.read_csv(eng_csv, has_header=False)
            .select(["column_1", "column_10"])
            .rename({"column_1": "code", "column_10": "postcode", "column_12": "close"})
            .filter(pl.col("close") != "")
            .drop("close")
            .write_csv(eng_csv_path)
        )
    eng = pl.read_csv(eng_csv_path)

    scot_csv_path = Paths.RAW / "pharmacies_scotland.csv"
    if not scot_csv_path.exists():
        (
            pl.read_csv(NHSScot.PHARMACIES_CSV)
            .select(["DispCode", "DispLocationPostcode"])
            .rename({"DispCode": "code", "DispLocationPostcode": "postcode"})
            .with_columns(("c" + pl.col("code").cast(pl.String)).alias("code"))
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

    (
        pl.concat([eng, scot, wales])
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
        .write_parquet(Paths.PROCESSED / "pharmacies.parquet")
    )


def process_greenspace():
    gs = gpd.read_file(Paths.RAW / "opgrsp_gb.gpkg", layer="access_point")
    gs["easting"], gs["northing"] = gs.geometry.x, gs.geometry.y
    pl.from_pandas(gs[["id", "easting", "northing"]]).write_parquet(
        Paths.PROCESSED / "greenspace.parquet"
    )


def process_education(postcodes, typ="Primary"):
    scot = pl.from_pandas(
        gpd.read_file(Paths.RAW / "SG_SchoolRoll_2023/SG_SchoolRoll_2023.shp")[
            ["SchUID", "SchoolType", "GridRefEas", "GridRefNor"]
        ].rename(
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
            Paths.RAW / "schools.csv",
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
            Paths.RAW / f"state_{typ.lower()}_schools_wales.csv",
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
    pl.read_csv(
        Paths.RAW / "Stops.csv",
        infer_schema_length=100_000,
        columns=["ATCOCode", "Easting", "Northing"],
    ).rename(
        {"ATCOCode": "code", "Easting": "easting", "Northing": "northing"}
    ).write_parquet(Paths.PROCESSED / "busstops.parquet")


def process_evpoints():
    transformer = Transformer.from_crs("epsg:4326", "epsg:27700")
    (
        pl.read_csv(
            Paths.RAW / "national-charge-point-registry.csv",
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
        .write_parquet(Paths.PROCESSED / "evpoints.parquet")
    )


def process_trainstations():
    transformer = Transformer.from_crs("epsg:4326", "epsg:27700")
    (
        pl.read_csv(Paths.RAW / "stations.csv", columns=["stationName", "lat", "long"])
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
# def process_health(hospitals, gpprac, dentists, pharmacies, overture):
#     overture_health = (
#         overture.filter(pl.col("high_category") == "Health and Lifestyle")
#         .drop("high_category")
#         .rename({"id": "code"})
#         .with_columns(
#             pl.col("easting").cast(pl.Int64), pl.col("northing").cast(pl.Int64)
#         )
#     )
#     return pl.concat([hospitals, gpprac, dentists, pharmacies, overture_health])

# def sustenance(overture):
#     return overture.filter(pl.col("high_category") == "Sustenance and Essentials")
#
#
# def community(overture):
#     return overture.filter(pl.col("high_category") == "Community and Culture")
#
#
# def services(overture):
#     return overture.filter(pl.col("high_category") == "Services")
#
#
# def food(overture):
#     return overture.filter(pl.col("high_category") == "Food and Drink")
#
#
# def retail(overture):
#     return overture.filter(pl.col("high_category") == "Retail")

# def process_overture():
#     transformer = Transformer.from_crs("epsg:4326", "epsg:27700")
#     overture = (
#         pl.read_parquet(
#             Paths.OVERTURE,
#             columns=["id", "lat", "long", "high_category", "low_category"],
#         )
#         .with_columns(
#             pl.struct(pl.col("lat"), pl.col("long"))
#             .map_elements(
#                 lambda row: transformer.transform(row["long"], row["lat"]),
#                 return_dtype=pl.List(pl.Float64),
#             )
#             .alias("coords")
#         )
#         .with_columns(
#             pl.col("coords").list[0].alias("easting"),
#             pl.col("coords").list[1].alias("northing"),
#         )
#         .with_columns(pl.col("high_category").str.strip_chars())
#         .select(["id", "easting", "northing", "high_category", "low_category"])
#         .unique()
#         .drop_nulls()
#     )
#     (
#         overture.group_by(["high_category", "low_category"])
#         .len()
#         .sort("len", descending=True)
#         .group_by("high_category")
#         .head(25)
#         .write_csv("./data/example_cats.csv")
#     )
#
