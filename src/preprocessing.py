import json
import logging
import urllib.request
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import geopandas as gpd
import pandas as pd
import polars as pl
from pyproj import Transformer
from shapely import MultiPolygon, Polygon
from ukroutes.oproad.utils import process_oproad

from src.common.utils import Config, Paths

FORMAT = "%(message)s"
logging.basicConfig(level="INFO", format=FORMAT, datefmt="[%X]")
logger = logging.getLogger(__name__)


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


def _welsh_hospitals():
    # view-source:https://111.wales.nhs.uk/localservices/?s=Hospital&pc=n&sort=default

    transformer = Transformer.from_crs("epsg:4326", "epsg:27700")
    data = [
        [51.4133444539694, -3.28377486575934],
        [51.4138556369325, -3.28514814376831],
        [51.4849474783373, -3.1612482472817],
        [51.5062657139985, -3.19123103037047],
        [51.5112804272092, -3.58044089655397],
        [51.51799663945, -3.57236981391907],
        [51.547415888765, -3.38999753947103],
        [51.6388065075537, -2.68531304159188],
        [51.7459258734778, -3.38833204579813],
        [51.7637316359143, -3.38450143362687],
        [51.7734643262788, -3.20110930158957],
        [51.7975016321807, -4.96711832888589],
        [51.8127008240244, -4.96524095535278],
        [51.8240075605359, -3.03295606642652],
        [51.8323876067318, -2.506967999478],
        [51.8324663287129, -2.99364155631735],
        [51.8565812621567, -4.33254807844281],
        [51.8659306591827, -2.23069399061431],
        [51.9486924983437, -3.38355491148454],
        [51.994385211708, -4.97824698404466],
        [51.9981794720837, -3.79578634614093],
        [51.9987670815114, -3.79677414894104],
        [52.2416535184377, -3.37628748618039],
        [52.2434963517339, -3.3768904209137],
        [52.2435883224226, -4.2552387714386],
        [52.3416287602581, -3.04823575371185],
        [52.3445166504757, -3.04704144819948],
        [52.4160909073528, -4.07179713249207],
        [52.4194934839656, -4.08155885269258],
        [52.4521418771372, -3.53839414737319],
        [52.5914029504628, -3.84453192591482],
        [52.7404087470174, -3.880553486068],
        [53.1298046783258, -4.26160487621684],
        [53.1857410134828, -3.40858876471376],
        [53.2085134951299, -2.89653389729877],
        [53.2563079735153, -4.30309266393227],
        [53.2659853886672, -3.57910547727238],
        [53.2690529053159, -3.21620301462873],
        [53.2879810001708, -3.70920633150231],
        [53.304111060094, -4.6147000124997],
        [53.3110990636943, -3.82670129751938],
    ]

    return (
        pl.DataFrame(
            {
                "code": [f"w{i}" for i in range(len(data))],
                "long": [row[1] for row in data],
                "lat": [row[0] for row in data],
            }
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
            pl.col("coords").list[0].alias("easting").cast(pl.Int64),
            pl.col("coords").list[1].alias("northing").cast(pl.Int64),
        )
        .select(["code", "easting", "northing"])
    )


def process_hospitals(postcodes):
    eng_csv_path = Paths.RAW / "nhs" / "hospitals_england.csv"
    if not eng_csv_path.exists():
        eng_csv = _read_zip_from_url(Config.NHS_ENG_FILES["hospitals"])
        (
            pl.read_csv(eng_csv, has_header=False)
            .select(["column_1", "column_10", "column_12"])
            .rename({"column_1": "code", "column_10": "postcode", "column_12": "close"})
            .filter(pl.col("close") == "")
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
    welsh = _welsh_hospitals()
    pl.concat(
        [
            pl.concat([eng, scot])
            .with_columns(pl.col("postcode").str.replace(" ", ""))
            .join(postcodes, on="postcode")
            .select(["code", "easting", "northing"]),
            welsh,
        ]
    ).write_parquet(Paths.PROCESSED / "hospitals.parquet")


def process_gppracs(postcodes):
    logger.info("Processing GP practices...")
    eng_csv_path = Paths.RAW / "nhs" / "gppracs_england.csv"
    if not eng_csv_path.exists():
        eng_csv = _read_zip_from_url(Config.NHS_ENG_FILES["gppracs"])
        (
            pl.read_csv(eng_csv, has_header=False)
            .select(["column_1", "column_10", "column_12"])
            .rename({"column_1": "code", "column_10": "postcode", "column_12": "close"})
            .filter(pl.col("close") == "")
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
            .filter(pl.col("close") == "")
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
        .filter(
            (pl.col("easting") > 0)
            & (pl.col("northing") > 0)
            & pl.col("easting").is_finite()
            & pl.col("northing").is_finite()
        )
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


def process_bluespace():
    logger.info("Processing bluespace...")
    bluespace = gpd.read_parquet(Paths.RAW / "osm" / "gb-water.parquet")
    coast = (
        gpd.read_parquet(Paths.RAW / "osm" / "gb-coast.parquet")
        .to_crs(27700)
        .get_coordinates()
        .round()
    )
    bs = bluespace[
        (bluespace.geometry.apply(lambda x: isinstance(x, (MultiPolygon, Polygon))))
    ].to_crs(27700)
    bs = bs[bs.area > 10_000].get_coordinates().round()
    bs = (
        pd.concat([coast, bs])
        .drop_duplicates()
        .rename(columns={"x": "easting", "y": "northing"})
    )
    bs.to_parquet(Paths.PROCESSED / "bluespace.parquet", index=False)


def process_overture():
    logger.info("Processing Overture...")
    overture = pl.read_parquet(
        Paths.RAW / "overture" / "places_uk_2024_07_22-categories.parquet"
    ).filter(pl.col("main_category") != "landmark_and_historical_building")
    pubs = overture.filter(
        (pl.col("main_category") == "pub")
        | pl.col("alternate_category").str.split("|").list.contains("pub")
    )
    restaurant = overture.filter((pl.col("main_category").str.contains("restaurant")))
    post_office = overture.filter(
        (pl.col("main_category") == "post_office")
        | (pl.col("alternate_category").str.split("|").list.contains("post_office"))
    )
    cafes = overture.filter(pl.col("main_category").is_in(["cafe", "coffee_shop"]))
    convenience_stores = overture.filter(
        (pl.col("main_category") == "convenience_store")
        | (
            pl.col("alternate_category")
            .str.split("|")
            .list.contains("convenience_store")
        )
    )
    pubs.write_parquet(Paths.PROCESSED / "pubs.parquet")
    restaurant.write_parquet(Paths.PROCESSED / "restaurants.parquet")
    post_office.write_parquet(Paths.PROCESSED / "post_offices.parquet")
    cafes.write_parquet(Paths.PROCESSED / "cafes.parquet")
    convenience_stores.write_parquet(Paths.PROCESSED / "convenience_stores.parquet")


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
    process_bluespace()
    process_overture()

    _ = process_oproad(save=True)


if __name__ == "__main__":
    main()
