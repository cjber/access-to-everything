import geopandas as gpd
from pyproj import Transformer
import polars as pl

from src.common.utils import Paths


def process_greenspace():
    gs = gpd.read_file(Paths.GREENSPACE)
    gs["easting"], gs["northing"] = gs.geometry.x, gs.geometry.y
    return pl.from_pandas(gs[["id", "easting", "northing"]])


def process_health(hospitals, gpprac, dentists, pharmacies, overture):
    overture_health = (
        overture.filter(pl.col("high_category") == "Health and Lifestyle")
        .drop("high_category")
        .rename({"id": "code"})
        .with_columns(
            pl.col("easting").cast(pl.Int64), pl.col("northing").cast(pl.Int64)
        )
    )
    return pl.concat([hospitals, gpprac, dentists, pharmacies, overture_health])


def process_education(postcodes):
    scot = pl.from_pandas(
        gpd.read_file(Paths.SCOT_EDUCATION)[
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
            Paths.ENG_EDUCATION,
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

    wales_primary = (
        pl.read_csv(
            "./data/raw/state_primary_schools_wales.csv",
            columns=["FID", "easting", "northing"],
        )
        .with_columns(
            pl.lit("Primary").alias("type"),
            pl.col("easting").cast(pl.Int64),
            pl.col("northing").cast(pl.Int64),
        )
        .rename({"FID": "code"})
        .select(["code", "type", "easting", "northing"])
    )
    primary_schools = pl.concat(
        [
            scot.filter(pl.col("type") == "Primary"),
            eng.filter(pl.col("type") == "Primary"),
            wales_primary,
        ]
    )

    wales_secondary = (
        pl.read_csv(
            "./data/raw/state_secondary_schools_wales.csv",
            columns=["FID", "easting", "northing"],
        )
        .with_columns(
            pl.lit("Primary").alias("type"),
            pl.col("easting").cast(pl.Int64),
            pl.col("northing").cast(pl.Int64),
        )
        .rename({"FID": "code"})
        .select(["code", "type", "easting", "northing"])
    )
    secondary_schools = pl.concat(
        [
            scot.filter(pl.col("type") == "Secondary"),
            eng.filter(pl.col("type") == "Secondary"),
            wales_secondary,
        ]
    )
    return primary_schools, secondary_schools


def process_busstops():
    return pl.read_csv(
        Paths.TRANSPORT,
        infer_schema_length=100_000,
        columns=["ATCOCode", "Easting", "Northing"],
    ).rename({"ATCOCode": "code", "Easting": "easting", "Northing": "northing"})


def process_evpoints():
    transformer = Transformer.from_crs("epsg:4326", "epsg:27700")
    return (
        pl.read_csv(
            "./data/raw/national-charge-point-registry.csv",
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
    )


def process_trainstations():
    transformer = Transformer.from_crs("epsg:4326", "epsg:27700")
    return (
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
    )


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
