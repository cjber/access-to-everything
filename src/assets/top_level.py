import geopandas as gpd
import polars as pl
from dagster import asset

from src.common.utils import Paths


@asset
def greenspace():
    gs = gpd.read_file(Paths.GREENSPACE)
    gs["easting"], gs["northing"] = gs.geometry.x, gs.geometry.y
    return pl.from_pandas(gs[["id", "easting", "northing"]])


@asset
def health(hospitals, gpprac, dentists, pharmacies, overture):
    overture_health = (
        overture.filter(pl.col("high_category") == "Health and Lifestyle")
        .drop("high_category")
        .rename({"id": "code"})
        .with_columns(
            pl.col("easting").cast(pl.Int64), pl.col("northing").cast(pl.Int64)
        )
    )
    return pl.concat([hospitals, gpprac, dentists, pharmacies, overture_health])


@asset
def education(postcodes):
    return (
        pl.read_csv(
            Paths.EDUCATION,
            encoding="latin1",
            truncate_ragged_lines=True,
            columns=["URN", "Postcode"],
        )
        .drop_nulls(subset=["Postcode"])
        .rename({"URN": "code", "Postcode": "postcode"})
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
    )


@asset
def sustenance(overture):
    return overture.filter(pl.col("high_category") == "Sustenance and Essentials")


@asset
def community(overture):
    return overture.filter(pl.col("high_category") == "Community and Culture")


@asset
def services(overture):
    return overture.filter(pl.col("high_category") == "Services")


@asset
def food(overture):
    return overture.filter(pl.col("high_category") == "Food and Drink")


@asset
def retail(overture):
    return overture.filter(pl.col("high_category") == "Retail")


@asset
def transport():
    return pl.read_csv(
        Paths.TRANSPORT,
        infer_schema_length=100_000,
        columns=["ATCOCode", "Easting", "Northing"],
    ).rename({"ATCOCode": "code", "Easting": "easting", "Northing": "northing"})
