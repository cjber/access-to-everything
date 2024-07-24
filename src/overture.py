
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
