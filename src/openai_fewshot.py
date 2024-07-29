import json

import geopandas as gpd
import pandas as pd
import polars as pl
from dotenv import load_dotenv
from openai import OpenAI
from tqdm import tqdm

load_dotenv()

client = OpenAI()

# new_categories_mapping = {
#     "Health and Lifestyle": ["...", "Other"],
#     "Sustenance and Essentials": ["...", "Other"],
#     "Community and Culture": ["...", "Other"],
#     "Services": ["...", "Other"],
#     "Food and Drink": ["...", "Other"],
#     "Retail": ["...", "Other"],
# }
# completion = client.chat.completions.create(
#     model="gpt-4o",
#     messages=[
#         {
#             "role": "user",
#             "content": f"""
#             You have been tasked with naming additional categories for a collection of points of interest within the United Kingdom. You have to complete the following JSON replacing the '...' with a collection of up to 9 new categories for each higher level category. Return only the structured JSON.
#
#             {new_categories_mapping}
#
#             Answer:\n\n
#             """,
#         },
#     ],
# )
# gpt_mapping = completion.choices[0].message.content
with open("./data/raw/overture/gpt_mappings.json", "r") as f:
    gpt_mapping = json.loads(f.read())

overture = (
    pl.from_pandas(
        gpd.read_parquet("./data/raw/overture/places_uk_2024_07_22.parquet").drop(
            columns="geometry"
        )
    )
    .filter(pl.col("LSOA21CD") != "")
    .with_columns(
        pl.col("alternate_category").str.split("|").list[0].alias("train_category")
    )
    .with_columns(
        pl.when(pl.col("train_category").is_null())
        .then(pl.col("main_category"))
        .otherwise(pl.col("train_category"))
        .alias("train_category")
    )
    .drop_nulls(subset=["train_category"])
)
overture

overture_categories = overture["train_category"].unique().to_list()


target_categories = """
"Green Space": Parks, open space, public gardens, organised recreation areas, playgrounds
"Health and Lifestyle": GPs, pharmacies, dentists, sports facilities, swimming pool, gyms
"Education": Schools, childcare, universities, day centres
"Sustenance and Essentials": Convenience stores, grocery stores, off-licences, supermarkets, fresh food markets
"Transport": bus stops, train stations, subway stations, tram stops, bicycle parking, EV charging
"Community and Culture": Event spaces, places of worship, cinemas, museums
"Services": Banks, post offices, beauty salons
"Food and Drink": Restaurants, bars, nightclubs, fast food, cafes
"Retail": Shopping malls, local shops
"Other": Anything that doesn't fit into the above categories
"""


high_categories = []
for row in tqdm(overture_categories):
    completion = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": f"""
                You are given a category name of a Point of Interest (POI) in the United Kingdom. Based on this category, you must suggest a single high level category from the list below. For each category in the list you are given some examples. ONLY OUTPUT THE NEW CATEGORY NAME WITH NO OTHER TEXT.

                Original Category:

                {row}

                High level categories: 

                {target_categories}

                ---

                High level category:

                """,
            },
        ],
    )

    high_categories.append(
        {
            "train_category": row,
            "high_category": completion.choices[0].message.content,
        }
    )

high_categories_df = pl.DataFrame(high_categories).with_columns(
    pl.col("high_category").str.replace_all('"', "")
)
high_categories_df["high_category"].unique()
high_categories_df.write_parquet(
    "./data/raw/overture/overture_high_categories_mapping.parquet"
)


low_categories = []
for row in tqdm(high_categories_df.rows(named=True)):
    high_category = row["high_category"]
    if high_category in gpt_mapping:
        choice = gpt_mapping[high_category]
    else:
        continue

    completion = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": f"""
                You are given a category name of a Point of Interest (POI) in the United Kingdom. Based on this category, you must suggest a new category from the list below. ONLY OUTPUT THE CATEGORY NAME WITH NO OTHER TEXT.

                Original Category:

                {row['train_category']}

                New Categories: 

                {choice}

                ---

                New Category:
                """,
            },
        ],
    )
    low_categories.append(
        {
            "train_category": row["train_category"],
            "high_category": high_category,
            "low_category": completion.choices[0].message.content,
        }
    )

all_categories_df = pl.DataFrame(low_categories)
all_categories_df.write_parquet(
    "./data/raw/overture/overture_categories_mapping.parquet"
)
all_categories_df.write_csv("./data/raw/overture/overture_categories_mapping.csv")
all_categories_df.filter(pl.col("low_category") == "Gym")["train_category"].to_list()
overture.join(all_categories_df, on="train_category").write_parquet(
    "./data/raw/overture/places_uk_2024_07_22-categories.parquet"
)
