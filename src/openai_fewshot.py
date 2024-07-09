import geopandas as gpd
from dotenv import load_dotenv
from openai import OpenAI
from tqdm import tqdm
import pandas as pd
import polars as pl

load_dotenv()

low_categories = gpd.read_file("./data/poi_uk.gpkg", geometry=False)
low_categories["main_category"] = low_categories["main_category"].str.strip('"')
# low_categories["combined_category"] = (
#     "Main Category: "
#     + low_categories["main_category"]
#     + "\n\nAlternative Categories: "
#     + low_categories["alternate_category"]
#     .str.split("|")
#     .apply(lambda x: x[0] if x else None)
# )
low_categories["alternate_category_0"] = (
    low_categories["alternate_category"]
    .str.split("|")
    .apply(lambda x: x[0] if x else None)
)
low_categories = low_categories["alternate_category_0"].dropna().unique().tolist()

client = OpenAI()

high_categories = """
"Green Space": Parks, open space, public gardens, organised recreation areas, playgrounds
"Health and Lifestyle": GPs, pharmacies, dentists, sports facilities, swimming pool, gyms
"Education": Schools, childcare, universities, day centres
"Sustenance and Essentials": Convenience stores, grocery stores, off-licences, supermarkets, fresh food markets
"Transport": bus stops, train stations, subway stations, tram stops, bicycle parking, EV charging
"Community and Culture": Event spaces, places of worship, cinemas, museums
"Services": Banks, Post offices, beauty salons
"Food and Drink": Restaurants, bars, nightclubs, fast food, cafes
"Retail": Shopping malls, local shops
"Other": Anything that doesn't fit into the above categories
"""

out_categories = []
for low in tqdm(low_categories):
    completion = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": f"""
                You are given the name of a point of interest (POI), a low level category name. You must classify this POI into a single high level category from the list below. For each category in the list you are given some examples. ONLY OUTPUT THE CATEGORY NAME WITH NO OTHER TEXT.

                Low level Category:

                {low}

                High level Categories: 

                {high_categories}

                High level category:
                """,
            },
        ],
    )
    out_categories.append({low: completion.choices[0].message.content})


out = [out for out in out_categories if None not in out]
# Prepare data for the DataFrame
low_category = []
high_category = []

for entry in out:
    for key, value in entry.items():
        low_category.append(key)
        high_category.append(value)

# Create the DataFrame
df = pl.DataFrame({"low_category": low_category, "high_category": high_category})
df.with_columns(pl.col("high_category").str.replace_all('"', "")).write_csv(
    "./data/openai_category_mapping-alternate.csv"
)


df = pd.read_csv("./data/openai_category_mapping-alternate.csv")
overture = gpd.read_file("./data/poi_uk.gpkg")
overture["alternate_category_0"] = (
    overture["alternate_category"].str.split("|").apply(lambda x: x[0] if x else None)
)
overture = overture.merge(
    df, left_on="alternate_category_0", right_on="low_category", how="left"
)

overture.to_parquet("./data/raw/overture_poi_alternate.parquet", index=False)
overture.sample(100).to_csv("./data/raw/overture_poi_alternate_sample.csv", index=False)
