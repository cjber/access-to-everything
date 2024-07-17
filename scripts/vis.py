import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd

df = pd.read_csv("./data/out/distances_health.csv")
df.loc[df["distance"] >= 60, "distance"] = 60
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.easting, df.northing))
gdf.plot(column="distance")
plt.show()
