import logging

import pandas as pd
from tqdm import tqdm
from ukroutes.common.utils import Paths
from ukroutes.routing import Route

FORMAT = "%(message)s"
logging.basicConfig(level="INFO", format=FORMAT, datefmt="[%X]")
logger = logging.getLogger(__name__)

postcodes = pd.read_parquet(Paths.PROCESSED / "onspd" / "postcodes.parquet")
nodes = pd.read_parquet(Paths.PROCESSED / "oproad" / "nodes.parquet")
edges = pd.read_parquet(Paths.PROCESSED / "oproad" / "edges.parquet")

pq_files = list(Paths.PROCESSED.glob("*.parquet"))
for file in tqdm(pq_files):
    outfile = Paths.OUT / f"{file.stem}_distances.parquet"
    if outfile.exists():
        logger.info(f"Skipping {file} as {outfile} already exists.")
        continue
    logger.info(f"Processing {file}...")
    source = pd.read_parquet(file).dropna(subset=["easting", "northing"])
    route = Route(source=source, target=postcodes, nodes=nodes, edges=edges)
    distances = route.route()
    distances.to_parquet(outfile)
    logger.info(f"Done processing {file}...")
