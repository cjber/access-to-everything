import cudf
import pandas as pd
from dagster import asset
from ukroutes import Routing
from ukroutes.common.utils import Paths
from ukroutes.process_routing import add_to_graph, add_topk


@asset
def nodes():
    return cudf.from_pandas(pd.read_parquet(Paths.OS_GRAPH / "nodes.parquet"))


@asset
def edges():
    return cudf.from_pandas(pd.read_parquet(Paths.OS_GRAPH / "edges.parquet"))


def _routing(df, name, outputs, nodes, edges):
    inputs = df.to_pandas().dropna()
    outputs = outputs.to_pandas().dropna()

    nodes_c = nodes.copy()
    edges_c = edges.copy()

    inputs, nodes_c, edges_c = add_to_graph(inputs, nodes_c, edges_c, 1)
    outputs, nodes_c, edges_c = add_to_graph(outputs, nodes_c, edges_c, 1)

    inputs = add_topk(inputs, outputs, 10)

    routing = Routing(
        name=name,
        edges=edges_c,
        nodes=nodes_c,
        outputs=outputs,
        inputs=inputs,
        weights="time_weighted",
        min_buffer=5000,
        max_buffer=500_000,
    )
    routing.fit()
    distances = (
        routing.distances.set_index("vertex")
        .join(cudf.from_pandas(outputs).set_index("node_id"), how="right")
        .reset_index()
    )
    OUT_FILE = Paths.OUT_DATA / f"distances_{name}.csv"
    distances.to_pandas().to_csv(OUT_FILE, index=False)


@asset
def greenspace_distances(greenspace, postcodes, nodes, edges):
    name = "greenspace"
    _routing(greenspace, name, postcodes, nodes, edges)


@asset
def health_distances(health, postcodes, nodes, edges):
    name = "health"
    _routing(health, name, postcodes, nodes, edges)


@asset
def education_distances(education, postcodes, nodes, edges):
    name = "education"
    _routing(education, name, postcodes, nodes, edges)


@asset
def sustenance_distances(sustenance, postcodes, nodes, edges):
    name = "sustenance"
    _routing(sustenance, name, postcodes, nodes, edges)


@asset
def community_distances(community, postcodes, nodes, edges):
    name = "community"
    _routing(community, name, postcodes, nodes, edges)


@asset
def services_distances(services, postcodes, nodes, edges):
    name = "services"
    _routing(services, name, postcodes, nodes, edges)


@asset
def food_distances(food, postcodes, nodes, edges):
    name = "food"
    _routing(food, name, postcodes, nodes, edges)


@asset
def retail_distances(retail, postcodes, nodes, edges):
    name = "retail"
    _routing(retail, name, postcodes, nodes, edges)


@asset
def transport_distances(transport, postcodes, nodes, edges):
    name = "transport"
    _routing(transport, name, postcodes, nodes, edges)
