# ------------------------------------------------------ #
# This script generates a random graphs, and then makes
# some graph analysis operations on them. The operations
# are:
# - Pagerank calculation
# - Community detection
# - Dijkstra shortest paths from the most important nodes
# ------------------------------------------------------ #


import lithops
import networkx as nx
import pickle
import community.community_louvain as community_louvain

NUM_FUNCTIONS = 4
NODES = 5000
EDGE_PROB = 0.5
N_DIJKSTRA = 150

BUCKET = "graph-analysis"  # change me
EXPERIMENT_NAME = "experiment-no1"  # change me


# Tune the constant to adjust the execution times of the stages
# For NODES = 5000, EDGE_PROB = 0.5, N_DIJKSTRA = 150:
# - graph size is 77MB
# - pagerank takes around 10 seconds
# - community detection takes around 5-10 mins
# - dijkstra takes around 5-10 mins


get_graph_name = lambda x: x.key.split("/")[-1]


def gen_graphs(n):
    storage.create_bucket(BUCKET)
    try:
        last_index = int(storage.list_objects(BUCKET, "graphs/")[-1]["Key"][-1]) + 1
    except (IndexError, KeyError):
        last_index = 0
    graphs = []
    for i in range(last_index, n):
        G = nx.erdos_renyi_graph(NODES, EDGE_PROB)
        graphs.append(G)
    for i, graph in enumerate(graphs):
        storage.put_object(BUCKET, "graphs/graph{}".format(i), pickle.dumps(graph))


def compute_pagerank(obj):
    storage = lithops.Storage()
    graph = pickle.loads(obj.data_stream.read())
    paqerank = nx.pagerank(graph, alpha=0.99)
    storage.put_object(BUCKET, "pagerank/" + get_graph_name(obj), pickle.dumps(paqerank))


def community_detection(obj):
    storage = lithops.Storage()
    graph = pickle.loads(obj.data_stream.read())
    communities = community_louvain.best_partition(graph)
    storage.put_object(BUCKET, "communities/" + get_graph_name(obj), pickle.dumps(communities))


def first_n_dijkstra(obj):
    storage = lithops.Storage()
    graph = pickle.loads(obj.data_stream.read())
    pagerank = pickle.loads(storage.get_object(BUCKET, "pagerank/" + get_graph_name(obj)))
    important_nodes = sorted(pagerank, key=pagerank.get, reverse=True)[:N_DIJKSTRA]
    shortest_paths = {}
    for i in important_nodes:
        shortest_paths[i] = nx.single_source_dijkstra_path(graph, i)
    storage.put_object(BUCKET, "dijkstra/" + get_graph_name(obj), pickle.dumps(shortest_paths))


if __name__ == "__main__":
    storage = lithops.Storage()
    gen_graphs(NUM_FUNCTIONS)
    fexec = lithops.FunctionExecutor()
    fexec.map(community_detection, BUCKET + "/graphs/")
    fexec.map(compute_pagerank, BUCKET + "/graphs/").get_result()
    fexec.map(first_n_dijkstra, BUCKET + "/graphs/")
    fexec.wait()
    fexec.dump_stats_to_csv(EXPERIMENT_NAME)
    print("Finished")
