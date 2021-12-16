import fileinput
from collections import defaultdict
import graphviz

FILTER = {
	# "quickwit-backward-compat",
	# "quickwit-actors",
	# "quickwit-cli",
	"quickwit-index-config",
	"quickwit-search",
	"quickwit-common",
	"quickwit-indexing",
	"quickwit-metastore",
	# "quickwit-swim",
	# "quickwit-proto",
	"quickwit-directories",
	"quickwit-common",
	"quickwit-serve",
	"quickwit-storage",
	"quickwit-cluster",
	"quickwit-core",
    "tantivy"
}

def deps():
    deps = defaultdict(set)
    last_level = {}
    old_code = 10
    for line in fileinput.input():
        line = line.strip()
        if len(line) < 2:
            continue
        (code, package) = (line[0], line[1:])
        if package not in FILTER:
            continue
        code = int(code)
        last_level[code] = package
        print(line)
        if code > 0:
            if (code - 1) in last_level:
                deps[last_level[code - 1]].add(package)
    return dict(deps)


deps_graph = deps()

dot = graphviz.Digraph(filename='deps', directory='.', format='svg')

for (from_node, to_nodes) in deps_graph.items():
    if from_node not in FILTER:
        continue
    dot.node(from_node, from_node)
    for to_node in to_nodes:
        print((from_node, to_node))
        if to_node == from_node:
            continue
        if to_node not in FILTER:
            continue
        dot.edge(from_node, to_node)
dot.render()
# val_map = {'A': 1.0,
#            'D': 0.5714285714285714,
#            'H': 0.0}

# values = [val_map.get(node, 0.25) for node in G.nodes()]

# pos = nx.spring_layout(G)
# nx.draw_networkx_labels(G, pos)

# nx.draw(G, cmap = plt.get_cmap('jet'))
# plt.savefig('deps.png', dpi=300)
# plt.show()
