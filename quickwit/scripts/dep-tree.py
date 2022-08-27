import fileinput
from collections import defaultdict
import graphviz

FILTER = {
	"quickwit-backward-compat",
	"quickwit-actors",
	"quickwit-cli",
	"quickwit-doc-mapper",
	"quickwit-search",
	"quickwit-common",
	"quickwit-indexing",
	"quickwit-metastore",
	"quickwit-proto",
	"quickwit-directories",
	"quickwit-common",
	"quickwit-serve",
	"quickwit-storage",
	"quickwit-cluster",
	"quickwit-index",
    "quickwit-control-plane",
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
