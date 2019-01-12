package es.evadell.db2etl;

import java.util.HashSet;
import java.util.Set;

public class RowGraph {
	private Set<RowGraphNode> nodes = new HashSet<RowGraphNode>();
	private Set<RowGraphEdge> edges = new HashSet<RowGraphEdge>();

	public Set<RowGraphNode> getNodes() {
		return nodes;
	}

	public void setNodes(Set<RowGraphNode> nodes) {
		this.nodes = nodes;
	}

	public Set<RowGraphEdge> getEdges() {
		return edges;
	}

	public void setEdges(Set<RowGraphEdge> edges) {
		this.edges = edges;
	}
	
	public boolean addNode(RowGraphNode node) {
		if (nodes.contains(node)) return false;
		else nodes.add(node);
		return true;
	}
	
	public void addEdge(RowGraphEdge edge) {
		this.edges.add(edge);
	}
	
}
