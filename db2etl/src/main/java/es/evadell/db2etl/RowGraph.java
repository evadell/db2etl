package es.evadell.db2etl;

import es.evadell.db2etl.model.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RowGraph {
	private HashMap<Table,ArrayList<RowGraphNode>> nodes = new HashMap<Table,ArrayList<RowGraphNode>>();
	private Set<RowGraphEdge> edges = new HashSet<RowGraphEdge>();

	public boolean addNode(RowGraphNode node) {
		ArrayList<RowGraphNode> list = nodes.get(node.getTable());
		if (list==null) {
			list=new ArrayList<RowGraphNode>();
			nodes.put(node.getTable(),list);
		}
		if (list.contains(node)) return false;
		list.add(node);
		return true;
	}
	
	public void addEdge(RowGraphEdge edge) {
		this.edges.add(edge);
	}
	
	public Map<Table,ArrayList<RowGraphNode>> getNodeMap() {
		return this.nodes;
	}
	
}
