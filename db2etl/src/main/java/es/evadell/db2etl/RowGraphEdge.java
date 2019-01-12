package es.evadell.db2etl;

import es.evadell.db2etl.model.Relation;

public class RowGraphEdge {

	private Relation rel;
	private RowGraphNode parent;
	private RowGraphNode child;
	
	public Relation getRel() {
		return rel;
	}
	public void setRel(Relation rel) {
		this.rel = rel;
	}
	public RowGraphNode getParent() {
		return parent;
	}
	public void setParent(RowGraphNode parent) {
		this.parent = parent;
	}
	public RowGraphNode getChild() {
		return child;
	}
	public void setChild(RowGraphNode child) {
		this.child = child;
	}
	
	
}
