package es.evadell.db2etl;

import es.evadell.db2etl.model.Table;

public class RowGraphNode {

	private Table table;
	private ColValues values;

	public RowGraphNode(Table table, ColValues values) {
		super();
		this.table = table;
		this.values = values;
	}

	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	public ColValues getValues() {
		return values;
	}

	public void setValues(ColValues values) {
		this.values = values;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof RowGraphNode) {
			RowGraphNode other = (RowGraphNode) obj;
			return this.table.equals(other.getTable()) && this.values.equals(other.values);
		}
		return false;
	}
}
