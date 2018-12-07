package es.evadell.db2etl;

import java.util.List;

import es.evadell.db2etl.model.Table;

public class RowSet {
	private Table table;
	
	private List<List<Object>> rows;
	public Table getTable() {
		return table;
	}
	public void setTable(Table table) {
		this.table = table;
	}
	public List<List<Object>> getRows() {
		return rows;
	}
	public void setRows(List<List<Object>> rows) {
		this.rows = rows;
	}
}
