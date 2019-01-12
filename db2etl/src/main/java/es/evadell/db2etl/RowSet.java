package es.evadell.db2etl;

import java.util.ArrayList;
import java.util.List;

import es.evadell.db2etl.model.Table;

public class RowSet {

	private List<ColValues> rows = new ArrayList<ColValues>();

	public List<ColValues> getRows() {
		return rows;
	}

	public void setRows(List<ColValues> rows) {
		this.rows = rows;
	}

	public void add(ColValues cvs) {
		this.rows.add(cvs);

	}
}
