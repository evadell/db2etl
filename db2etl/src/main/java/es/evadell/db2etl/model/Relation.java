package es.evadell.db2etl.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

public class Relation {

	private Table childTable;
	private String relName;
	private Table parentTable;
	private List<Pair<String,String>> foreignKeyColumns = new ArrayList<Pair<String,String>>();
	
	public Table getChildTable() {
		return childTable;
	}
	public void setChildTable(Table childTable) {
		this.childTable = childTable;
	}
	public String getRelName() {
		return relName;
	}
	public void setRelName(String relName) {
		this.relName = relName;
	}
	public Table getParentTable() {
		return parentTable;
	}
	public void setParentTable(Table parentTable) {
		this.parentTable = parentTable;
	}
	public List<Pair<String, String>> getForeignKeyColumns() {
		return foreignKeyColumns;
	}
	public void setForeignKeyColumns(List<Pair<String, String>> foreignKeyColumns) {
		this.foreignKeyColumns = foreignKeyColumns;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Relation) {
			Relation other = (Relation) obj;
			return this.getRelName().equals(other.getRelName());
		}
		return super.equals(obj);
	}
	
	
}
