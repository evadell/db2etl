package es.bancamarch.db2etl.model;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

public class Relation {

	private String childTable;
	private String relName;
	private String parentTable;
	private List<Pair<String,String>> foreignKeyColumns;
	
	public String getChildTable() {
		return childTable;
	}
	public void setChildTable(String childTable) {
		this.childTable = childTable;
	}
	public String getRelName() {
		return relName;
	}
	public void setRelName(String relName) {
		this.relName = relName;
	}
	public String getParentTable() {
		return parentTable;
	}
	public void setParentTable(String parentTable) {
		this.parentTable = parentTable;
	}
	public List<Pair<String, String>> getForeignKeyColumns() {
		return foreignKeyColumns;
	}
	public void setForeignKeyColumns(List<Pair<String, String>> foreignKeyColumns) {
		this.foreignKeyColumns = foreignKeyColumns;
	}
	
	
}
