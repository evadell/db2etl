package es.bancamarch.db2etl.model;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Table {
	private String name;
	private String dbName;
	private String remarks;
	private String label;
	private List<Column> columns;	
	
	public List<Column> getPrimaryKey() {
		return columns.stream().filter(s->s.getKeySeq()>0).sorted(Comparator.comparingInt(Column::getKeySeq)).collect(Collectors.toList());
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getRemarks() {
		return remarks;
	}

	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

	public Optional<Column> getColumn(String key) {
		return columns.stream().filter(s->s.getName().equals(key)).findFirst();
	}


}
