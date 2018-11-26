package es.evadell.db2etl.model;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Table {
	private String name;
	private String creator;
	private String remarks;
	private String label;
	private List<Column> columns;
	private List<Relation> childRelations;
	
	public List<Column> getPrimaryKey() {
		return columns.stream().filter(s->s.getKeySeq()>0).sorted(Comparator.comparingInt(Column::getKeySeq)).collect(Collectors.toList());
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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

	public String getCreator() {
		return creator;
	}

	public void setCreator(String creator) {
		this.creator = creator;
	}

	public List<Relation> getChildRelations() {
		return childRelations;
	}

	public void setChildRelations(List<Relation> childRelations) {
		this.childRelations = childRelations;
	}


}
