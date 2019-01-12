package es.evadell.db2etl.model;

import java.util.ArrayList;
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
	private List<Column> columns = new ArrayList<Column>();
	private List<Relation> childRelations = new ArrayList<Relation>();
	private List<Relation> parentRelations = new ArrayList<Relation>();
	
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

	public Column getColumn(String key) {
		Optional<Column> oc = columns.stream().filter(s->s.getName().equals(key)).findFirst();
		if (oc.isPresent()) return oc.get();
		else return null;
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

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Table) {
			Table other = (Table) obj;
			return this.getCreator().equals(other.getCreator()) && this.getName().equals(other.getName());
		}
		return super.equals(obj);
	}

	public List<Relation> getParentRelations() {
		return parentRelations;
	}

	public void setParentRelations(List<Relation> parentRelations) {
		this.parentRelations = parentRelations;
	}
}
 