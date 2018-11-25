package es.bancamarch.db2etl.model;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Model {

	private Map<String,Table> tablas;
	private List<Relation> rels;
	
	
	public List<Relation> getParentRels(String tableName) {
		return rels.stream().filter(s->s.getChildTable().equals(tableName)).collect(Collectors.toList());
	}
	
	public Table getTable(String tableName) {
		return tablas.get(tableName);
	}
	
	public Map<String, Table> getTablas() {
		return tablas;
	}
	public void setTablas(Map<String, Table> tablas) {
		this.tablas = tablas;
	}
	public List<Relation> getRels() {
		return rels;
	}
	public void setRels(List<Relation> rels) {
		this.rels = rels;
	}
	
	
	
}
