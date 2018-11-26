package es.evadell.db2etl.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Model {

	private Logger log = LoggerFactory.getLogger(Model.class);
	
	private List<Table> tablas;
	private List<Relation> rels;

	public List<Relation> getParentRels(String tableName) {
		return rels.stream().filter(s -> s.getChildTable().equals(tableName)).collect(Collectors.toList());
	}

	public List<Relation> getChildRels(String tableName) {
		return rels.stream().filter(s -> s.getParentTable().equals(tableName)).collect(Collectors.toList());
	}

	public List<Table> getTablas() {
		return tablas;
	}

	public void setTablas(List<Table> tablas) {
		this.tablas = tablas;
	}
	
	public Table getTable(String creator, String name) {
		Optional<Table> t = this.tablas.stream().filter(s->s.getCreator().equals(creator) && s.getName().equals(name)).findFirst();
		if (t.isPresent()) return t.get();
		else return null;
	}

	public List<Relation> getRels() {
		return rels;
	}

	public void setRels(List<Relation> rels) {
		this.rels = rels;
	}

	public static Model loadModel(Connection conn, String creator, String tableName) throws SQLException {
		Model model = new Model();
		Table table = getOrLoadTable(conn, model, creator, tableName);
		String cmd = "SELECT RELNAME, CREATOR, TBNAME FROM SYSIBM.SYSRELS WHERE REFTBCREATOR = ? AND REFTBNAME = ? ORDER BY A.RELNAME";
		try (PreparedStatement statement = conn.prepareStatement(cmd)) {
			statement.setString(0, creator);
			statement.setString(1, tableName);
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				String relName = rs.getString(0);
				String childTableCreator = rs.getString(1);
				String childTableName = rs.getString(2);
				Relation rel = new Relation();
				rel.setRelName(relName);
				rel.setParentTable(table);
				rel.setChildTable(getOrLoadTable(conn, model, childTableCreator, childTableName));
				table.getChildRelations().add(rel);
			}
		}
		cmd = "SELECT COLNAME, COLNO FROM SYSIBM.SYSFOREIGNKEYS WHERE RELNAME = ? ORDER BY COLNO";
		for(Relation rel:table.getChildRelations()) {
			
		}
		return model;
	}

	private static Table getOrLoadTable(Connection conn, Model model, String creator, String name) throws SQLException {
		Table t = model.getTable(creator, name);
		if (t==null) {
			t = loadTable(conn, creator, name);
			model.getTablas().add(t);
		}
		return t;
	}

	private static Table loadTable(Connection conn, String creator, String tableName) throws SQLException {
		Table table;
		String cmd = "select REMARKS, LABEL from SYSIBM.SYSTABLES where CREATOR=? AND TABLENAME=?";
		try (PreparedStatement statement = conn.prepareStatement(cmd)) {
			statement.setString(0, creator);
			statement.setString(1, tableName);
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				table = new Table();
				table.setCreator(creator);
				table.setName(tableName);
				table.setRemarks(rs.getString(0));
				table.setLabel(rs.getString(1));
				return table;
			}
		}
		return null;
	}

}
