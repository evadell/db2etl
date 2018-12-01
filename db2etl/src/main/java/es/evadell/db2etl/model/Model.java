package es.evadell.db2etl.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
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
		Stack<Table> stack = new Stack<Table>();
		stack.add(table);
		Set<Table> visitedDescendants = new HashSet<Table>();
		while(!stack.isEmpty()) {
			Table tbl = stack.pop();
			if (visitedDescendants.contains(tbl)) continue;
			visitedDescendants.add(tbl);
			discoverChildRelations(conn, model, tbl);
			for(Relation rel:tbl.getChildRelations())
				stack.push(rel.getChildTable());
		}
		Set<Table> visitedAscendants = new HashSet<Table>();
		stack.addAll(visitedDescendants);
		while(!stack.isEmpty()) {
			Table tbl = stack.pop();
			if (visitedAscendants.contains(tbl)) continue;
			visitedAscendants.add(tbl);
			discoverParentRelations(conn, model, tbl);
			for(Relation rel:tbl.getParentRelations())
				stack.push(rel.getParentTable());
		}
		return model;
	}
	
	private static void discoverParentRelations(Connection conn, Model model, Table table)
			throws SQLException {
		String creator = table.getCreator(); 
		String tableName = table.getName();
		String cmd = "SELECT RELNAME, REFTBCREATOR, REFTBNAME, TBNAME"
				+ " FROM SYSIBM.SYSRELS"
				+ " WHERE CREATOR = ? AND TBNAME = ?"
				+ " ORDER BY A.RELNAME";
		try (PreparedStatement statement = conn.prepareStatement(cmd)) {
			statement.setString(0, creator);
			statement.setString(1, tableName);
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				String relName = rs.getString(0);
				String parentTableCreator = rs.getString(1);
				String parentTableName = rs.getString(2);
				Table parentTable = new Table();
				parentTable.setCreator(parentTableCreator);
				parentTable.setName(parentTableName);
				Relation rel = new Relation();
				rel.setRelName(relName);
				rel.setParentTable(table);
				rel.setParentTable(parentTable);
				if (!table.getParentRelations().contains(rel)) table.getParentRelations().add(rel);
			}
		}

		for(Relation rel:table.getParentRelations()) {
			Table parentTable = getOrLoadTable(conn, model, rel.getChildTable().getCreator(), rel.getChildTable().getName());
			rel.setParentTable(parentTable);
			if (rel.getForeignKeyColumns().isEmpty()) getRelationFKColumns(conn, creator, tableName, rel);
		}
	}
	
	
	private static void discoverChildRelations(Connection conn, Model model, Table table)
			throws SQLException {
		String creator = table.getCreator(); 
		String tableName = table.getName();
		String cmd = "SELECT RELNAME, CREATOR, TBNAME"
				+ " FROM SYSIBM.SYSRELS"  
				+ " WHERE REFTBCREATOR = ? AND REFTBNAME = ?"  
				+ " ORDER BY A.RELNAME";
		try (PreparedStatement statement = conn.prepareStatement(cmd)) {
			statement.setString(0, creator);
			statement.setString(1, tableName);
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				String relName = rs.getString(0);
				String childTableCreator = rs.getString(1);
				String childTableName = rs.getString(2);
				Table childTable = new Table();
				childTable.setCreator(childTableCreator);
				childTable.setName(childTableName);
				Relation rel = new Relation();
				rel.setRelName(relName);
				rel.setParentTable(table);
				rel.setChildTable(childTable);
				table.getChildRelations().add(rel);
			}
		}

		for(Relation rel:table.getChildRelations()) {
			Table childTable = getOrLoadTable(conn, model, rel.getChildTable().getCreator(), rel.getChildTable().getName());
			rel.setChildTable(childTable);
			childTable.getParentRelations().add(rel);
			if (rel.getForeignKeyColumns().isEmpty()) getRelationFKColumns(conn, creator, tableName, rel);
			
		}
	}

	private static void getRelationFKColumns(Connection conn, String creator, String tableName,	Relation rel) throws SQLException {
		String cmd;
		cmd = "SELECT "
				+ "	F.COLNAME childColName,"
				+ "	K.COLNAME parentColName"
				+ "FROM SYSIBM.SYSFOREIGNKEYS F"
				+ ", SYSIBM.SYSRELS R"
				+ ", SYSIBM.SYSINDEXES I"
				+ ", SYSIBM.SYSKEYS K"
				+ "WHERE "
				+ "	R.REFTBNAME=? AND R.REFTBCREATOR=? AND R.RELNAME=? AND"
				+ "	AND F.CREATOR = R.CREATOR AND F.TBNAME = R.TBNAME AND F.RELNAME = R.RELNAME"
				+ "	AND I.TBNAME = R.REFTBNAME AND I.CREATOR = R.REFTBCREATOR AND ( "
				+ "		( I.UNIQUERULE = 'P' AND R.IXNAME = '' ) OR "
				+ "		( I.UNIQUERULE = 'R' AND I.NAME = R.IXNAME AND I.CREATOR = R.IXOWNER)"
				+ "	)"
				+ "	AND I.NAME = K.IXNAME AND I.CREATOR = K.IXCREATOR"
				+ "	AND F.COLSEQ = K.COLSEQ"
				+ "ORDER BY F.COLSEQ "
				+ "WITH UR";
		try (PreparedStatement statement = conn.prepareStatement(cmd)) {
			statement.setString(0, creator);
			statement.setString(1, tableName);
			statement.setString(2, rel.getRelName());
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				String childColName = rs.getString(0);
				String parentColName = rs.getString(1);
				rel.getForeignKeyColumns().add(Pair.of(childColName, parentColName));
			}
		}
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
		Table table = null;
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
			}
		}
		cmd = "select NAME, COLNO, COLTYPE, LENGTH, SCALE, NULLS, REMARKS, DEF, KEYSEQ, LABEL from SYSIBM.SYSCOLUMNS where TBCREATOR=? AND TBNAME=?";
		try (PreparedStatement statement = conn.prepareStatement(cmd)) {
			statement.setString(0, creator);
			statement.setString(1, tableName);
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				Column col = new Column();
				col.setName(rs.getString(0));
				col.setColno(rs.getInt(1));
				col.setColtype(rs.getString(2));
				col.setLength(rs.getInt(3));
				col.setScale(rs.getInt(4));
				col.setNullable(rs.getString(5).charAt(0)=='Y');
				col.setRemarks(rs.getString(6));
				col.setDef(rs.getString(7).charAt(0));
				col.setKeySeq(rs.getInt(8));
				col.setLabel(rs.getString(9));
				table.getColumns().add(col);
			}
		}
		return table;

	}

}
