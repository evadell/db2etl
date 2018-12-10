package es.evadell.db2etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;

import es.evadell.db2etl.model.Model;
import es.evadell.db2etl.model.Relation;
import es.evadell.db2etl.model.Table;

public class Etl {
	private Model model;
	private Map<String,List<List<Object>>> rowTree = new HashMap<String, List<List<Object>>>();
	private Connection srcConn;
	private Connection dstConn;
	
	public void getRowTree(String creator, String tableName, List<Pair<String, Object>> values) throws Exception {
		Table table = model.getTable(creator, tableName);
		
		List<List<Object>> rset = select(srcConn, table, values);
		
		getRowsetAscendantRows(table, rset, null);
		getRowsetDescendantRows(table, rset);
		
	}
	
	private void getDescentantRows(Relation rel, List<Pair<String,Object>> values) throws SQLException, Exception {
		Table table = rel.getChildTable();
		List<List<Object>> rset = select(srcConn, table, values);
		storeRows(table, rset);
		getRowsetAscendantRows(table, rset, rel);
		getRowsetDescendantRows(table, rset);
	}

	private void getRowsetDescendantRows(Table table, List<List<Object>> results) throws SQLException, Exception {
		for(Relation r:table.getChildRelations()) {
			for(List<Object> row:results) {
				List<Pair<String,Object>> fkValues = getRowFkValues(r,row,false);
				getDescentantRows(r,fkValues);
			}
		}
	}

	private void storeRows(Table table, List<List<Object>> results) {
		List<List<Object>> rows = rowTree.get(table.getName());
		if (rows==null) rowTree.put(table.getName(),results);
		else rows.addAll(results);
	}
	
	private void getAscentantRows(Relation rel, List<Pair<String, Object>> values) throws SQLException, Exception {
		Table table = rel.getParentTable();
		List<List<Object>> rset = select(srcConn, table, values);
		storeRows(table,rset);
		getRowsetAscendantRows(table, rset, null);
	}

	private void getRowsetAscendantRows(Table table, List<List<Object>> results, Relation sourceRel) throws SQLException, Exception {
		for(Relation r:table.getParentRelations()) {
			for(List<Object> row:results) {
				if (r.equals(sourceRel)) continue;
				List<Pair<String,Object>> fkValues = getRowFkValues(r,row,true);
				getAscentantRows(r,fkValues);
			}
		}
	}

	private List<Pair<String, Object>> getRowFkValues(Relation r, List<Object> row, boolean isChildRow) {
		List<Pair<String, Object>> fk = new ArrayList<Pair<String,Object>>();
		if (!isChildRow) {
			Table tbl = r.getChildTable();
			for(Pair<String, String> fknp:r.getForeignKeyColumns()) {
				String childColName = fknp.getLeft();
				String parentColName = fknp.getRight();
				Object value = row.get(tbl.getColumn(parentColName).get().getColno());
				fk.add(Pair.of(childColName, value));
			}
		} else {
			Table tbl = r.getParentTable();
			for(Pair<String, String> fknp:r.getForeignKeyColumns()) {
				String childColName = fknp.getLeft();
				String parentColName = fknp.getRight();
				Object value = row.get(tbl.getColumn(childColName).get().getColno());
				fk.add(Pair.of(parentColName, value));
			}
		}
		return fk;
	}

	public void copyRows(String creator, String tableName, List<Pair<String, Object>> values) throws Exception {
		Table table = model.getTable(creator, tableName);
		
		List<List<Object>> results = select(srcConn, table, values);
		rowTree.put(tableName, results);
		for(List<Object> row:results) {
			insertRow(table,row);
		}
		//checkParentDependencies(table,results);
		
		
	}

	private void insertRow(Table table, List<Object> row) {
		//copyChildRows(table,row);
		
	}

	private void copyChildRows(Table table, List<Object> row) {
		List<Relation> parentRels = model.getChildRels(table.getName());
		for(Relation r:parentRels) {
			Table childTable = r.getChildTable();
			List<Pair<String, Object>> fk = getFkValues(table,row,r);
		}
		
	}

	private List<Pair<String, Object>> getFkValues(Table table, List<Object> row, Relation rel) {
		Stream<String> parentPkColNames = rel.getForeignKeyColumns().stream().map(p->p.getLeft());
		
		return null;
	}

	private static List<List<Object>> select(Connection conn, Table table, List<Pair<String, Object>> values) throws Exception, SQLException {
		List<List<Object>> results;
		results = new ArrayList<List<Object>>();

		PreparedStatement selectStmt = QueryBuilder.buildSelectStatement(conn, table, values);
		ResultSet resulset = selectStmt.executeQuery();
		while (resulset.next()) {
			ArrayList<Object> r = new ArrayList<Object>();
			results.add(r);
			for(int i=0;i<table.getColumns().size();i++) {
				String type = table.getColumns().get(i).getColtype();
				switch (type) {
				case "CHAR":
				case "VARCHAR":
					r.add(resulset.getString(i));
					break;
				case "DECIMAL":
					r.add(resulset.getBigDecimal(i));
					break;
				case "INTEGER":
				case "SMALLINT":
					r.add(resulset.getInt(i));
					break;
				case "TIME":
					r.add(resulset.getTime(i));
					break;
				case "TIMESTMP":
					r.add(resulset.getTimestamp(i));
					break;
				default:
					throw new Exception("Tipo de campo no soportado");
				}
			}
		}
		return results;
	}
	

	private void checkParentDependencies(Table table, List<List<Object>> results) {
		for(List<Object> srcCurrentRow:results) {
			List<Relation> parentRels = model.getParentRels(table.getName());
			for(Relation r:parentRels) {
				Table parentTable = r.getParentTable();
				checkExistance(dstConn, parentTable, getFkValues(r,srcCurrentRow));
			}
		}
	}





	private void checkExistance(Connection dstConn2, Table parentTable, Object fkValues) {
		// TODO Auto-generated method stub
		
	}

	private Object getFkValues(Relation r, List<Object> srcCurrentRow) {
		// TODO Auto-generated method stub
		return null;
	}
}