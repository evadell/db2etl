package es.evadell.db2etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;

import es.evadell.db2etl.model.Model;
import es.evadell.db2etl.model.Relation;
import es.evadell.db2etl.model.Table;

public class Etl {
	private Model model;
	private Connection srcConn;
	private Connection dstConn;
	
	
	public void copyRows(String creator, String tableName, List<Pair<String, Object>> values) throws Exception {
		Table table = model.getTable(creator, tableName);
		
		List<List<Object>> results = select(srcConn, table, values);
		for(List<Object> row:results) {
			insertRow(table,row);
		}
		//checkParentDependencies(table,results);
		
		
	}

	private void insertRow(Table table, List<Object> row) {
		copyChildRows(table,row);
		
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