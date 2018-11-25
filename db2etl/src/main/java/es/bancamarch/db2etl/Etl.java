package es.bancamarch.db2etl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import es.bancamarch.db2etl.model.Model;
import es.bancamarch.db2etl.model.Relation;
import es.bancamarch.db2etl.model.Table;

public class Etl {
	private Model model;
	private Connection srcConn;
	private Connection dstConn;
	
	
	public void copyRow(String tableName, List<Pair<String, Object>> values) throws Exception {
		Table table = model.getTablas().get(tableName);
		
		List<List<Object>> results = select(srcConn, table, values);
		//checkParentDependencies(table,results);
		
		
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
				String parentTableName = r.getParentTable();
				Table parentTable = model.getTablas().get(parentTableName);
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