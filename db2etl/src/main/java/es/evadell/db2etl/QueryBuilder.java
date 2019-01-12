package es.evadell.db2etl;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import es.evadell.db2etl.model.Column;
import es.evadell.db2etl.model.Model;
import es.evadell.db2etl.model.Table;

public class QueryBuilder {
	
	public static PreparedStatement buildSelectStatement(Connection conn, Table table, ColValues values) throws Exception {
				
		String cmd = buildSelectCmd(table, values);
		PreparedStatement statement = conn.prepareStatement(cmd);
		int i=0;
		for(ColValue c:values.getList()) {
			String type = c.getCol().getColtype();
			Object value=c.getValue();
			setParameterValue(statement, i, type, value);
			i++;
		}
		return statement;
	}
	
	public static PreparedStatement buildInsertStatement(Model model, Connection conn, String creator, String tableName, List<Pair<String, Object>> values) throws Exception {
		Table table = model.getTable(creator, tableName);
		
		String cmd = buildInsertCmd(table);
		PreparedStatement statement = conn.prepareStatement(cmd);
		int i=0;
		for(Pair<String, Object> c:values) {
			String type = table.getColumn(c.getKey()).getColtype();
			Object value=c.getValue();
			setParameterValue(statement, i, type, value);
			i++;
		}
		return statement;
	}

	private static String buildInsertCmd(Table table) {
		StringBuilder strb = new StringBuilder("insert into ").append(table.getName()).append("(");
		int i=0;
		for(Column c:table.getColumns()) {
			if (i++>0) strb.append(", ");
			strb.append(c.getName());
		}
		strb.append(") values (");
		for(i=0;i<table.getColumns().size();i++) {
			if (i>0) strb.append(", ");
			strb.append("?");
		}
		return strb.toString();
	}

	private static void setParameterValue(PreparedStatement statement, int index, String type, Object value)
			throws SQLException, Exception {
		/* Tipos usados en prod
		 * BLOB    
		 CHAR    
		 CLOB    
		 DECIMAL 
		 INTEGER 
		 ROWID   
		 SMALLINT
		 TIME    
		 TIMESTMP
		 VARCHAR
		 */
		switch (type) {
		case "CHAR":
		case "VARCHAR":
			statement.setString(index, (String)value);
			break;
		case "DECIMAL":
			statement.setBigDecimal(index, (BigDecimal)value);
			break;
		case "INTEGER":
		case "SMALLINT":
			statement.setInt(index, (Integer)value);
			break;
		case "TIME":
			statement.setTime(index, (Time)value);
			break;
		case "TIMESTMP":
			statement.setTimestamp(index, (Timestamp)value);
			break;
		default:
			throw new Exception("Tipo de campo no soportado");
		}
	}

	private static String buildSelectCmd(Table table, ColValues pk) {
		
		StringBuilder strb = new StringBuilder("select ");
		int i=0;
		for(Column c:table.getColumns()) {
			if (i>0) strb.append(", ");
			strb.append(c.getName());
			i++;
		}
		strb.append(" from ").append(table.getName()).append(" where ");
		i=0;
		for(ColValue c:pk.getList()) {
			if (i++>0) strb.append(" and ");
			strb.append(c.getCol().getName()).append("=?");
		}
		return strb.toString();
	}

}
