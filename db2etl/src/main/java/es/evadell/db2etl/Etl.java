package es.evadell.db2etl;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.tuple.Pair;

import es.evadell.db2etl.model.Column;
import es.evadell.db2etl.model.Model;
import es.evadell.db2etl.model.Relation;
import es.evadell.db2etl.model.Table;

public class Etl {
	private Model model;
	private Connection srcConn;
	private Connection dstConn;

	public Etl(Model model, Connection srcConn, Connection dstConn) {
		super();
		this.model = model;
		this.srcConn = srcConn;
		this.dstConn = dstConn;
	}

	public RowGraph getRowGraph(String creator, String tableName, ColValues values) throws SQLException, Exception {
		RowGraph rt = new RowGraph();
		Table table = model.getTable(creator, tableName);

		RowSet rset = select(srcConn, table, values);

		for (ColValues row : rset.getRows()) {
			RowGraphNode rowGraphNode = new RowGraphNode(table, row);
			rt.addNode(rowGraphNode);
			getNodeAscendantRows(rowGraphNode, rt);
			getNodeDescendantRows(rowGraphNode, rt);
		}
		return rt;
	}

	private void getNodeAscendantRows(RowGraphNode childNode, RowGraph rt) throws SQLException, Exception {
		ColValues row = childNode.getValues();
		Table table = childNode.getTable();
		for (Relation r : table.getParentRelations()) {
			Table parentTable = r.getParentTable();
			ColValues fkValues = getRowFkValues(r, row, true);
			RowSet rset = select(srcConn, parentTable, fkValues);
			for (ColValues prow : rset.getRows()) {
				RowGraphNode rowGraphNode = new RowGraphNode(table, prow);
				if (rt.addNode(rowGraphNode))
					getNodeAscendantRows(rowGraphNode, rt);
				RowGraphEdge e = new RowGraphEdge();
				e.setRel(r);
				e.setParent(rowGraphNode);
				e.setChild(childNode);
				rt.addEdge(e);
			}
		}
	}

	private void getNodeDescendantRows(RowGraphNode parentNode, RowGraph rt) throws SQLException, Exception {
		ColValues row = parentNode.getValues();
		Table table = parentNode.getTable();
		for (Relation r : table.getChildRelations()) {
			Table childTable = r.getChildTable();
			ColValues fkValues = getRowFkValues(r, row, false);
			RowSet rset = select(srcConn, childTable, fkValues);
			for (ColValues crow : rset.getRows()) {
				RowGraphNode rowGraphNode = new RowGraphNode(table, crow);
				if (rt.addNode(rowGraphNode))
					getNodeDescendantRows(rowGraphNode, rt);
				RowGraphEdge e = new RowGraphEdge();
				e.setRel(r);
				e.setParent(parentNode);
				e.setChild(rowGraphNode);
				rt.addEdge(e);
			}
		}
	}

	private ColValues getRowFkValues(Relation r, ColValues row, boolean isChildRow) {
		ColValues fk = new ColValues();
		if (isChildRow) {
			Table tbl = r.getParentTable();
			for (Pair<String, String> fknp : r.getForeignKeyColumns()) {
				String childColName = fknp.getLeft();
				Object value = row.get(childColName);

				String parentColName = fknp.getRight();
				Column parentCol = tbl.getColumn(parentColName);
				fk.add(new ColValue(parentCol, value));
			}
		} else {
			Table tbl = r.getChildTable();
			for (Pair<String, String> fknp : r.getForeignKeyColumns()) {
				String parentColName = fknp.getRight();
				Object value = row.get(parentColName);

				String childColName = fknp.getLeft();
				Column childCol = tbl.getColumn(childColName);
				fk.add(new ColValue(childCol, value));
			}
		}
		return fk;
	}

	private static RowSet select(Connection conn, Table table, ColValues values) throws Exception, SQLException {
		RowSet ros = new RowSet();

		PreparedStatement selectStmt = QueryBuilder.buildSelectStatement(conn, table, values);
		ResultSet rs = selectStmt.executeQuery();
		while (rs.next()) {
			ColValues cvs = new ColValues();
			for (int i = 0; i < table.getColumns().size(); i++) {
				Column col = table.getColumns().get(i);
				String type = col.getColtype();
				Object val;
				switch (type) {
				case "CHAR":
				case "VARCHAR":
					val = rs.getString(i);
					break;
				case "DECIMAL":
					val = rs.getBigDecimal(i);
					break;
				case "INTEGER":
				case "SMALLINT":
					val = rs.getInt(i);
					break;
				case "TIME":
					val = rs.getTime(i);
					break;
				case "TIMESTMP":
					val = rs.getTimestamp(i);
					break;
				default:
					throw new Exception("Tipo de campo no soportado");
				}
				ColValue cv = new ColValue(col, val);
				cvs.add(cv);
			}
			ros.add(cvs);
		}
		return ros;
	}

	public void writeCsv(String path, RowGraph rg) throws IOException {
		for(Table t:rg.getNodeMap().keySet()) {
			Path p = Paths.get(path, t.getName().concat(".csv"));
			try (BufferedWriter writer = Files.newBufferedWriter(p);
				 CSVPrinter csv = new CSVPrinter(writer,CSVFormat.EXCEL.withFirstRecordAsHeader().withTrim())) {
				List<String> headerList = t.getColumns().stream().map(c->c.getName()).collect(Collectors.toList());
				csv.printRecord(headerList);
				for(RowGraphNode n:rg.getNodeMap().get(t)) {
					csv.printRecord(n.getValues().getList().stream().map(o->o.getValue().toString()).collect(Collectors.toList()));
				}				
			}
		}
		
	}

}