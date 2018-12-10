package es.evadell.db2etl;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import es.evadell.db2etl.model.Column;
import es.evadell.db2etl.model.Model;
import es.evadell.db2etl.model.Relation;
import es.evadell.db2etl.model.Table;

public class RowTreeUtils {
	
	public static void printRowTree(Model model, Map<String,List<List<Object>>> rowTree, OutputStreamWriter os) throws IOException {
		HashSet<Table> done = new HashSet<>();
		CSVPrinter csv = new CSVPrinter(os,CSVFormat.EXCEL);
		for(Table t:model.getTablas()) {
			traverse(model,rowTree,done,t,csv);
		}
	}
		

	private static void traverse(Model model, Map<String, List<List<Object>>> rowTree, HashSet<Table> done, Table table, CSVPrinter csv) throws IOException {
		if (done.contains(table)) return;
		else {
			for(Relation r:table.getParentRelations()) {
				traverse(model,rowTree,done,r.getParentTable(),csv);
			}
			printTableRows(rowTree,table,csv);
			done.add(table);
			for(Relation r:table.getChildRelations()) {
				traverse(model,rowTree,done,r.getChildTable(),csv);
			}
		}
		
	}


	private static void printTableRows(Map<String, List<List<Object>>> rowTree, Table table, CSVPrinter csv) throws IOException {
		List<List<Object>> rset = rowTree.get(table.getName());
		if (rset==null) return;
		csv.print(table.getName());
		csv.println();
		csv.printRecord(table.getColumns().stream().map(c->c.getName()));
		for(List<Object> row:rset) {
			csv.printRecord(row);
		}
		csv.println();
	}

}
