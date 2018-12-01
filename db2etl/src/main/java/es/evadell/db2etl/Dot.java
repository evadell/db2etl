package es.evadell.db2etl;

import es.evadell.db2etl.model.Model;
import es.evadell.db2etl.model.Relation;
import es.evadell.db2etl.model.Table;

public class Dot {
	public static String generateDot(Model model) {
		StringBuilder strb = new StringBuilder();
		strb.append("strict digraph grafico {\n");
		for (Table t : model.getTablas()) {
			String id = t.getCreator()+"."+t.getName();
				strb.append(String.format("%1$s [id=%1$s, comment=\"%2$s\"];\n", id, t.getRemarks()));
				for (Relation rel : t.getChildRelations()) {
					String id1 = rel.getChildTable().getCreator()+"."+rel.getChildTable().getName();
					strb.append(String.format("%1$s -> %2$s;\n", id, id1 ));
				}
		}
		strb.append("}");
		return strb.toString();
	}
}
