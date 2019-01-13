package es.evadell.db2etl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import es.evadell.db2etl.model.Model;

public class RowGraphTest {

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		RowGraphTest test = new RowGraphTest();
		
		try (Connection conn = RowGraphTest.getConnection()) {
			Model model = Model.loadModel(conn, "GLSQL", "MPRCON");
			Etl etl = new Etl(model, conn, null);
			
			ColValues values = new ColValues();
			
			//etl.getRowGraph("GLSQL", "MPRCON", values);
		}
	}
	
	public static Connection getConnection() throws ClassNotFoundException, SQLException {
		String constr = System.getProperty("db2etl.modeltest.connectionString");
		String user = System.getProperty("db2etl.modeltest.user");
		String pass = System.getProperty("db2etl.modeltest.pwd");

		Class.forName("com.ibm.db2.jcc.DB2Driver");
		Connection conn = DriverManager.getConnection(constr, user, pass);
		
		return conn;
	}

}
