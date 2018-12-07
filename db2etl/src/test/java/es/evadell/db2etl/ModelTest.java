package es.evadell.db2etl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import es.evadell.db2etl.model.Model;

public class ModelTest {
	Connection conn;

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		ModelTest test = new ModelTest();
		test.conn = getConnection();
		test.run();
	}
	
	private void run() throws SQLException {
		//Model model = Model.loadModel(conn, "GLSQL", "AURATD");
		//Model model = Model.loadModel(conn, "GLSQL", "AURGRP");
		Model model = Model.loadModel(conn, "GLSQL", "SERTRA");
		String dot = Dot.generateDot(model);
		System.out.println(dot);
		conn.close();
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
