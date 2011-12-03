/*
 * author: Taranov V.V.
 * mailto: taranov.vv@gmail.com
 */
package com.javatask.second;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

// tested with postgres, once runned on derby)
public class JDBCTest {

	private Connection con = null;
	private Statement stmt = null;
	private ResultSet rs = null;

	public JDBCTest() {
//		connect("demodb", "postgresql", "postgres", "pass");
		connect("demodb", "derby", "", "");

		initSQL();
	}

	public void closeConnection() {
		disconnect();
	}

	public void testStatement() {
		System.out.println("\n\n--------  Statement test  --------");

		String sqlQuery = "select * from BANDS";
		try {
			ResultSet rs = stmt.executeQuery(sqlQuery);

			System.out.println("BANDS:");
			while (rs.next()) {
				int id = rs.getInt("ID");
				String name = rs.getString("NAME");
				String genre = rs.getString("GENRE");
				System.out.println(" id: '" + id + "'\tname: '" + name + "'\tgenre: '" + genre + "'");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void testPreparedStatement() {
		System.out.println("\n\n--------  Prepared statement test  --------");

		String query = "select * from STARS where BAND_ID=(select ID from BANDS where NAME=?)";

		try {
			PreparedStatement ps = con.prepareStatement(query);

			String[] bands = { "Metallica", "Scorpions", "Nightwish" };
			for (String band : bands) {
				System.out.println("\nThe members of the '" + band + "' band:");

				ps.setString(1, band);
				ResultSet rs = ps.executeQuery();

				while (rs.next()) {
					System.out.println(" - " + rs.getString("NAME") + "\t(" + rs.getString("ROLE") + ")");
				}
			}
			
			ps.close();

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public void testTransaction() {
		System.out.println("\n\n--------  Transaction test  --------");
		
		Statement st = null;
		try {
			con.setAutoCommit(false);
			st = con.createStatement();
			
			String addDeepPurple = "insert into BANDS values (4, 'DeepPurple', 'Hard rock, heavy metal, blues rock, progressive rock')";
			
			st.executeUpdate(addDeepPurple);
			
			// add stars
			st.executeUpdate("insert into STARS values (100, 4, 'Ian Paice', 'drummer' )");
			st.executeUpdate("insert into STARS values (101, 4, 'Roger Glover', 'bassist, keyboardist, songwriter, record producer' )");
			st.executeUpdate("insert into STARS values (102, 4, 'Ian Gillan', 'vocalist, songwriter' )");
			st.executeUpdate("insert into STARS values (103, 4, 'Steve Morse', 'guitarist' )");
			st.executeUpdate("insert into STARS values (104, 4, 'Don Airey', 'keyboardist' )");
			
			con.commit();
			System.out.println("Band 'Deep Purple' is added successfully");
			
		} catch (SQLException e) {
			System.out.println("Error: rollback changes");
			e.printStackTrace();
			try {
				con.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
	}

	private void connect(String database, String driver, String username, String password) {
		try {
			if (driver.equals("postgresql")) {
				Class.forName("org.postgresql.Driver");
				con = DriverManager.getConnection("jdbc:postgresql:" + database, username, password);
			} else if (driver.equals("derby")) {
				// for creating data base 
//				con = DriverManager.getConnection("jdbc:derby://localhost:1527/db;create=true");
				con = DriverManager.getConnection("jdbc:derby://localhost:1527/db");
			}
			
			stmt = con.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
	}

	private void disconnect() {
		try {
			if (rs != null) {
				rs.close();
			}
			if (stmt != null) {
				stmt.close();
			}
			if (con != null) {
				con.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void initSQL() {
		System.out.print("creating table...");

		String createBandTable = "create table BANDS ( ID	int PRIMARY KEY, " + "NAME	varchar(25), "
				+ "GENRE	varchar(100))";
		String createStarTable = "create table STARS ( " + " ID	int PRIMARY KEY, " + " BAND_ID	int,"
				+ " NAME   	varchar(50)," + " ROLE   	varchar(100) )";

		String addMetallica = "insert into BANDS values (1, 'Metallica', 'Heavy metal, thrash metal, hard rock, speed metal')";
		String addScorpions = "insert into BANDS values (2, 'Scorpions', 'Heavy metal, hard rock')";
		String addNightwish = "insert into BANDS values (3, 'Nightwish', 'Symphonic metal, gothic metal, power metal')";

		try {
//			 stmt.executeUpdate("drop table BAND");
//			 stmt.executeUpdate("drop table STARS");

			stmt.executeUpdate(createBandTable);
			stmt.executeUpdate(createStarTable);

			stmt.executeUpdate(addMetallica);
			stmt.executeUpdate(addScorpions);
			stmt.executeUpdate(addNightwish);

			// Metallica
			stmt.executeUpdate("insert into STARS values (1, 1, 'James Hetfield', 'rhythm guitarist, co-founder, main songwriter, lead vocalist' )");
			stmt.executeUpdate("insert into STARS values (2, 1, 'Lars Ulrich', 'drummer' )");
			stmt.executeUpdate("insert into STARS values (3, 1, 'Kirk Hammett', 'lead guitarist, songwriter' )");
			stmt.executeUpdate("insert into STARS values (4, 1, 'Robert Trujillo', 'bassist' )");

			// Scorpions
			stmt.executeUpdate("insert into STARS values (5, 2, 'Rudolf Schenker', 'guitarist' )");
			stmt.executeUpdate("insert into STARS values (6, 2, 'Klaus Meine', 'lead vocalist, occasional rhythm guitarist' )");
			stmt.executeUpdate("insert into STARS values (7, 2, 'Matthias Jabs', 'guitarist and songwriter' )");
			stmt.executeUpdate("insert into STARS values (8, 2, 'James Kottak', 'drummer' )");
			stmt.executeUpdate("insert into STARS values (9, 2, 'Paweł Mąciwoda', 'bassist' )");

			// Nightwish
			stmt.executeUpdate("insert into STARS values (10, 3, 'Tarja Turunen', 'singer-songwriter, composer' )");
			stmt.executeUpdate("insert into STARS values (11, 3, 'Tuomas Holopainen', 'composer' )");
			stmt.executeUpdate("insert into STARS values (12, 3, 'Marco Hietala', 'vocalist and bassist' )");
			stmt.executeUpdate("insert into STARS values (13, 3, 'Anette Olzon', 'lead vocalist' )");
			stmt.executeUpdate("insert into STARS values (14, 3, 'Jukka Nevalainen', 'drummer' )");
			stmt.executeUpdate("insert into STARS values (15, 3, 'Emppu Vuorinen', 'guitarist' )");
			
		} catch (SQLException e) {
			System.out.println("\nError: table can't be created(may be table is already exists)");
			//e.printStackTrace();
			return;
		}
		System.out.println("\t[ok]");
	}
}
