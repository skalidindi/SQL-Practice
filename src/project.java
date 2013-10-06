import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;


public class project {

	public static void main(String[] args) throws IOException {		
		Connection con = null;

		try {
			// Get the connection to the database
			FileWriter out = new FileWriter(args[3]); 
			DriverManager.registerDriver(new SQLServerDriver());
			String url = "jdbc:sqlserver://ACS-CSEB-SRV.ucsd.edu:1433;databaseName="
					+ args[0];
			con = DriverManager.getConnection(url, args[0], args[1]);

			// Query the Flights table
			Statement stmt = con.createStatement();
			/*stmt.executeUpdate("DROP TABLE T");
			stmt.executeUpdate("DROP TABLE DELTA");
			stmt.executeUpdate("DROP TABLE T_OLD");
			stmt.executeUpdate("DROP TABLE TClosure");
			stmt.executeUpdate("DROP TABLE G");*/
			
			stmt.executeUpdate("SELECT * INTO G FROM " + args[2]);
			stmt.executeUpdate("SELECT * INTO T FROM G");
			stmt.executeUpdate("SELECT * INTO DELTA FROM G");
			
			stmt.executeUpdate("CREATE TABLE TClosure (origin char(32), destination char(32), stops integer NOT NULL DEFAULT(0))");
			
			ResultSet rset = stmt.executeQuery("SELECT * from DELTA");
			boolean isDeltaEmpty = rset.next();
			int counter = 0;
			
			stmt.executeUpdate("INSERT INTO TClosure SELECT x.origin AS origin, x.destination AS destination, " +
					+ counter + "AS stops FROM DELTA x");
			
			// Do Semi-Naive Algorithmn
			while (isDeltaEmpty) {
				// T_old = T
				stmt.executeUpdate("SELECT * INTO T_OLD FROM T");
				
				// Update T
				stmt.executeUpdate("DROP TABLE T");
				stmt.executeUpdate("SELECT * INTO T FROM ((SELECT * FROM T_OLD) " +
						" UNION (SELECT x.origin, y.destination" + 
					" FROM G x, DELTA y WHERE x.destination = y.origin)) AS tmp");
				
				// Update delta
				stmt.executeUpdate("DROP TABLE DELTA");
				stmt.executeUpdate("SELECT * INTO DELTA FROM ((SELECT * FROM T) " +
						" EXCEPT (SELECT * FROM T_OLD)) AS tmp");
				counter++;
				// Update t_closure
				stmt.executeUpdate("INSERT INTO TClosure SELECT x.origin AS origin, x.destination AS destination, " +
						+ counter + "AS stops FROM DELTA x");
				
				stmt.executeUpdate("DROP TABLE T_OLD");
				// update resultSet
				rset = stmt.executeQuery("SELECT * from DELTA");
				isDeltaEmpty = rset.next();
			}
			
			// Write to file
			rset = stmt.executeQuery("SELECT * from TClosure WHERE origin <> destination ORDER BY origin, destination");
			out.write("Origin\t\t\t");
			out.write("Destination\t\t\t");
			out.write("Stops");
			out.write("\n");
			out.write("==========================================================================");
			out.write("\n");

			while (rset.next()) {
				out.write(rset.getString("Origin") + "\t");
				out.write(rset.getString("Destination") + "\t");
				out.write(rset.getInt("stops") + "\t");
				out.write("\n");
			}
			
			stmt.executeUpdate("DROP TABLE DELTA");
			stmt.executeUpdate("DROP TABLE T");
			stmt.executeUpdate("DROP TABLE TClosure");
			stmt.executeUpdate("DROP TABLE G");
			
			out.flush();
			out.close();
			// close the result set, statement
			//rset.close();
			stmt.close();
		} catch (SQLException e) {
			throw new RuntimeException("There was a problem!", e);
		} finally {
			// I have to close the connection in the "finally" clause otherwise
			// in case of an exception i would leave it open.
			try {
				if (con != null)
					con.close();
			} catch (SQLException e) {
				throw new RuntimeException(
						"Help! I could not close the connection!!!", e);
			}
		}
	}
}
