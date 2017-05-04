package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class CursorFetchPriorIT extends BaseHBaseManagedTimeIT {
	
    private static final String TABLE_NAME = "MARKET";
    protected static final Log LOG = LogFactory.getLog(CursorFetchPriorIT.class);

    public void createAndInitializeTestTable() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());

        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + TABLE_NAME +
                "(TICKER VARCHAR NOT NULL PRIMARY KEY, " +
                "PRICE FLOAT)");
        stmt.execute();

        synchronized (conn){
            conn.commit();
        }
        
        //Upsert test values into the test table
        Random rand = new Random();
        stmt = conn.prepareStatement("UPSERT INTO " + TABLE_NAME +
                "(TICKER, PRICE) VALUES (?,?)");
        int rowCount = 0;
        while(rowCount < 10){
            stmt.setString(1, "A"+rowCount);
            stmt.setFloat(2, rand.nextInt(501));
            stmt.execute();
            ++rowCount;
        }
        synchronized (conn){
            conn.commit();
        }
    }
    
    public void deleteTestTable() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        PreparedStatement stmt = conn.prepareStatement("DROP TABLE IF EXISTS " + TABLE_NAME);
        stmt.execute();
        synchronized (conn){
            conn.commit();
        }
    }
    
    public void createAndInitializeDetailsTable() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());

        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS DETAILS (ID INTEGER NOT NULL PRIMARY KEY, TICKER VARCHAR, LOCATION VARCHAR)");
        stmt.execute();
        synchronized (conn){
            conn.commit();
        }
        
        Statement statement = conn.createStatement();
        statement.executeUpdate("UPSERT INTO DETAILS VALUES (1, 'APPL', 'CA')");
        statement.executeUpdate("UPSERT INTO DETAILS VALUES (2, 'GOOG', 'CA')");
        statement.executeUpdate("UPSERT INTO DETAILS VALUES (3, 'GOOG', 'MA')");
        statement.executeUpdate("UPSERT INTO DETAILS VALUES (4, 'GOOG', 'NY')");
        statement.executeUpdate("UPSERT INTO DETAILS VALUES (5, 'MSFT', 'WA')");
        statement.executeUpdate("UPSERT INTO DETAILS VALUES (6, 'FB', 'MA')");
        statement.executeUpdate("UPSERT INTO DETAILS VALUES (7, 'MSFT', 'MA')");   
        synchronized (conn){
            conn.commit();
        }
    }
    
    public void deleteDetailsTable() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        PreparedStatement stmt = conn.prepareStatement("DROP TABLE IF EXISTS DETAILS");
        stmt.execute();
        synchronized (conn){
            conn.commit();
        }
    }
    
    @Test
	public void testFetchPriorOnCursorsSimple() throws SQLException {
    	try {
    		createAndInitializeTestTable();
    		String querySQL = "SELECT * FROM " + TABLE_NAME;
    		// Test actual cursor implementation
    		String cursorSQL = "DECLARE testCursor CURSOR FOR " + querySQL;
    		DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
    		cursorSQL = "OPEN testCursor";
    		DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
    		cursorSQL = "FETCH NEXT FROM testCursor";
    		ResultSet rs = DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).executeQuery();
    		int i = 0;
    		String rowID = "A" + i;
            while (rs.next()) {
    		    assertEquals(rowID, rs.getString(1));
            }
    		i++;
    		rowID = "A" + i;
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    		    assertEquals(rowID, rs.getString(1));
    		}
    		cursorSQL = "FETCH PRIOR FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).executeQuery();
            while (rs.next()) {
    		    assertEquals(rowID, rs.getString(1));
            }
    		i--;
    		rowID = "A" + i;
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    		    assertEquals(rowID, rs.getString(1));
    		}
    		rs.close();
    	} finally {
			DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
			deleteTestTable();
		}

	}
    
    @Test
	public void testFetchPriorOnCursorsComplex() throws SQLException {
    	try {
    		createAndInitializeDetailsTable();
    		String querySQL = "SELECT LOCATION FROM DETAILS GROUP BY LOCATION";

    		// Test actual cursor implementation
    		String cursorSQL = "DECLARE testCursor CURSOR FOR " + querySQL;
    		DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
    		cursorSQL = "OPEN testCursor";
    		DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
    		cursorSQL = "FETCH NEXT FROM testCursor";
    		ResultSet rs = DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).executeQuery();
    		String rowID = "CA";
            while (rs.next()) {
    		    assertEquals(rowID, rs.getString(1));
            }
    		rowID = "MA";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    		    assertEquals(rowID, rs.getString(1));
    		}
    		cursorSQL = "FETCH PRIOR FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).executeQuery();
    		rowID = "MA";
            while (rs.next()) {
    		    assertEquals(rowID, rs.getString(1));
            }
    		rowID = "CA";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    		    assertEquals(rowID, rs.getString(1));
    		}
    		rs.close();
    	} finally {
			DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
			deleteDetailsTable();
		}
	}
    
    @Test
    public void testFetchPriorOnCursorsCount() throws SQLException {
    	try {
    		createAndInitializeDetailsTable();
    		String querySQL = "SELECT LOCATION, COUNT(1) AS COUNT FROM DETAILS GROUP BY LOCATION";
    		// Test actual cursor implementation
    		String cursorSQL = "DECLARE testCursor CURSOR FOR " + querySQL;
    		DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
    		cursorSQL = "OPEN testCursor";
    		DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
    		cursorSQL = "FETCH NEXT FROM testCursor";
    		ResultSet rs = DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).executeQuery();
    		String rowID = "CA";
    		while (rs.next()) {
    			assertEquals(rowID, rs.getString(1));
    			assertEquals(2, rs.getInt(2));
    		}

    		rowID = "MA";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			assertEquals(rowID, rs.getString(1));
    			assertEquals(3, rs.getInt(2));
    		}

    		cursorSQL = "FETCH PRIOR FROM testCursor";
    		PreparedStatement ps =  DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL);
    		rs = ps.executeQuery();
    		rowID = "MA";
    		while (rs.next()) {
    			assertEquals(rowID, rs.getString(1));
    			assertEquals(3, rs.getInt(2));
    		}

    		rowID = "CA";
    		rs = ps.executeQuery(cursorSQL);
    		while (rs.next()){
    			assertEquals(rowID, rs.getString(1));
    			assertEquals(2, rs.getInt(2));
    		}

    		rs.close();
    	} finally {
    		DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
    		deleteDetailsTable();
    	}
    }
}
