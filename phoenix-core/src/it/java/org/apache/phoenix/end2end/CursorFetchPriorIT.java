package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
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
    		stmt.setInt(2, rowCount);
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
    		String querySQL = "SELECT * FROM " + TABLE_NAME + " ORDER BY TICKER DESC";
    		// Test actual cursor implementation
    		String cursorSQL = "DECLARE testCursor CURSOR FOR " + querySQL;
    		DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
    		cursorSQL = "OPEN testCursor";
    		DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
    		cursorSQL = "FETCH NEXT FROM testCursor";
    		ResultSet rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		int i = 9;
    		String rowID = "A" + i;
    		while (rs.next()) {
    			System.out.println("**** Value 0 next****"+ rs.getString(1));
    			assertEquals(rowID, rs.getString(1));
    		}
    		i--;
    		rowID = "A" + i;
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			System.out.println("**** Value 1 next****"+ rs.getString(1));
    			assertEquals(rowID, rs.getString(1));
    		}
    		i--;
    		rowID = "A" + i;
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			System.out.println("**** Value 1 next****"+ rs.getString(1));
    			assertEquals(rowID, rs.getString(1));
    		}
    		cursorSQL = "FETCH PRIOR FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			System.out.println("**** Value 2 prior****"+ rs.getString(1));
    			assertEquals(rowID, rs.getString(1));
    		}
    		i++;
    		rowID = "A" + i;
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			System.out.println("**** Value 3 prior****"+ rs.getString(1));
    			assertEquals(rowID, rs.getString(1));
    		}
    		cursorSQL = "FETCH NEXT FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		rowID = "A"+i;
    		while (rs.next()) {
    			System.out.println("**** Value 4 next****"+ rs.getString(1));
    			assertEquals(rowID, rs.getString(1));
    		}
    		rs.close();
    	} finally {
    		DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
    		deleteTestTable();
    	}

    }
    
    @Test
    public void testFetchPriorOnCursorsSimple1() throws SQLException {
    	try {
    		createAndInitializeTestTable();
    		String querySQL = "SELECT * FROM " + TABLE_NAME;
    		// Test actual cursor implementation
    		String cursorSQL = "DECLARE testCursor CURSOR FOR " + querySQL;
    		DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
    		cursorSQL = "OPEN testCursor";
    		DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
    		cursorSQL = "FETCH NEXT FROM testCursor";
    		ResultSet rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		int i = 0;
    		String rowID = "A" + i;
    		while (rs.next()) {
    			System.out.println("**** Value 0 next****"+ rs.getString(1));
    			assertEquals(rowID, rs.getString(1));
    		}
    		i++;
    		rowID = "A" + i;
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			System.out.println("**** Value 1 next****"+ rs.getString(1));
    			assertEquals(rowID, rs.getString(1));
    		}
    		i++;
    		rowID = "A" + i;
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			System.out.println("**** Value 1 next****"+ rs.getString(1));
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(2,i);
    		cursorSQL = "FETCH PRIOR FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			System.out.println("**** Value 2 prior****"+ rs.getString(1));
    			assertEquals(rowID, rs.getString(1));
    		}
    		i--;
    		rowID = "A" + i;
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			System.out.println("**** Value 3 prior****"+ rs.getString(1));
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(1,i);
    		cursorSQL = "FETCH NEXT FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		rowID = "A"+i;
    		while (rs.next()) {
    			System.out.println("**** Value 4 next****"+ rs.getString(1));
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(1,i);
    		rs.close();
    	} finally {
    		DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
    		deleteTestTable();
    	}

    }
    
    @Test
    public void testFetchPriorOnCursorsSimple2() throws SQLException {
    	try {
    		createAndInitializeTestTable();
    		String querySQL = "SELECT * FROM " + TABLE_NAME;
    		// Test actual cursor implementation
    		String cursorSQL = "DECLARE testCursor CURSOR FOR " + querySQL;
    		DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
    		cursorSQL = "OPEN testCursor";
    		DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
    		cursorSQL = "FETCH NEXT 4 ROWS FROM testCursor";
    		ResultSet rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		int i = 0;
    		String rowID = null;
    		while (rs.next()) {
    			rowID = "A" + i;
    			System.out.println("**** Value 0 next****"+ rs.getString(1));
    			assertEquals(rowID, rs.getString(1));
    			i++;
    		}
    		assertEquals(4,i);
    		i--;
    		cursorSQL = "FETCH PRIOR 4 ROWS FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			rowID = "A" + i;
    			System.out.println("**** Value 2 prior****"+ rs.getString(1));
    			assertEquals(rowID, rs.getString(1));
        		i--;
    		}
    		assertEquals(-1,i);
    		rs.close();
    	} finally {
    		DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
    		deleteTestTable();
    	}

    }
    
    @Test
    public void testFetchPriorOnCursorsComplex() throws SQLException {
    	/*
    	 * Test case on a complex query and in this case using a group by on a non key column
    	 */
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
    		String rsRowID = null;
    		String rowID = "CA";
    		while (rs.next()) {
    			assertEquals(rowID, rs.getString(1));
    			rsRowID = rs.getString(1);
    		}
    		assertEquals(rowID, rsRowID);
    		rowID = "MA";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			assertEquals(rowID, rs.getString(1));
    			rsRowID = rs.getString(1);
    		}
    		assertEquals(rowID, rsRowID);
    		cursorSQL = "FETCH PRIOR FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).executeQuery();
    		rowID = "MA";
    		while (rs.next()) {
    			assertEquals(rowID, rs.getString(1));
    			rsRowID = rs.getString(1);
    		}
    		assertEquals(rowID, rsRowID);
    		rowID = "CA";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			assertEquals(rowID, rs.getString(1));
    			rsRowID = rs.getString(1);
    		}
    		assertEquals(rowID, rsRowID);
    		rs.close();
    	} finally {
    		DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
    		deleteDetailsTable();
    	}
    }
    
    @Test
    public void testFetchPriorOnCursorsCount() throws SQLException {
    	/*
    	 * Test case for a query using a function and in this case COUNT
    	 */
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
    		String rsRowID = null;
    		while (rs.next()) {
    			assertEquals(rowID, rs.getString(1));
    			rsRowID = rs.getString(1);
    			assertEquals(2, rs.getInt(2));
    		}
    		assertEquals(rowID, rsRowID);
    		rowID = "MA";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			assertEquals(rowID, rs.getString(1));
    			rsRowID = rs.getString(1);
    			assertEquals(3, rs.getInt(2));
    		}
    		assertEquals(rowID, rsRowID);
    		cursorSQL = "FETCH PRIOR FROM testCursor";
    		rs =  DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		rowID = "MA";
    		while (rs.next()) {
    			assertEquals(rowID, rs.getString(1));
    			rsRowID = rs.getString(1);
    			assertEquals(3, rs.getInt(2));
    		}
    		assertEquals(rowID, rsRowID);
    		rowID = "CA";
    		rs =  DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()){
    			assertEquals(rowID, rs.getString(1));
    			rsRowID = rs.getString(1);
    			assertEquals(2, rs.getInt(2));
    		}
    		assertEquals(rowID, rsRowID);
    		rs.close();
    	} finally {
    		DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
    		deleteDetailsTable();
    	}
    }
    
    @Test
    public void testFetchPriorOnCursorCache() throws SQLException {
    	/*
    	 * Simple test to check the use of cursor cache
    	 */
    	try {
    		createAndInitializeTestTable();
    		String querySQL = "SELECT * FROM " + TABLE_NAME + " ORDER BY PRICE DESC";
    		// Test actual cursor implementation
    		String cursorSQL = "DECLARE testCursor CURSOR FOR " + querySQL;
    		DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
    		cursorSQL = "OPEN testCursor";
    		DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
    		cursorSQL = "FETCH NEXT 4 ROWS FROM testCursor";
    		ResultSet rs = DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).executeQuery();
    		int i = 9;
    		String rowID = null;
    		while (rs.next()) {
    			rowID = "A"+i;
    			assertEquals(rowID, rs.getString(1));
    			i--;
    		}
    		i++;
    		assertEquals(6,i);
    		cursorSQL = "FETCH PRIOR 2 ROW FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).executeQuery();
    		while (rs.next()) {
    			rowID = "A"+i;
    			assertEquals(rowID, rs.getString(1));
    			i++;
    		}
    		assertEquals(8,i);
    		cursorSQL = "FETCH NEXT 4 ROWS FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).executeQuery();
    		i--;
    		while (rs.next()) {
    			rowID = "A"+i;
    			assertEquals(rowID, rs.getString(1));
    			i--;
    		}
    		assertEquals(3,i);
    		rs.close();
    	} finally {
    		DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
    		deleteTestTable();
    	}

    }
    
    @Test
    public void testFetchPriorOnCursorPopulate() throws SQLException {
    	/*
    	 * Test with a small cache size of 2 rows
    	 */
    	try {
    		createAndInitializeTestTable();
    		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    		props.setProperty(QueryServices.MAX_CURSOR_CACHE_ROW_COUNT_ATTRIB, Integer.toString(2));
    		Connection conn = DriverManager.getConnection(getUrl(), props);
    		String querySQL = "SELECT * FROM " + TABLE_NAME + " ORDER BY PRICE DESC";
    		// Test actual cursor implementation
    		String cursorSQL = "DECLARE testCursor CURSOR FOR " + querySQL;
    		conn.prepareStatement(cursorSQL).execute();
    		cursorSQL = "OPEN testCursor";
    		conn.prepareStatement(cursorSQL).execute();
    		cursorSQL = "FETCH NEXT 4 ROWS FROM testCursor";
    		ResultSet rs = conn.prepareStatement(cursorSQL).executeQuery();
    		int i = 9;
    		String rowID = null;
    		while (rs.next()) {
    			rowID = "A"+i;
    			assertEquals(rowID, rs.getString(1));
    			i--;
    		}
    		i++;
    		assertEquals(6, i);
    		cursorSQL = "FETCH PRIOR 4 ROW FROM testCursor";
    		rs = conn.prepareStatement(cursorSQL).executeQuery();
    		while (rs.next()) {
    			rowID = "A"+i;
    			assertEquals(rowID, rs.getString(1));
    			i++;
    		}
    		assertEquals(10,i);
    		cursorSQL = "FETCH NEXT 4 ROWS FROM testCursor";
    		rs = conn.prepareStatement(cursorSQL).executeQuery();
    		i--;
    		while (rs.next()) {
    			rowID = "A"+i;
    			assertEquals(rowID, rs.getString(1));
    			i--;
    		}
    		cursorSQL = "FETCH PRIOR 4 ROWS FROM testCursor";
    		rs = conn.prepareStatement(cursorSQL).executeQuery();
    		i++;
    		assertEquals(6, i);
    		while (rs.next()) {
    			rowID = "A"+i;
    			assertEquals(rowID, rs.getString(1));
    			i++;
    		}
    		assertEquals(10,i);
    		rs.close();
    		conn.prepareStatement("CLOSE testCursor").execute();
    	} finally {
    		deleteTestTable();
    	}

    }
}
