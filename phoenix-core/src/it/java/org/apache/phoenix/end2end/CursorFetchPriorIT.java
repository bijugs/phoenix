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
    		String resultID = null;
    		while (rs.next()) {
    			resultID = rs.getString(1);
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(rowID,resultID);
    		i++;
    		rowID = "A" + i;
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			resultID = rs.getString(1);
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(rowID,resultID);
    		cursorSQL = "FETCH PRIOR FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			resultID = rs.getString(1);
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(rowID,resultID);
    		i--;
    		rowID = "A" + i;
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			resultID = rs.getString(1);
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(rowID,resultID);
    		cursorSQL = "FETCH NEXT FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		rowID = "A"+i;
    		while (rs.next()) {
    			resultID = rs.getString(1);
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(rowID,resultID);
    		i++;
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		rowID = "A"+i;
    		while (rs.next()) {
    			resultID = rs.getString(1);
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(rowID,resultID);
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
    			assertEquals(rowID, rs.getString(1));
    		}
    		i++;
    		rowID = "A" + i;
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			assertEquals(rowID, rs.getString(1));
    		}
    		i++;
    		rowID = "A" + i;
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(2,i);
    		cursorSQL = "FETCH PRIOR FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			assertEquals(rowID, rs.getString(1));
    		}
    		i--;
    		rowID = "A" + i;
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(1,i);
    		cursorSQL = "FETCH NEXT FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		rowID = "A"+i;
    		while (rs.next()) {
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
    			assertEquals(rowID, rs.getString(1));
    			i++;
    		}
    		assertEquals(4,i);
    		i--;
    		cursorSQL = "FETCH PRIOR 4 ROWS FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			rowID = "A" + i;
    			assertEquals(rowID, rs.getString(1));
        		i--;
    		}
    		assertEquals(-1,i);
    		cursorSQL = "FETCH NEXT 4 ROWS FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		i = 0;
    		rowID = null;
    		while (rs.next()) {
    			rowID = "A" + i;
    			assertEquals(rowID, rs.getString(1));
    			i++;
    		}
    		assertEquals(4,i);
    		i--;
    		cursorSQL = "FETCH PRIOR 4 ROWS FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			rowID = "A" + i;
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
    public void testFetchPriorOnCursorsSimple21() throws SQLException {
    	/*
    	 * Failing
    	 */
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
    		String rowID = null;
    		while (rs.next()) {
    			rowID = "A" + i;
    			assertEquals(rowID, rs.getString(1));
    			i++;
    			rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		}
    		assertEquals(10,i);
    		i--;
    		cursorSQL = "FETCH PRIOR FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			rowID = "A" + i;
    			assertEquals(rowID, rs.getString(1));
        		i--;
        		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		}
    		assertEquals(-1,i);
    		rs.close();
    	} finally {
    		DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
    		deleteTestTable();
    	}

    }
    
    @Test
    public void testFetchPriorOnCursorsSimple22() throws SQLException {
    	try {
    		createAndInitializeTestTable();
    		String querySQL = "SELECT * FROM " + TABLE_NAME;
    		// Test actual cursor implementation
    		String cursorSQL = "DECLARE testCursor CURSOR FOR " + querySQL;
    		DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
    		cursorSQL = "OPEN testCursor";
    		DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
    		cursorSQL = "FETCH NEXT 20 ROWS FROM testCursor";
    		ResultSet rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		int i = 0;
    		String rowID = null;
    		while (rs.next()) {
    			rowID = "A" + i;
    			assertEquals(rowID, rs.getString(1));
    			i++;
    		}
    		assertEquals(10,i);
    		i--;
    		cursorSQL = "FETCH PRIOR 4 ROWS FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			rowID = "A" + i;
    			assertEquals(rowID, rs.getString(1));
        		i--;
    		}
            assertEquals(5,i);
    		rs.close();
    	} finally {
    		DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
    		deleteTestTable();
    	}

    }
    
    @Test
    public void testFetchPriorOnCursorsSimple3() throws SQLException {
    	/*
    	 * Test with a small cache size of 2 rows
    	 */
    	try {
    		createAndInitializeTestTable();
    		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    		props.setProperty(QueryServices.MAX_CURSOR_CACHE_ROW_COUNT_ATTRIB, Integer.toString(2));
    		Connection conn = DriverManager.getConnection(getUrl(), props);
    		String querySQL = "SELECT * FROM " + TABLE_NAME + " ORDER BY TICKER DESC";
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
    			rowID = "A"+i;;
    			assertEquals(rowID, rs.getString(1));
    			i--;
    		}
    		i++;
    		assertEquals(6, i);
    		cursorSQL = "FETCH PRIOR 4 ROWS FROM testCursor";
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
    
    @Test
    public void testFetchPriorOnCursorsSimple31() throws SQLException {
    	/*
    	 * Test with a small cache size of 2 rows
    	 */
    	try {
    		createAndInitializeTestTable();
    		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    		props.setProperty(QueryServices.MAX_CURSOR_CACHE_ROW_COUNT_ATTRIB, Integer.toString(2));
    		Connection conn = DriverManager.getConnection(getUrl(), props);
    		String querySQL = "SELECT * FROM " + TABLE_NAME + " ORDER BY TICKER DESC";
    		// Test actual cursor implementation
    		String cursorSQL = "DECLARE testCursor CURSOR FOR " + querySQL;
    		conn.prepareStatement(cursorSQL).execute();
    		cursorSQL = "OPEN testCursor";
    		conn.prepareStatement(cursorSQL).execute();
    		cursorSQL = "FETCH NEXT 20 ROWS FROM testCursor";
    		ResultSet rs = conn.prepareStatement(cursorSQL).executeQuery();
    		int i = 9;
    		String rowID = null;
    		while (rs.next()) {
    			rowID = "A"+i;;
    			assertEquals(rowID, rs.getString(1));
    			i--;
    		}
    		i++;
    		assertEquals(0, i);
    		cursorSQL = "FETCH PRIOR 4 ROWS FROM testCursor";
    		rs = conn.prepareStatement(cursorSQL).executeQuery();
    		while (rs.next()) {
    			rowID = "A"+i;
    			assertEquals(rowID, rs.getString(1));
    			i++;
    		}
    		assertEquals(4,i);
    		rs.close();
    		conn.prepareStatement("CLOSE testCursor").execute();
    	} finally {
    		deleteTestTable();
    	}
    }
    
    @Test
    public void testFetchPriorOnCursorsSimpleDesc() throws SQLException {
    	try {
    		createAndInitializeTestTable();
    		String querySQL = "SELECT * FROM " + TABLE_NAME +" ORDER BY TICKER DESC";
    		// Test actual cursor implementation
    		String cursorSQL = "DECLARE testCursor CURSOR FOR " + querySQL;
    		DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
    		cursorSQL = "OPEN testCursor";
    		DriverManager.getConnection(getUrl()).prepareStatement(cursorSQL).execute();
    		cursorSQL = "FETCH NEXT FROM testCursor";
    		ResultSet rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		int i = 9;
    		String rowID = "A" + i;
    		String resultID = null;
    		while (rs.next()) {
    			resultID = rs.getString(1);
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(rowID,resultID);
    		i--;
    		rowID = "A" + i;
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			resultID = rs.getString(1);
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(rowID,resultID);
    		cursorSQL = "FETCH PRIOR FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			resultID = rs.getString(1);
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(rowID,resultID);
    		i++;
    		rowID = "A" + i;
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			resultID = rs.getString(1);
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(rowID,resultID);
    		cursorSQL = "FETCH NEXT FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		rowID = "A"+i;
    		while (rs.next()) {
    			resultID = rs.getString(1);
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(rowID,resultID);
    		i--;
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		rowID = "A"+i;
    		while (rs.next()) {
    			resultID = rs.getString(1);
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(rowID,resultID);
    		cursorSQL = "FETCH PRIOR FROM testCursor";
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			resultID = rs.getString(1);
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(rowID,resultID);
    		i++;
    		rowID = "A" + i;
    		rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			resultID = rs.getString(1);
    			assertEquals(rowID, rs.getString(1));
    		}
    		assertEquals(rowID,resultID);
    		rs.close();
    	} finally {
    		DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
    		deleteTestTable();
    	}

    }
    
    @Test
    public void testFetchPriorOnCursorsSimpleDesc1() throws SQLException {
    	/*
    	 * Test with a small cache size of 2 rows
    	 */
    	Connection conn = null;
    	ResultSet rs = null;
    	try {
    		createAndInitializeTestTable();
    		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    		props.setProperty(QueryServices.MAX_CURSOR_CACHE_ROW_COUNT_ATTRIB, Integer.toString(2));
    		conn = DriverManager.getConnection(getUrl(), props);
    		String querySQL = "SELECT * FROM " + TABLE_NAME + " ORDER BY TICKER DESC";
    		// Test actual cursor implementation
    		String cursorSQL = "DECLARE testCursor CURSOR FOR " + querySQL;
    		conn.prepareStatement(cursorSQL).execute();
    		cursorSQL = "OPEN testCursor";
    		conn.prepareStatement(cursorSQL).execute();
    		cursorSQL = "FETCH NEXT FROM testCursor";
    		rs = conn.createStatement().executeQuery(cursorSQL);
    		int i = 9;
    		String rowID = null;
    		while (rs.next()) {
    			rowID = "A"+i;
    			assertEquals(rowID, rs.getString(1));
    			i--;
    			rs = conn.createStatement().executeQuery(cursorSQL);
    		}
    		i++;
    		assertEquals(0, i);
    		cursorSQL = "FETCH PRIOR FROM testCursor";
    		rs = conn.createStatement().executeQuery(cursorSQL);
    		while (rs.next()) {
    			rowID = "A"+i;
    			assertEquals(rowID, rs.getString(1));
    			i++;
    			rs = conn.createStatement().executeQuery(cursorSQL);
    		}
    		assertEquals(10,i);
    	} finally {
    		rs.close();
    		conn.prepareStatement("CLOSE testCursor").execute();
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
    		while (rs.next()) {;
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
    		rs =  DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
    		rowID = "MA";
    		while (rs.next()) {
    			assertEquals(rowID, rs.getString(1));
    			assertEquals(3, rs.getInt(2));
    		}

    		rowID = "CA";
    		rs =  DriverManager.getConnection(getUrl()).createStatement().executeQuery(cursorSQL);
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
    
    @Test
    public void testFetchPriorOnCursorCache() throws SQLException {
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
    		rs.close();
    	} finally {
    		DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
    		deleteTestTable();
    	}

    }
    
    @Test
    public void testFetchPriorOnCursorPopulate() throws SQLException {
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
    		while (rs.next()) {
    			rowID = "A"+i;
    			assertEquals(rowID, rs.getString(1));
    			i++;
    		}
    		rs.close();
    	} finally {
    		DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
    		deleteTestTable();
    	}

    }
    
    @Test
    public void testFetchPriorOnCursorPopulate1() throws SQLException {
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
    		cursorSQL = "FETCH NEXT 20 ROWS FROM testCursor";
    		ResultSet rs = conn.prepareStatement(cursorSQL).executeQuery();
    		int i = 9;
    		String rowID = null;
    		while (rs.next()) {
    			rowID = "A"+i;
    			assertEquals(rowID, rs.getString(1));
    			i--;
    		}
    		i++;
    		assertEquals(0, i);
    		cursorSQL = "FETCH PRIOR 4 ROW FROM testCursor";
    		rs = conn.prepareStatement(cursorSQL).executeQuery();
    		while (rs.next()) {
    			rowID = "A"+i;
    			assertEquals(rowID, rs.getString(1));
    			i++;
    		}
    		assertEquals(4,i);
    		rs.close();
    	} finally {
    		DriverManager.getConnection(getUrl()).prepareStatement("CLOSE testCursor").execute();
    		deleteTestTable();
    	}

    }
}