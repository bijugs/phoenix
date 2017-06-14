package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ScanUtil;
import org.junit.BeforeClass;
import org.junit.Test;

public class ReverseScanTest extends BaseHBaseManagedTimeIT {
	
	protected static final Log LOG = LogFactory.getLog(ReverseScanTest.class);

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> props = new HashMap<String, String>();
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testReverseScan() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE " + tableName + " (id INTEGER NOT NULL PRIMARY KEY, val varchar)");
        for (int i = 0; i < 10; i++) {
            conn.createStatement().executeUpdate("UPSERT INTO " + tableName + " VALUES(" + i + ",'A" + i + "')");
        }
        conn.commit();
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM " + tableName);
        QueryPlan plan = statement.unwrap(PhoenixStatement.class).getQueryPlan();
        Scan scan = plan.getContext().getScan();
        assertTrue(!scan.isReversed());
        for (int i = 0; i < 10; i++) {
            rs.next();
            assertEquals(i, rs.getInt(1));
            LOG.debug("******** "+rs.getInt(1));
        }
        scan.setStartRow(PInteger.INSTANCE.toBytes(4));
        scan.setStopRow(PInteger.INSTANCE.toBytes(8));
        rs = new PhoenixResultSet(plan.iterator(), plan.getProjector(), plan.getContext());
        while (rs.next()) {
            LOG.debug("**for*** "+rs.getInt(1));
        }
        ScanUtil.setReversed(scan);
        scan.setStartRow(PInteger.INSTANCE.toBytes(4));
        scan.setStopRow(PInteger.INSTANCE.toBytes(8));
        rs = new PhoenixResultSet(plan.iterator(), plan.getProjector(), plan.getContext());
        assertTrue(ScanUtil.isReversed(scan));
        for (int i = 7; i >= 4; i--) {
            rs.next();
            assertEquals(i, rs.getInt(1));
            LOG.debug("**rev*** "+rs.getInt(1));
        }
        rs.close();
        conn.close();
    }   
    
    @Test
    public void testReverseScan1() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE " + tableName + " (id INTEGER NOT NULL PRIMARY KEY, val varchar)");
        for (int i = 0; i < 10; i++) {
            conn.createStatement().executeUpdate("UPSERT INTO " + tableName + " VALUES(" + i + ",'A" + i + "')");
        }
        conn.commit();
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM " + tableName +" ORDER BY id DESC");
        QueryPlan plan = statement.unwrap(PhoenixStatement.class).getQueryPlan();
        Scan scan = plan.getContext().getScan();
        for (int i = 9; i >= 8; i--) {
            rs.next();
            assertEquals(i, rs.getInt(1));
            LOG.debug("******** "+rs.getInt(1));
        }
        ScanUtil.unsetReversed(scan);
        plan.getContext().setReversalAlreadySet(true);
        scan.setStartRow(PInteger.INSTANCE.toBytes(8));
        scan.setStopRow(PInteger.INSTANCE.toBytes(10));
        rs = new PhoenixResultSet(plan.iterator(), plan.getProjector(), plan.getContext());
        for (int i = 8; i <= 9; i++) {
            rs.next();
            LOG.debug("**rev*** "+rs.getInt(1));
            assertEquals(i, rs.getInt(1));
        }
        rs.close();
        conn.close();
    } 
    
    
}