/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.execute.CursorFetchPlan;
import org.apache.phoenix.iterate.CursorResultIterator;
import org.apache.phoenix.iterate.LimitingResultIterator;
import org.apache.phoenix.iterate.OffsetResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.parse.CloseStatement;
import org.apache.phoenix.parse.DeclareCursorStatement;
import org.apache.phoenix.parse.OpenStatement;
import org.apache.phoenix.schema.tuple.Tuple;

public final class CursorUtil {

    private static class CursorWrapper {
        private final String cursorName;
        private final String selectSQL;
        private boolean isOpen = false;
        QueryPlan queryPlan;
        ImmutableBytesWritable nextRow;
        ImmutableBytesWritable currentRow;
        ImmutableBytesWritable previousRow;
        private Scan scan;
        private boolean moreValues=true;
        private boolean isAlreadyReversed;
        private boolean islastCallNext;
        private CursorFetchPlan fetchPlan;
        private int offset = -1;
		private boolean isAggregate;
		private ImmutableBytesWritable pPreviousRow;
		private boolean isSeqScan;

        private CursorWrapper(String cursorName, String selectSQL, QueryPlan queryPlan){
            this.cursorName = cursorName;
            this.selectSQL = selectSQL;
            this.queryPlan = queryPlan;
            this.islastCallNext = true;
            this.fetchPlan = new CursorFetchPlan(queryPlan, cursorName);
            this.isAggregate = fetchPlan.isAggregate();
    		this.isSeqScan = fetchPlan.isSeqScan();
        }

        private synchronized void openCursor(Connection conn) throws SQLException {
            if(isOpen){
                return;
            }
            this.scan = this.fetchPlan.getContext().getScan();
            isAlreadyReversed=OrderBy.REV_ROW_KEY_ORDER_BY.equals(this.queryPlan.getOrderBy());
            isOpen = true;
        }

        private void closeCursor() throws SQLException {
            isOpen = false;
            ((CursorResultIterator) fetchPlan.iterator()).closeCursor();
            //TODO: Determine if the cursor should be removed from the HashMap at this point.
            //Semantically it makes sense that something which is 'Closed' one should be able to 'Open' again.
            mapCursorIDQuery.remove(this.cursorName);
        }

        private QueryPlan getFetchPlan(boolean isNext, int fetchSize) throws SQLException {
        	if (!isOpen)
                throw new SQLException("Fetch call on closed cursor '" + this.cursorName + "'!");
            if (!isNext) {
            	fetchPlan.updateDirection(true);
            } else
            	fetchPlan.updateDirection(false);
            fetchPlan.setFetchSize(fetchSize);
            setupScanForFetch(isNext);
            return this.fetchPlan;
        }

		private void setupScanForFetch(boolean isNext) {
			if (isSeqScan) 
			{
				if (islastCallNext != isNext) {
					// we are switching from prior to next or next to prior
					if (islastCallNext && !isAlreadyReversed) {
						// reverse the scan if last call was next
						ScanUtil.setReversed(scan);
					} else {
						ScanUtil.unsetReversed(scan);
					}
					isAlreadyReversed = !isAlreadyReversed;
					if (!isAlreadyReversed) {
						setScanBoundariesIfNotnull(currentRow, new ImmutableBytesWritable(scan.getStopRow()));
					} else {
						setScanBoundariesIfNotnull(currentRow, nextRow);
					}
				} else {	
					if (!isAlreadyReversed) {
						setScanBoundariesIfNotnull(nextRow, null);
					} else {
						setScanBoundariesIfNotnull(null, currentRow);
					}
					if (previousRow != null) {
						pPreviousRow = new ImmutableBytesWritable(previousRow.get());
					}
				}
				islastCallNext = isNext;
			}
		}
		
		private void setScanBoundariesIfNotnull(ImmutableBytesWritable startRow, ImmutableBytesWritable stopRow) {
			if (startRow != null) {
				scan.setStartRow(startRow.get());
			}
			if (stopRow != null) {
				scan.setStopRow(stopRow.get());
			}
		}
		
        public void updateLastScanRow(Tuple rowValues,Tuple nextRowValues) {
        	
        	this.moreValues = isSeqScan ? nextRowValues != null : rowValues != null;
            if(!moreValues()){
               return;
            }        
            if (nextRow == null) {
                nextRow = new ImmutableBytesWritable();
            }
			if (currentRow == null) {
				currentRow = new ImmutableBytesWritable();
			} else {
				previousRow = new ImmutableBytesWritable(currentRow.get());
			}
            if (nextRowValues != null) {
                nextRowValues.getKey(nextRow);
            } /*else
            	nextRow = null;*/
            if (rowValues != null) {
                rowValues.getKey(currentRow);
            } else
            	return;
            if (pPreviousRow == null) {
				pPreviousRow = new ImmutableBytesWritable(scan.getStartRow());
			}
            offset++;
        }

        public boolean moreValues() {
            return moreValues;
        }

        public String getFetchSQL() throws SQLException {
            if (!isOpen)
                throw new SQLException("Fetch call on closed cursor '" + this.cursorName + "'!");
            return selectSQL;
        }
        
        public ResultIterator getLimitOffsetIterator(int offset, int limit) {
        	ResultIterator rsIterator = null;
        	try {
				rsIterator = queryPlan.iterator();
				rsIterator = new OffsetResultIterator(rsIterator, offset);
				rsIterator = new LimitingResultIterator(rsIterator, limit);			
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return rsIterator;
        }
    }

    private static Map<String, CursorWrapper> mapCursorIDQuery = new HashMap<String,CursorWrapper>();

    /**
     * Private constructor
     */
    private CursorUtil() {
    }

    /**
     *
     * @param stmt DeclareCursorStatement instance intending to declare a new cursor.
     * @return Returns true if the new cursor was successfully declared. False if a cursor with the same
     * identifier already exists.
     */
    public static boolean declareCursor(DeclareCursorStatement stmt, QueryPlan queryPlan) throws SQLException {
        if(mapCursorIDQuery.containsKey(stmt.getCursorName())){
            throw new SQLException("Can't declare cursor " + stmt.getCursorName() + ", cursor identifier already in use.");
        } else {
            mapCursorIDQuery.put(stmt.getCursorName(), new CursorWrapper(stmt.getCursorName(), stmt.getQuerySQL(), queryPlan));
            return true;
        }
    }

    public static boolean openCursor(OpenStatement stmt, Connection conn) throws SQLException {
        if(mapCursorIDQuery.containsKey(stmt.getCursorName())){
            mapCursorIDQuery.get(stmt.getCursorName()).openCursor(conn);
            return true;
        } else{
            throw new SQLException("Cursor " + stmt.getCursorName() + " not declared.");
        }
    }

    public static void closeCursor(CloseStatement stmt) throws SQLException {
        if(mapCursorIDQuery.containsKey(stmt.getCursorName())){
            mapCursorIDQuery.get(stmt.getCursorName()).closeCursor();
        }
    }

    public static QueryPlan getFetchPlan(String cursorName, boolean isNext, int fetchSize) throws SQLException {
        if(mapCursorIDQuery.containsKey(cursorName)){
            return mapCursorIDQuery.get(cursorName).getFetchPlan(isNext, fetchSize);
        } else {
            throw new SQLException("Cursor " + cursorName + " not declared.");
        }
    }
    
    public static String getFetchSQL(String cursorName) throws SQLException {
        if (mapCursorIDQuery.containsKey(cursorName)) {
            return mapCursorIDQuery.get(cursorName).getFetchSQL();
        } else {
            throw new SQLException("Cursor " + cursorName + " not declared.");
        }
    }

    public static void updateCursor(String cursorName, Tuple rowValues, Tuple nextRowValues) throws SQLException {
        mapCursorIDQuery.get(cursorName).updateLastScanRow(rowValues,nextRowValues);
    }

    public static boolean cursorDeclared(String cursorName){
        return mapCursorIDQuery.containsKey(cursorName);
    }

    public static boolean moreValues(String cursorName) {
        return mapCursorIDQuery.get(cursorName).moreValues();
    }
    
    public static ResultIterator getLimitOffsetIterator(String cursorName, int offset, int limit) {
    	return mapCursorIDQuery.get(cursorName).getLimitOffsetIterator(offset, limit);
    }
    
}
