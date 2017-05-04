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
package org.apache.phoenix.iterate;

import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.Aggregators;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.CursorUtil;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class CursorResultIterator implements ResultIterator {
    private String cursorName;
    private ResultIterator delegate;
    //TODO Configure fetch size from FETCH call
    private int fetchSize = 0;
    private int rowsRead = 0;
    private int rowsPriorRead = 0;
    private boolean isPrior = false;
    private Deque<Tuple> items = new LinkedList<Tuple>();
    private Queue<Tuple> stack = Collections.asLifoQueue(items);
    private Deque<Tuple> prevItems = new LinkedList<Tuple>();
    private Queue<Tuple> prevStack = Collections.asLifoQueue(prevItems);
    private boolean isAggregate;
    private boolean useCacheForNext = false;
    private Aggregators aggregators;
    private int cacheSize;
    private int offset;
    private int usedOffset;
    
    public CursorResultIterator(ResultIterator delegate, String cursorName, Aggregators aggregators, int cacheSize) {
        this.delegate = delegate;
        this.cursorName = cursorName;
        this.isAggregate = true;
        this.aggregators = aggregators;
        this.cacheSize = cacheSize;
    }
    
    public CursorResultIterator(PeekingResultIterator delegate, String cursorName) {
        this.delegate = delegate;
        this.cursorName = cursorName;
    }

    @Override
    public Tuple next() throws SQLException {
    	if (isPrior && isAggregate) {
    		if (fetchSize != rowsPriorRead) {
    			rowsRead = 0;
    			Tuple next = stack.poll();
    			useCacheForNext = true;
    			if (next != null) {
        			prevStack.offer(next);
    				rowsPriorRead++;
    				if (aggregators != null) {
    					Aggregator[] rowAggregators = aggregators.getAggregators();
    					aggregators.reset(rowAggregators);
    					aggregators.aggregate(rowAggregators, next);
    				}
    			} else if (usedOffset != 0) {
    				usedOffset -= 2*cacheSize;
    				if (usedOffset < 0)
    					usedOffset = 0;
    				this.delegate = CursorUtil.getOffsetIterator(cursorName, usedOffset);
    				Tuple cacheFeed = delegate.next();
    				int count = 1;
    				while (count < cacheSize && cacheFeed != null) {
    					stack.offer(cacheFeed);
    					cacheFeed = delegate.next();
    					count++;
    				}
    				stack.offer(cacheFeed);
    				offset = usedOffset + cacheSize;
    				rowsPriorRead++;
    				next = stack.poll();
    				prevStack.offer(next);
    				CursorUtil.updateCursor(cursorName, next, null);
    			} 
    			return next;
    		} else 
    			return null;
    	}
    	rowsPriorRead = 0;
    	if (useCacheForNext && isAggregate) {
    		if (fetchSize != rowsRead) {
    			Tuple next = prevStack.poll();
    			if (next != null) {
    				stack.offer(next);
    				rowsRead++;
    				if (aggregators != null) {
    					Aggregator[] rowAggregators = aggregators.getAggregators();
    					aggregators.reset(rowAggregators);
    					aggregators.aggregate(rowAggregators, next);
    				}
    				return next;
    			}
    			else {
    				useCacheForNext = false;
    				usedOffset = offset;
    			}
    		} else
    			return null;
    	}
    	if(!CursorUtil.moreValues(cursorName)){
    	    return null;
        } else if (fetchSize == rowsRead) {
            return null;
    	}
        Tuple next = delegate.next();
        if (isAggregate && rowsRead >= cacheSize) {
        	items.removeLast();
        }
        CursorUtil.updateCursor(cursorName, next, isAggregate ? null : ((PeekingResultIterator) delegate).peek());
        rowsRead++;
        offset++;
        usedOffset++;
        stack.offer(next);
        return next;
    }
    
    @Override
    public void explain(List<String> planSteps) {
        delegate.explain(planSteps);
        planSteps.add("CLIENT CURSOR " + cursorName);
    }

    @Override
    public String toString() {
        return "CursorResultIterator [cursor=" + cursorName + "]";
    }

    @Override
    public void close() throws SQLException {
        this.rowsRead = 0;
        this.fetchSize = 0;
    }
    
    public void closeIterator() throws SQLException {
        delegate.close();
    }

    public void closeCursor() throws SQLException {
        delegate.close();
    }

    public void setFetchSize(int fetchSize){
        this.fetchSize = fetchSize;
        this.rowsRead = 0;
        this.rowsPriorRead = 0;
    }
    
    public void setIsPrior(boolean isPrior){
        this.isPrior = isPrior;
    }
}
