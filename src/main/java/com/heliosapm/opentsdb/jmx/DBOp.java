/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.heliosapm.opentsdb.jmx;

/**
 * <p>Title: DBOp</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.opentsdb.jmx.DBOp</code></p>
 */

public abstract class DBOp implements Comparable<DBOp>, Runnable {
	/** The Op Type */
	public final MetaDBOp op;
	/** The cache insert op */
	public final Runnable cacheOp;
	
	/** The op arguments */
	protected final Object[] args;

	/**
	 * Creates a new DBOp
	 * @param op The op type
	 * @param cacheOp The cache insert op 
	 * @param args the Op arguments
	 */
	public DBOp(final MetaDBOp op, final Runnable cacheOp, final Object...args) {
		this.op = op;
		this.args = args;
		this.cacheOp = cacheOp;
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(final DBOp otherOp) {
		if(otherOp.op==op) return 1;
		return otherOp.op.compareTo(op);
	}
	
	public Object[] getArgs() {
		return args;
	}

	/**
	 * Returns the cache insert op 
	 * @return the cacheOp
	 */
	public Runnable getCacheOp() {
		return cacheOp;
	}
	
}
