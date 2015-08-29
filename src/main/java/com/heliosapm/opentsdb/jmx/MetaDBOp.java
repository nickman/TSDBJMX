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

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.heliosapm.tsdbex.sqlbinder.SQLWorker;

import net.opentsdb.meta.UIDMeta;

/**
 * <p>Title: MetaDBOp</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.opentsdb.jmx.MetaDBOp</code></p>
 */

public enum MetaDBOp implements DBOpFactory {	
	/** Saves a tagK UIDMeta */
	UIDMETAK("INSERT INTO TSD_TAGK VALUES(?, 1, ?, ?, ?, null, null, null, null)"){
		@Override
		public DBOp newDbOp(final SQLWorker worker, final PreparedStatement ps, final Object... args) {
			return new DBOp(UIDMETAK) {
				@Override
				public void run() {
					final UIDMeta meta = (UIDMeta)args[0];
					try {
						worker.batch(ps.getConnection(), ps, sqlOp, meta.getUID(), meta.getName(), meta.getCreated(), System.currentTimeMillis());
					} catch (SQLException ex) {
						throw new RuntimeException("DBOp Failure [" + op + "]", ex);
					}
				}
			};			
		}
	},
	/** Saves a tagV UIDMeta */
	UIDMETAV("INSERT INTO TSD_TAGV VALUES(?, 1, ?, ?, ?, null, null, null, null)"){
		@Override
		public DBOp newDbOp(final SQLWorker worker, final PreparedStatement ps, final Object... args) {
			return new DBOp(UIDMETAK) {
				@Override
				public void run() {
					final UIDMeta meta = (UIDMeta)args[0];
					try {
						worker.batch(ps.getConnection(), ps, sqlOp, meta.getUID(), meta.getName(), meta.getCreated(), System.currentTimeMillis());
					} catch (SQLException ex) {
						throw new RuntimeException("DBOp Failure [" + op + "]", ex);
					}
				}
			};			
		}
	},
	/** Saves a metric UIDMeta */
	UIDMETAM("INSERT INTO TSD_METRIC VALUES(?, 1, ?, ?, ?, null, null, null, null)"){
		@Override
		public DBOp newDbOp(final SQLWorker worker, final PreparedStatement ps, final Object... args) {
			return new DBOp(UIDMETAK) {
				@Override
				public void run() {
					final UIDMeta meta = (UIDMeta)args[0];
					try {
						worker.batch(ps.getConnection(), ps, sqlOp, meta.getUID(), meta.getName(), meta.getCreated(), System.currentTimeMillis());
					} catch (SQLException ex) {
						throw new RuntimeException("DBOp Failure [" + op + "]", ex);
					}
				}
			};			
		}
	};	
//	UIDPAIR(){
//		@Override
//		public DBOp newDbOp(UIDPAIR) {			
//			return null;
//		}
//	},
//	UIDPAIRFQN(){
//		@Override
//		public DBOp newDbOp(UIDPAIRFQN) {			
//			return null;
//		}
//	},
//	TSMETA(){
//		@Override
//		public DBOp newDbOp(TSMETA) {			
//			return null;
//		}
//	};
	
	final String sqlOp;
	
	private MetaDBOp(final String sqlOp) {
		this.sqlOp = sqlOp;
	}
}
