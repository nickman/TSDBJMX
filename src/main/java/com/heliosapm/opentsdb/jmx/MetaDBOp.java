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
import java.sql.Timestamp;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import net.opentsdb.meta.UIDMeta;
import net.opentsdb.uid.UniqueId.UniqueIdType;

import com.heliosapm.tsdbex.sqlbinder.SQLWorker;
import com.heliosapm.utils.time.SystemClock;

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
		public DBOp newDbOp(final SQLWorker worker, final PreparedStatement ps, final Runnable cachePush, final Object...args) {
			final Object[] opArgs = args;
			return new DBOp(UIDMETAK, cachePush, opArgs) {
				@Override
				public void run() {
					final UIDMeta meta = (UIDMeta)opArgs[0];
					try {
//						worker.batch(ps.getConnection(), ps, sqlOp, meta.getUID(), meta.getName(), meta.getCreated(), System.currentTimeMillis());
						worker.execute(sqlOp, meta.getUID(), meta.getName(), meta.getCreated(), System.currentTimeMillis());
					} catch (Exception ex) {
						throw new RuntimeException("DBOp Failure [" + op + "]", ex);
					}
				}
			};			
		}
	},
	/** Saves a tagV UIDMeta */
	UIDMETAV("INSERT INTO TSD_TAGV VALUES(?, 1, ?, ?, ?, null, null, null, null)"){
		@Override
		public DBOp newDbOp(final SQLWorker worker, final PreparedStatement ps, final Runnable cachePush, final Object...args) {
			final Object[] opArgs = args;
			return new DBOp(UIDMETAM, cachePush, opArgs) {
				@Override
				public void run() {
					final UIDMeta meta = (UIDMeta)opArgs[0];
					try {
//						worker.batch(ps.getConnection(), ps, sqlOp, meta.getUID(), meta.getName(), meta.getCreated(), System.currentTimeMillis());
						worker.execute(sqlOp, meta.getUID(), meta.getName(), meta.getCreated(), System.currentTimeMillis());
					} catch (Exception ex) {
						throw new RuntimeException("DBOp Failure [" + op + "]", ex);
					}
				}
			};			
		}
	},
	/** Saves a metric UIDMeta */
	UIDMETAM("INSERT INTO TSD_METRIC VALUES(?, 1, ?, ?, ?, null, null, null, null)"){
		@Override
		public DBOp newDbOp(final SQLWorker worker, final PreparedStatement ps, final Runnable cachePush, final Object...args) {
			final Object[] opArgs = args;
			return new DBOp(UIDMETAM, cachePush, opArgs) {
				@Override
				public void run() {
					final UIDMeta meta = (UIDMeta)opArgs[0];
					try {
//						worker.batch(ps.getConnection(), ps, sqlOp, meta.getUID(), meta.getName(), meta.getCreated(), System.currentTimeMillis());
						worker.execute(sqlOp, meta.getUID(), meta.getName(), meta.getCreated(), System.currentTimeMillis());
					} catch (Exception ex) {
						throw new RuntimeException("DBOp Failure [" + op + "]", ex);
					}
				}
			};			
		}
	},	
	/** Tag Pair Insert */
	UIDPAIR("INSERT INTO TSD_TAGPAIR VALUES(?,?,?,?)"){
		@Override
		public DBOp newDbOp(final SQLWorker worker, final PreparedStatement ps, final Runnable cachePush, final Object...args) {
			final Object[] opArgs = args;
			return new DBOp(UIDPAIR, cachePush, opArgs) {
				@Override
				public void run() {
					final String pairKey = (String)opArgs[0];
					final String tagKey = (String)opArgs[1];
					final String tagValue = (String)opArgs[2];
					final String name = (String)opArgs[3];
					try {
//						worker.batch(ps.getConnection(), ps, sqlOp, pairKey, tagKey, tagValue, name);
						worker.execute(sqlOp, pairKey, tagKey, tagValue, name);
					} catch (Exception ex) {
						throw new RuntimeException("DBOp Failure [" + op + "]", ex);
					}
				}
			};			
		}
	},
	/** TSMETA Insert */
						//			seq, 1, metric_uid, fqn, tsuid, created, last_up 
	TSMETA("INSERT INTO TSD_TSMETA VALUES(?,1,?,?,?,?,?, null, null, null, null, null, null, null, null, null)"){
		@Override
		public DBOp newDbOp(final SQLWorker worker, final PreparedStatement ps, final Runnable cachePush, final Object...args) {
			final Object[] opArgs = args;
			return new DBOp(TSMETA, cachePush, opArgs) {
				@Override
				public void run() {					
					final String metricUid = (String)opArgs[0];
					final String fqn = (String)opArgs[1];
					final String tsuid = (String)opArgs[2];
					final long[] seqRef = (long[])opArgs[3];
					seqRef[0] = worker.sqlForLong("SELECT nextval('fqn_seq')");
					try {
//						worker.batch(ps.getConnection(), ps, sqlOp, pairKey, tagKey, tagValue, name);
						final Timestamp ts = SystemClock.getTimestamp();
						worker.execute(sqlOp, seqRef[0], metricUid, fqn, tsuid, ts, ts);
					} catch (Exception ex) {
						throw new RuntimeException("DBOp Failure [" + op + "]", ex);
					}
				}
			};			
		}
	},
	/** Inserts into TSD_FQN_TAGPAIR */
	UIDPAIRFQN("INSERT INTO TSD_FQN_TAGPAIR VALUES(?,?,?,?,?)"){
		@Override
		public DBOp newDbOp(final SQLWorker worker, final PreparedStatement ps, final Runnable cachePush, final Object...args) {
			final Object[] opArgs = args;
			return new DBOp(UIDPAIR, cachePush, opArgs) {
				@Override
				public void run() {
					
					final long[] seqRef = (long[])opArgs[0];
					seqRef[0] = worker.sqlForLong("SELECT nextval('fqn_tp_seq')");
					final long fqnId = (Long)opArgs[1];
					final String xuid = (String)opArgs[2];
					final int porder = (Integer)opArgs[3];
					final String node = (String)opArgs[4];
					try {
//						worker.batch(ps.getConnection(), ps, sqlOp, pairKey, tagKey, tagValue, name);
						worker.execute(sqlOp, seqRef[0], fqnId, xuid, porder, node);
					} catch (Exception ex) {
						throw new RuntimeException("DBOp Failure [" + op + "]", ex);
					}
				}
			};			
		}
	},
	/** Inserts annotations */
	//                                         seq, 1, start, updt, null, null, fqnid, endtime, null
	ANNOTATION("INSERT INTO TSD_ANNOTATION VALUES(nextval('ann_seq'),1,?,?,null, null, ?, ?, null)"){
		@Override
		public DBOp newDbOp(final SQLWorker worker, final PreparedStatement ps, final Runnable cachePush, final Object...args) {
			final Object[] opArgs = args;
			return new DBOp(UIDPAIR, cachePush, opArgs) {
				@Override
				public void run() {
					
					final long[] seqRef = (long[])opArgs[0];
					seqRef[0] = worker.sqlForLong("SELECT nextval('fqn_tp_seq')");
					final long fqnId = (Long)opArgs[1];
					final String xuid = (String)opArgs[2];
					final int porder = (Integer)opArgs[3];
					final String node = (String)opArgs[4];
					try {
//						worker.batch(ps.getConnection(), ps, sqlOp, pairKey, tagKey, tagValue, name);
						worker.execute(sqlOp, seqRef[0], fqnId, xuid, porder, node);
					} catch (Exception ex) {
						throw new RuntimeException("DBOp Failure [" + op + "]", ex);
					}
				}
			};			
		}
	};	
	
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
	
	public static final Map<UniqueIdType, MetaDBOp> TYPE2ENUM;
	
	static {
		final Map<UniqueIdType, MetaDBOp> tmp = new EnumMap<UniqueIdType, MetaDBOp>(UniqueIdType.class);
		tmp.put(UniqueIdType.METRIC, UIDMETAM);
		tmp.put(UniqueIdType.TAGK, UIDMETAK);
		tmp.put(UniqueIdType.TAGV, UIDMETAV);
		TYPE2ENUM = Collections.unmodifiableMap(tmp);
	}
	
	private MetaDBOp(final String sqlOp) {
		this.sqlOp = sqlOp;
	}
	
	public static MetaDBOp forType(final UniqueIdType type) {
		return TYPE2ENUM.get(type);
	}
	
	public static String render(final UIDMeta meta) {
		final StringBuilder b = new StringBuilder("UIDMeta[").append(meta.getType()).append("]:");
		
		return b.toString();
	}
}
