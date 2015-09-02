/**
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
 */
package com.heliosapm.opentsdb.jmx;

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import jsr166e.LongAdder;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.heliosapm.tsdbex.sqlbinder.SQLWorker;
import com.heliosapm.tsdbex.sqlbinder.SQLWorker.ResultSetHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.JMXManagedThreadFactory;
import com.heliosapm.utils.jmx.JMXManagedThreadPool;
import com.heliosapm.utils.reflect.PrivateAccessor;
import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.time.SystemClock.ElapsedTime;
import com.heliosapm.utils.url.URLHelper;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * <p>Title: Probe</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.opentsdb.jmx.Probe</code></p>
 */

public class Probe extends SearchPlugin implements Runnable {
	/** Retain system out */
	protected static final PrintStream OUT = System.out;
	/** Retain system err */
	protected static final PrintStream ERR = System.err;
	
	private static final Charset CHARSET = Charset.forName("ISO-8859-1");
	private static final Logger LOG = LoggerFactory.getLogger(Probe.class);
	
	final HBaseClient client;
	final TSDB tsdb;
	final HikariConfig config;
	final HikariDataSource ds;	
	final SQLWorker sqlWorker;
	
	final int batchSize = 128;
	int batchIndex = 0;
	
	final LongAdder tsMetaCount = new LongAdder();
	final LongAdder metricUidCount = new LongAdder();
	final LongAdder tagVUidCount = new LongAdder();
	final LongAdder tagKUidCount = new LongAdder();
	final LongAdder annotationCount = new LongAdder();
	
	final Map<String, UIDMeta> metricNames = new ConcurrentHashMap<String, UIDMeta>();
	final Map<String, UIDMeta> tagVNames = new ConcurrentHashMap<String, UIDMeta>();
	final Map<String, UIDMeta> tagKNames = new ConcurrentHashMap<String, UIDMeta>();
	
	final Map<UniqueIdType, Set<String>> missingUids = new EnumMap<UniqueIdType, Set<String>>(UniqueIdType.class);
	
	final Map<String, String[]> tagPairKeys = new ConcurrentHashMap<String, String[]>();
	
	final Map<String, Long> fqnKeys = new ConcurrentHashMap<String, Long>();
	
	final BlockingQueue<DBOp> processingQueue = new PriorityBlockingQueue<DBOp>(1024);
	
	/** Cache population SQL for metrics */
	public static final String CACHE_POP_METRIC_SQL = "SELECT XUID, NAME, CREATED FROM TSD_METRIC";
	/** Cache population SQL for tagks */
	public static final String CACHE_POP_TAGK_SQL = "SELECT XUID, NAME, CREATED FROM TSD_TAGK";
	/** Cache population SQL for tagvs */
	public static final String CACHE_POP_TAGV_SQL = "SELECT XUID, NAME, CREATED FROM TSD_TAGV";
	/** Cache population SQL for tag pairs */
	public static final String CACHE_POP_TAGPAIRS_SQL = "SELECT XUID, TAGK, TAGV FROM TSD_TAGPAIR";
	/** Cache population SQL for fqn keys */
	public static final String CACHE_POP_FQNKEYS_SQL = "SELECT TSUID, FQNID FROM TSD_TSMETA";
	
	
	Connection batchConn = null;
	PreparedStatement metricInsertPs  = null;
	PreparedStatement tagKInsertPs  = null;
	PreparedStatement tagVInsertPs  = null;
	PreparedStatement tagPairInsertPs  = null;
	
	Thread processingThread = null;
	
	boolean keepProcessing = true;
	
	
	final JMXManagedThreadFactory threadFactory = (JMXManagedThreadFactory) JMXManagedThreadFactory.newThreadFactory("Probe", true); 
	final JMXManagedThreadPool threadPool = new JMXManagedThreadPool(JMXHelper.objectName(getClass()), "Probe", 32, 32, 100000, 60000, 200, 99);
	final Set<Deferred<Object>> defs = new CopyOnWriteArraySet<Deferred<Object>>();

	
	
	//tagPairKeys
	public void populateTagPairCache() {
		final ElapsedTime et = SystemClock.startClock();
		tagPairKeys.clear();
		sqlWorker.executeQuery(CACHE_POP_TAGPAIRS_SQL, new ResultSetHandler(){
			@Override
			public boolean onRow(final int rowId, final ResultSet rset) {
				try {
					final String xuid = rset.getString(1);
					final String[] pair = new String[]{rset.getString(2), rset.getString(3)};
					tagPairKeys.put(xuid, pair);
				} catch (Exception ex) {
					LOG.error("Row failure in cache pop for tag pairs", ex);
				}
				return true;
			}
		});
		LOG.info("Populated Tag Pair Cache: {}", et.printAvg("Rows", tagPairKeys.size()));
	}
	
	public void populateFQNKeysCache() {
		final ElapsedTime et = SystemClock.startClock();
		fqnKeys.clear();
		// CACHE_POP_FQNKEYS_SQL = "SELECT TSUID, FQNIND FROM TSD_TSMETA";
		sqlWorker.executeQuery(CACHE_POP_FQNKEYS_SQL, new ResultSetHandler(){
			@Override
			public boolean onRow(final int rowId, final ResultSet rset) {
				try {
					final String tsuid = rset.getString(1);
					final long fqnKey = rset.getLong(2);
					fqnKeys.put(tsuid, fqnKey);
				} catch (Exception ex) {
					LOG.error("Row failure in cache pop for fqn keys", ex);
				}
				return true;
			}
		});
		LOG.info("Populated FQN KEYS Cache: {}", et.printAvg("Rows", fqnKeys.size()));
	}
	
	
	//  Map<String, Long> fqnKeys
	
	
	public void populateMetricCache() {
		final ElapsedTime et = SystemClock.startClock();
		metricNames.clear();
		sqlWorker.executeQuery(CACHE_POP_METRIC_SQL, new ResultSetHandler(){
			@Override
			public boolean onRow(final int rowId, final ResultSet rset) {
				try {
					final String uids = rset.getString(1);
					final UIDMeta uid = new UIDMeta(UniqueIdType.METRIC, UniqueId.stringToUid(uids), rset.getString(2));
					uid.setCreated(rset.getTimestamp(3).getTime());
					metricNames.put(uids, uid);
				} catch (Exception ex) {
					LOG.error("Row failure in cache pop for metrics", ex);
				}
				return true;
			}
		});
		LOG.info("Populated Metric UID Cache: {}", et.printAvg("Rows", metricNames.size()));
	}
	
	public void populateTagKCache() {
		final ElapsedTime et = SystemClock.startClock();
		tagKNames.clear();
		sqlWorker.executeQuery(CACHE_POP_TAGK_SQL, new ResultSetHandler(){
			@Override
			public boolean onRow(final int rowId, final ResultSet rset) {
				try {
					final String uids = rset.getString(1);
					final UIDMeta uid = new UIDMeta(UniqueIdType.TAGK, UniqueId.stringToUid(uids), rset.getString(2));
					uid.setCreated(rset.getTimestamp(3).getTime());
					tagKNames.put(uids, uid);
				} catch (Exception ex) {
					LOG.error("Row failure in cache pop for tagks", ex);
				}
				return true;
			}
		});
		LOG.info("Populated TagK UID Cache: {}", et.printAvg("Rows", tagKNames.size()));
	}
	
	public void populateTagVCache() {
		final ElapsedTime et = SystemClock.startClock();
		tagVNames.clear();
		sqlWorker.executeQuery(CACHE_POP_TAGV_SQL, new ResultSetHandler(){
			@Override
			public boolean onRow(final int rowId, final ResultSet rset) {
				try {
					final String uids = rset.getString(1);
					final UIDMeta uid = new UIDMeta(UniqueIdType.TAGV, UniqueId.stringToUid(uids), rset.getString(2));
					uid.setCreated(rset.getTimestamp(3).getTime());
					tagVNames.put(uids, uid);
				} catch (Exception ex) {
					LOG.error("Row failure in cache pop for tagvs", ex);
				}
				return true;
			}
		});
		LOG.info("Populated TagV UID Cache: {}", et.printAvg("Rows", tagVNames.size()));
	}
	
	public void resetCounts() {
		tsMetaCount.reset();
		metricUidCount.reset();
		metricNames.clear();
		tagVUidCount.reset();
		annotationCount.reset();
		tagVNames.clear();
		tagKUidCount.reset();
		tagKNames.clear();
	}
	
	public String reportCounts() {
		final StringBuilder b = new StringBuilder("\n\tObject Counts");
		b.append("\n\t\tTSMetas:").append(tsMetaCount.longValue());
		b.append("\n\t\tMetrics:").append(metricUidCount.longValue());
		b.append("\n\t\tTag Keys:").append(tagKUidCount.longValue());
		b.append("\n\t\tTag Values:").append(tagVUidCount.longValue());
		b.append("\n\t\tAnnotations:").append(annotationCount.longValue());
		return b.toString();
	}
	

	/**
	 * Creates a new Probe
	 * @param client 
	 * @param props 
	 */
	public Probe(final HBaseClient client, final Properties props) {
		threadPool.prestartAllCoreThreads();
		this.client = client;
		try {
			tsdb = new TSDB(client, new Config("opentsdb.conf"));
			//"private SearchPlugin search = null;"
			PrivateAccessor.setFieldValue(tsdb, "search", this);
			config = new HikariConfig(props);
			ds = new HikariDataSource(config);	
			sqlWorker = SQLWorker.getInstance(ds);
			for(UniqueIdType uit: UniqueIdType.values()) {
				missingUids.put(uit, new CopyOnWriteArraySet<String>());
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			throw new RuntimeException(ex);
		}
		LOG.info("Probe Created");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		LOG.info("Probe Test");
		HBaseClient client = null;
		Scanner scanner = null;
		Properties props = null;
		if(args.length==1) {
			props = URLHelper.readProperties(URLHelper.toURL(args[0]));
		}
		try {
			if(props==null) {
				URL url = Probe.class.getClassLoader().getResource("hikari.properties");
				props = URLHelper.readProperties(url);
			}
			LOG.info("DataSource DB: [{}]", props.get("dataSource.databaseName"));
			//client = new HBaseClient("pdk-pt-cltsdb-01");
			client = new HBaseClient("localhost");
			log("Client Created");
			client.ensureTableExists("tsdb".getBytes()).joinUninterruptibly();
			client.ensureTableExists("tsdb-meta".getBytes()).joinUninterruptibly();
			client.ensureTableExists("tsdb-tree".getBytes()).joinUninterruptibly();
			client.ensureTableExists("tsdb-uid".getBytes()).joinUninterruptibly();
			LOG.info("===== All tables exist =====");
			Probe p = new Probe(client, props);
			p.purgeMeta();
			p.metaSync();
//			p.metaSync();
			LOG.info(p.reportCounts());
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		} finally {
			if(scanner!=null) try { scanner.close(); } catch (Exception x) {/* No Op */} 
			if(client!=null) try { client.shutdown().joinUninterruptibly(); log("Disconnected"); } catch (Exception ex) {
				LOG.error("Failed to close client [{}]", client, ex);
			}
		}

	}
	
	  /**
	   * Returns the max metric ID from the UID table
	   * @param tsdb The TSDB to use for data access
	   * @return The max metric ID as an integer value, may be 0 if the UID table
	   * hasn't been initialized or is missing the UID row or metrics column.
	   * @throws IllegalStateException if the UID column can't be found or couldn't
	   * be parsed
	   */
	  static long getMaxMetricID(final TSDB tsdb) {
	    // first up, we need the max metric ID so we can split up the data table
	    // amongst threads.
	    final GetRequest get = new GetRequest(tsdb.uidTable(), new byte[] { 0 });
	    get.family("id".getBytes(CHARSET));
	    get.qualifier("metrics".getBytes(CHARSET));
	    ArrayList<KeyValue> row;
	    try {
	      row = tsdb.getClient().get(get).joinUninterruptibly();
	      if (row == null || row.isEmpty()) {
	        return 0;
	      }
	      final byte[] id_bytes = row.get(0).value();
	      if (id_bytes.length != 8) {
	        throw new IllegalStateException("Invalid metric max UID, wrong # of bytes");
	      }
	      return Bytes.getLong(id_bytes);
	    } catch (Exception e) {
	      throw new RuntimeException("Shouldn't be here", e);
	    }
	  }
	  
	  public void storeUID(final UniqueIdType type, final UIDMeta meta) {
		  final Map<String, UIDMeta> cache = getCache(type);
		  if(!cache.containsKey(meta.getUID())) {
			  synchronized(cache) {
				  if(!cache.containsKey(meta.getUID())) {
					  MetaDBOp.forType(type).newDbOp(sqlWorker, metricInsertPs, null, meta).run();
					  cache.put(meta.getUID(), meta);
				  }
			  }
		  }
	  }

	  
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.search.SearchPlugin#indexTSMeta(net.opentsdb.meta.TSMeta)
		 */
//		@Override
		public Deferred<Object> indexTSMeta(final TSMeta meta) {
			final Deferred<Object> def = new Deferred<Object>();
			threadPool.execute(new Runnable(){
				public void run() {
					tsMetaCount.increment();
					final Set<String> XUIDS = new LinkedHashSet<String>();
					try {
						UIDMeta metric = meta.getMetric();
						storeUID(UniqueIdType.METRIC, metric);
						UIDMeta keyUID = null;
						for(UIDMeta uid: meta.getTags()) {
							if(UniqueIdType.TAGK==uid.getType()) {
								storeUID(UniqueIdType.TAGK, uid);
								keyUID = uid;
							} else {
								storeUID(UniqueIdType.TAGV, uid);
								final String xuid = keyUID.getUID() + uid.getUID();
								XUIDS.add(xuid);
								if(!tagPairKeys.containsKey(xuid)) {
									synchronized(tagPairKeys) {
										if(!tagPairKeys.containsKey(xuid)) {
											MetaDBOp.UIDPAIR.newDbOp(sqlWorker, tagPairInsertPs, null, xuid, keyUID.getUID(), uid.getUID(), keyUID.getName() + "=" + uid.getName()).run();
											tagPairKeys.put(xuid, new String[]{keyUID.getName(), uid.getName()});									
										}
									}
								}
								keyUID = null;
							}
						}
						final String fqn = renderTSMeta(meta);
						final long[] seq = new long[1];
						MetaDBOp.TSMETA.newDbOp(sqlWorker, null, null, metric.getUID(), fqn, meta.getTSUID(), seq).run();
						int x = 0;
						for(Iterator<String> xuidIter = XUIDS.iterator(); xuidIter.hasNext();) {
							x++;
							final String xuid = xuidIter.next();
							xuidIter.remove();
							final long[] pseq = new long[]{-1L};
							MetaDBOp.UIDPAIRFQN.newDbOp(sqlWorker, null, null, pseq, seq[0], xuid, x, XUIDS.isEmpty() ? "B" : "L").run();					
						}
//						Annotation.getAnnotation(tsdb, meta.getTSUID(), 0);
//						LOG.info("Saved TSUID: [{}]", fqn);
					} catch (Exception ex) {
						LOG.info("Unexpected TSMeta Error", ex);
					} finally {
						def.callback(null);
					}
				}
			});			
//			def.addBoth(new Callback<Void, Object>(){
//				@Override
//				public Void call(final Object arg) throws Exception {
//					//defs.remove(def);
//					return null;
//				}
//			});
			defs.add(def);
			return def;
		}
	  
	  
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.search.SearchPlugin#indexTSMeta(net.opentsdb.meta.TSMeta)
		 */
//		@Override
		public Deferred<Object> indexTSMetaX(final TSMeta meta) {
			tsMetaCount.increment();
			try {
				
//				final String pairKey = (String)opArgs[0];
//				final String tagKey = (String)opArgs[1];
//				final String tagValue = (String)opArgs[2];
				
				String fqn = popTSUID(meta.getTSUID());
				if(fqn==null) {
					fqn = fixTSUID(meta.getTSUID());
				}
				
				if(fqn==null) {
					//LOG.error("FAILED TO RESOLVE [{}]", meta.getTSUID());
					return Deferred.fromResult(null);
				}
				
				final TreeMap<String, String[]> pairMap = parseTSUID(meta.getTSUID());
				
				for(Map.Entry<String, String[]> entry : pairMap.entrySet()) {
					final String key = entry.getKey();
					final String[] pair = entry.getValue();
					if(!tagPairKeys.containsKey(key)) {
						synchronized(tagPairKeys) {
							if(!tagPairKeys.containsKey(key)) {
//								LOG.info("TAG Pair Key: [{}]", key);
								final UIDMeta keyUID = get(UniqueIdType.TAGK, pair[0]);
								final UIDMeta valueUID = get(UniqueIdType.TAGV, pair[1]);
								final String names = (keyUID==null ? "?" : keyUID.getName()) + "=" + (valueUID==null ? "?" : valueUID.getName());
								processingQueue.put(MetaDBOp.UIDPAIR.newDbOp(sqlWorker, tagPairInsertPs, null, key, pair[0], pair[1], names));
//								MetaDBOp.UIDPAIR.newDbOp(sqlWorker, tagPairInsertPs, null, key, pair[0], pair[1], names).run();
								
								tagPairKeys.put(key, pair);
							}
						}
					}
				}
				
				final long[] seq = new long[]{-1L};
				final String metricUID = meta.getTSUID().substring(0, 6);
				if(!fqnKeys.containsKey(fqn)) {
					synchronized(fqnKeys) {
						if(!fqnKeys.containsKey(fqn)) {
							processingQueue.put(MetaDBOp.TSMETA.newDbOp(sqlWorker, null, null, metricUID, fqn, meta.getTSUID(), seq));
							MetaDBOp.TSMETA.newDbOp(sqlWorker, null, null, metricUID, fqn, meta.getTSUID(), seq).run();
							fqnKeys.put(meta.getTSUID(), seq[0]);
						}
					}
				}
				final String[] xuids = pairMap.keySet().toArray(new String[0]);
				final int lastXuidIndex = xuids.length-1;
				for(int i = 0; i <= lastXuidIndex; i++) {
//=================================================================================================							
//					final long[] seqRef = (long[])opArgs[0];
//					seqRef[0] = worker.sqlForLong("SELECT nextval('fqn_tp_seq')");
//					final long fqnId = (Long)opArgs[1];
//					final String xuid = (String)opArgs[2];
//					final int porder = (Integer)opArgs[3];
//					final String node = (String)opArgs[4];
//=================================================================================================							
					final long[] pseq = new long[]{-1L};
					processingQueue.put(MetaDBOp.UIDPAIRFQN.newDbOp(sqlWorker, null, null, pseq, seq[0], xuids[i], i, (i==lastXuidIndex) ? "B" : "L"));
					
				}
				
				
//				final String metricUid = (String)opArgs[0];
//				final String fqn = (String)opArgs[1];
//				final String tsuid = (String)opArgs[2];
				
				
			} catch (Exception ex) {
				LOG.error("Failed to enqueue tag pair insert", ex);
			}

			return Deferred.fromResult(null);
		}
		
		
		public TreeMap<String, String[]> parseTSUID(final String tsuid) {
			try {
				int start = 6;
				int end = 18;
				final int tagSeqLen = (tsuid.length() - start)/12;
				final TreeMap<String, String[]> map = new TreeMap<String, String[]>();
				for(int i = 1; i <= tagSeqLen; i++) {
					final String s = tsuid.substring(start,  end);
					final String key = s.substring(0,6);
					final String value = s.substring(6);					
					map.put(s, new String[]{key, value});
				    start += 12;
				    end += 12;				
				}
				return map;
			} catch (Exception ex) {
				LOG.error("Failed to parse TSDUID [{}]", tsuid, ex);
				throw new RuntimeException("Failed to parse TSUID", ex);
			}
		}
		
		public String renderTSMeta(final TSMeta meta) {
			final StringBuilder b = new StringBuilder();
			b.append(meta.getMetric().getName()).append("{");
			boolean inKey = true;
			for(UIDMeta uid: meta.getTags()) {
				if(inKey) {
					b.append(uid.getName()).append("=");
				} else {
					b.append(uid.getName()).append(",");
				}
				inKey = !inKey;
			}
			
			return b.deleteCharAt(b.length()-1).append("}").toString();
		}
		
		public String popTSUID(final String tsuid) {
			final StringBuilder b = new StringBuilder();
			try {
				int start = 6;
				int end = 18;
				final String metric = metricNames.get(tsuid.substring(0, start)).getName();
				b.append(metric).append("{");
				final int tagSeqLen = (tsuid.length() - start)/12;
				
				for(int i = 1; i <= tagSeqLen; i++) {
					final String s = tsuid.substring(start,  end);
					final String key = tagKNames.get(s.substring(0,6)).getName();
					final String value = tagVNames.get(s.substring(6)).getName();					
					b.append(key).append("=").append(value).append(",");
				    start += 12;
				    end += 12;				
				}
				return b.deleteCharAt(b.length()-1).append("}").toString();
			} catch (NullPointerException nex) {
				return null;
			} catch (Exception ex) {
				LOG.error("Failed to pop TSDUID [{}]", tsuid, ex);
				throw new RuntimeException("Failed to pop TSUID", ex);
			}
		}
		
		public String fixTSUID(final String tsuid) {
			final StringBuilder b = new StringBuilder();
			try {
				int start = 6;
				int end = 18;
				final String metric = metricNames.get(tsuid.substring(0, start)).getName();
				b.append(metric).append("{");
				final int tagSeqLen = (tsuid.length() - start)/12;
				
				for(int i = 1; i <= tagSeqLen; i++) {
					final String s = tsuid.substring(start,  end);
					//========================================================================
					final UIDMeta keyMeta = get(UniqueIdType.TAGK, s.substring(0,6));
					final UIDMeta valueMeta = get(UniqueIdType.TAGV, s.substring(6));					
					//========================================================================
					final String key = keyMeta.getName();
					final String value = valueMeta.getName();					
					b.append(key).append("=").append(value).append(",");
				    start += 12;
				    end += 12;				
				}
				return b.deleteCharAt(b.length()-1).append("}").toString();
			} catch (NullPointerException nex) {
				return null;
			} catch (Exception ex) {
				LOG.error("Failed to pop TSDUID [{}]", tsuid, ex);
				throw new RuntimeException("Failed to pop TSUID", ex);
			}
		}
		
		public UIDMeta get(final UniqueIdType type, final String uid) {
			if(missingUids.get(type).contains(uid)) {
				return null;
			}
			final AtomicReference<UIDMeta> ref = new AtomicReference<UIDMeta>(); 
			ref.set(getCache(type).get(uid));
			if(ref.get()!=null) return ref.get();
			sqlWorker.executeQuery(getSql(type), new ResultSetHandler() { 
				public boolean onRow(final int rowId, final ResultSet rset) {
					try {
						final String uids = rset.getString(1);
						ref.set(new UIDMeta(type, UniqueId.stringToUid(uids), rset.getString(2)));
						ref.get().setCreated(rset.getTimestamp(3).getTime());
						getCache(type).put(uids, ref.get());
					} catch (Exception ex) {
						/** No Op */
					}					
					return false;
				}
			}, uid);
			if(ref.get()!=null) return ref.get();
			try {
				ref.set(UIDMeta.getUIDMeta(tsdb, type, uid).join(500));
			} catch (Exception ex) {
				LOG.warn("Failed to get UIDMeta [{}] for XUID [{}]",  type, uid);
			}
			
			if(ref.get()!=null) return ref.get();
			LOG.info("Failed to find UID [{}] for XUID [{}]",  type, uid);
			missingUids.get(type).add(uid);
			return null;			
		}
		
		protected Map<String, UIDMeta> getCache(final UniqueIdType type) {
			switch(type) {
			case METRIC:
				return metricNames;
			case TAGK:
				return tagKNames;
			case TAGV:
				return tagVNames;
			}
			return null;
		}
		
		protected String getSql(final UniqueIdType type) {
			switch(type) {
			case METRIC:
				return CACHE_POP_METRIC_SQL + " WHERE XUID = ?";
			case TAGK:
				return CACHE_POP_TAGK_SQL + " WHERE XUID = ?";
			case TAGV:
				return CACHE_POP_TAGV_SQL + " WHERE XUID = ?";
			}
			return null;
		}
		
		
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.search.SearchPlugin#indexUIDMeta(net.opentsdb.meta.UIDMeta)
		 */
		@Override
		public Deferred<Object> indexUIDMeta(final UIDMeta meta) {
			switch(meta.getType()) {
			case METRIC:
				metricUidCount.increment();
				break;
			case TAGK:
				tagKUidCount.increment();
				break;
			case TAGV:
				tagVUidCount.increment();
				break;
			default:
				break;
				
			}
////			LOG.info("UIDMeta: {}", meta.getName());
//			try {
//				switch(meta.getType()) {
//				case METRIC:					
//					metricUidCount.increment();
//					if(!metricNames.containsKey(meta.getUID())) {
//						synchronized(metricNames) {
//							if(!metricNames.containsKey(meta.getUID())) {
//								processingQueue.put(MetaDBOp.UIDMETAM.newDbOp(sqlWorker, metricInsertPs, null, meta));
////								MetaDBOp.UIDMETAM.newDbOp(sqlWorker, metricInsertPs, null, meta).run();		
//								metricNames.put(meta.getUID(), meta);
//							}
//						}
//					}
//					break;				
//				case TAGK:
//					tagKUidCount.increment();
//					if(!tagKNames.containsKey(meta.getUID())) {
//						synchronized(tagKNames) {
//							if(!tagKNames.containsKey(meta.getUID())) {
//								processingQueue.put(MetaDBOp.UIDMETAK.newDbOp(sqlWorker, tagKInsertPs, null, meta));
////								MetaDBOp.UIDMETAK.newDbOp(sqlWorker, tagKInsertPs, null, meta).run();
//								tagKNames.put(meta.getUID(), meta);
//							}
//						}
//					}
//					break;
//				case TAGV:
//					tagVUidCount.increment();
//					if(!tagVNames.containsKey(meta.getUID())) {
//						synchronized(tagVNames) {
//							if(!tagVNames.containsKey(meta.getUID())) {
//								processingQueue.put(MetaDBOp.UIDMETAV.newDbOp(sqlWorker, tagVInsertPs, null, meta));
////								MetaDBOp.UIDMETAV.newDbOp(sqlWorker, tagVInsertPs, null, meta).run();
//								tagVNames.put(meta.getUID(), meta);
//							}
//						}
//					}					
//					break;					
//				default:
//					break;			
//				}
//			} catch (Exception ex) {
//				LOG.error("Failed to enqueue Op", ex);
//			}
			return Deferred.fromResult(null);
		}
	  
	public void initBatches() {
		try {
			batchConn = ds.getConnection();
			batchConn.setAutoCommit(true);
			metricInsertPs  = batchConn.prepareStatement(MetaDBOp.UIDMETAM.sqlOp);
			tagKInsertPs  = batchConn.prepareStatement(MetaDBOp.UIDMETAK.sqlOp);
			tagVInsertPs  = batchConn.prepareStatement(MetaDBOp.UIDMETAV.sqlOp);
			tagPairInsertPs  = batchConn.prepareStatement(MetaDBOp.UIDPAIR.sqlOp);
		} catch (Exception ex) {
			LOG.error("Failed to init batches", ex);
			throw new RuntimeException("Failed to init batches", ex);
		}
		
	}
	
	public void metaSync() {
		try {
			resetCounts();
			LOG.info("Populating Caches.....");
			populateMetricCache();
			populateTagKCache();
			populateTagVCache();
			populateTagPairCache();
			populateFQNKeysCache();
			LOG.info("Initializing SQLs.....");
			initBatches();
//			LOG.info("Starting processing thread....");
//			processingThread = new Thread(this, "ProcessingThread");
//			processingThread.start();
			LOG.info("Starting synchronizeFromStore.....");
			final long start = System.currentTimeMillis();
			Class<?> uidManagerClazz =  Class.forName("net.opentsdb.tools.UidManager");
			Method method = uidManagerClazz.getDeclaredMethod("metaSync", TSDB.class);
			method.setAccessible(true);
			Number x = (Number)method.invoke(null, tsdb);
//			LOG.info("Waiting for tasks to enqueue. So far: {}", defs.size());
//			SystemClock.sleep(20000);
			LOG.info("Waiting for {} tasks to complete....", defs.size());
			final Deferred<ArrayList<Object>> masterDef = Deferred.group(defs);
			masterDef.joinUninterruptibly();
			defs.clear();
			final long end = System.currentTimeMillis();
			final long elapsed = end - start;
			LOG.info("Tasks Complete: {} ms.", elapsed);
			LOG.info("MetaSync Result:" + x);
			LOG.info("metaSync complete in {}: result {}", elapsed, x);
			final Annotation ann = new Annotation();
			ann.setStartTime(start/1000);
			ann.setEndTime(end/1000);
			ann.setDescription("MetaSync Elapsed Time");
			ann.syncToStorage(tsdb,  true).joinUninterruptibly(5000);
//			LOG.info("Waiting for processing thread....");
//			processingThread.join(120000);
			LOG.info("Processing Complete");
		} catch (Exception ex) {
			LOG.error("MetaSync Failed", ex);
		}		
	}
	
	public void run() {
//		final int batchSize = 128;
//		int batchIndex = 0;
		final Set<Runnable> cacheOpRunnables = new HashSet<Runnable>(batchSize);
		while(keepProcessing) {
			try {
				final DBOp dbOp = processingQueue.take();
				LOG.info("------------------------------------------> [{}:{}]", dbOp.op, Arrays.toString(dbOp.getArgs()));
				dbOp.run();
//				final DBOp dbOp = processingQueue.take();
//				final Runnable r = dbOp.cacheOp;
//				if(r!=null) {
//					cacheOpRunnables.add(r);
//				}
//				dbOp.run();
//				batchIndex++;
//				if(batchIndex==batchSize) {
//					LOG.info("Commiting Batch....");
//					execBatch();
//					batchConn.commit();
//					LOG.info("Batch Commited");
//					batchIndex = 0;
//					for(Runnable rq: cacheOpRunnables) {
//						rq.run();
//					}
//					LOG.info("CacheOps Executed");
//				}
			} catch (InterruptedException iex) {
				if(!keepProcessing) break;
				LOG.error("Processing Thread Interrupted", iex);
				if(Thread.interrupted()) Thread.interrupted();
				continue;
			} catch (Exception ex) {
				LOG.error("Processing Thread Op Error", ex);
			}			
		}
		LOG.info("Processing Thread Terminated");
	}
	
	public void execBatch() throws SQLException {
		try {
			metricInsertPs.executeBatch();
			tagKInsertPs.executeBatch();
			tagVInsertPs.executeBatch();
			tagPairInsertPs.executeBatch();
		} catch (SQLException sex) {
			LOG.error("Failed to execute batch", sex);
			final Exception ex = sex.getNextException();
			if(ex!=null) {
				LOG.error("execute batch failure cause", ex);
			}
		}
		
	}
	
	public void purgeMeta() {
		try {
			resetCounts();
			LOG.info("Purging Meta.....");
			final long start = System.currentTimeMillis();
			Class<?> uidManagerClazz =  Class.forName("net.opentsdb.tools.UidManager");
			Method method = uidManagerClazz.getDeclaredMethod("metaPurge", TSDB.class);
			method.setAccessible(true);
			Number x = (Number)method.invoke(null, tsdb);
			LOG.info("metaPurge Result:" + x);
			long elapsed = System.currentTimeMillis() - start;
			LOG.info("metaPurge complete in {}: {}", elapsed, x);
			LOG.info(reportCounts());
		} catch (Exception ex) {
			LOG.error("metaPurge Failed", ex);
		}		
	}
	
	
	/**
	 * Out printer
	 * @param fmt the message format
	 * @param args the message values
	 */
	public static void log(String fmt, Object...args) {
		OUT.println(String.format(fmt, args));
	}
	
	/**
	 * Err printer
	 * @param fmt the message format
	 * @param args the message values
	 */
	public static void loge(String fmt, Object...args) {
		ERR.print(String.format(fmt, args));
		if(args!=null && args.length>0 && args[0] instanceof Throwable) {
			ERR.println("  Stack trace follows:");
			((Throwable)args[0]).printStackTrace(ERR);
		} else {
			ERR.println("");
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.search.SearchPlugin#initialize(net.opentsdb.core.TSDB)
	 */
	@Override
	public void initialize(TSDB tsdb) {
		LOG.info("Search Plugin Initialized");		
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.search.SearchPlugin#shutdown()
	 */
	@Override
	public Deferred<Object> shutdown() {
		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.search.SearchPlugin#version()
	 */
	@Override
	public String version() {
		return "X.X";
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.search.SearchPlugin#collectStats(net.opentsdb.stats.StatsCollector)
	 */
	@Override
	public void collectStats(StatsCollector collector) {
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.search.SearchPlugin#deleteTSMeta(java.lang.String)
	 */
	@Override
	public Deferred<Object> deleteTSMeta(String tsuid) {
		return Deferred.fromResult(null);
	}


	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.search.SearchPlugin#deleteUIDMeta(net.opentsdb.meta.UIDMeta)
	 */
	@Override
	public Deferred<Object> deleteUIDMeta(UIDMeta meta) {
		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.search.SearchPlugin#indexAnnotation(net.opentsdb.meta.Annotation)
	 */
	@Override
	public Deferred<Object> indexAnnotation(Annotation note) {
		annotationCount.increment();
		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.search.SearchPlugin#deleteAnnotation(net.opentsdb.meta.Annotation)
	 */
	@Override
	public Deferred<Object> deleteAnnotation(Annotation note) {
		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.search.SearchPlugin#executeQuery(net.opentsdb.search.SearchQuery)
	 */
	@Override
	public Deferred<SearchQuery> executeQuery(SearchQuery query) {
		return Deferred.fromResult(null);
	}	

}
