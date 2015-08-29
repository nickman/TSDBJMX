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
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import jsr166e.LongAdder;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.heliosapm.tsdbex.sqlbinder.SQLWorker;
import com.heliosapm.utils.reflect.PrivateAccessor;
import com.heliosapm.utils.url.URLHelper;
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

public class Probe extends SearchPlugin {
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
	
	final LongAdder tsMetaCount = new LongAdder();
	final LongAdder metricUidCount = new LongAdder();
	final LongAdder tagVUidCount = new LongAdder();
	final LongAdder tagKUidCount = new LongAdder();
	
	final Map<String, UIDMeta> metricNames = new ConcurrentHashMap<String, UIDMeta>();
	final Map<String, UIDMeta> tagVNames = new ConcurrentHashMap<String, UIDMeta>();
	final Map<String, UIDMeta> tagKNames = new ConcurrentHashMap<String, UIDMeta>();
	
	final Map<String, String[]> tagPairKeys = new ConcurrentHashMap<String, String[]>();
	final Map<String, Long> fqnKeys = new ConcurrentHashMap<String, Long>();
	
	
	
	public void resetCounts() {
		tsMetaCount.reset();
		metricUidCount.reset();
		metricNames.clear();
		tagVUidCount.reset();
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
		return b.toString();
	}
	

	/**
	 * Creates a new Probe
	 */
	public Probe(final HBaseClient client, final Properties props) {
		this.client = client;
		try {
			tsdb = new TSDB(client, new Config("opentsdb.conf"));
			//"private SearchPlugin search = null;"
			PrivateAccessor.setFieldValue(tsdb, "search", this);
			config = new HikariConfig(props);
			ds = new HikariDataSource(config);	
			sqlWorker = SQLWorker.getInstance(ds);
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
	  
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.search.SearchPlugin#indexTSMeta(net.opentsdb.meta.TSMeta)
		 */
		@Override
		public Deferred<Object> indexTSMeta(TSMeta meta) {
			tsMetaCount.increment();
			LOG.info("TSUID: [{}]", meta.getTSUID());
			return Deferred.fromResult(null);
		}
		
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.search.SearchPlugin#indexUIDMeta(net.opentsdb.meta.UIDMeta)
		 */
		@Override
		public Deferred<Object> indexUIDMeta(final UIDMeta meta) {
//			LOG.info("UIDMeta: {}", meta.getName());
			switch(meta.getType()) {
			case METRIC:
				metricUidCount.increment();
				metricNames.put(meta.getUID(), meta);
				sqlWorker.executeUpdate("INSERT INTO TSD_METRIC VALUES(?, 1, ?, ?, ?, null, null, null, null)", meta.getUID(), meta.getName(), meta.getCreated(), System.currentTimeMillis());
				break;				
			case TAGK:
				tagKUidCount.increment();
				tagKNames.put(meta.getUID(), meta);
				sqlWorker.executeUpdate("INSERT INTO TSD_TAGK VALUES(?, 1, ?, ?, ?, null, null, null, null)", meta.getUID(), meta.getName(), meta.getCreated(), System.currentTimeMillis());
				break;
			case TAGV:
				tagVUidCount.increment();
				tagVNames.put(meta.getUID(), meta);
				sqlWorker.executeUpdate("INSERT INTO TSD_TAGV VALUES(?, 1, ?, ?, ?, null, null, null, null)", meta.getUID(), meta.getName(), meta.getCreated(), System.currentTimeMillis());
				break;
			default:
				break;			
			}
			return Deferred.fromResult(null);
		}
	  
	
	public void metaSync() {
		try {
			resetCounts();
			LOG.info("Starting synchronizeFromStore.....");
			final long start = System.currentTimeMillis();
			Class<?> uidManagerClazz =  Class.forName("net.opentsdb.tools.UidManager");
			Method method = uidManagerClazz.getDeclaredMethod("metaSync", TSDB.class);
			method.setAccessible(true);
			Number x = (Number)method.invoke(null, tsdb);
			long elapsed = System.currentTimeMillis() - start;
			LOG.info("MetaSync Result:" + x);
			LOG.info("metaSync complete in {}: {}", elapsed, x);
			
		} catch (Exception ex) {
			LOG.error("MetaSync Failed", ex);
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
