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

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.RpcPlugin;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

/**
 * <p>Title: JMXRPC</p>
 * <p>Description: An OpenTSDB RPC plugin to expose some basic OpenTSDB management operations via JMX</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.opentsdb.jmx.JMXRPC</code></p>
 */

public class JMXRPC extends RpcPlugin implements JMXRPCMBean {
	private static final Logger LOG = LoggerFactory.getLogger(JMXRPC.class);
	
	/** The injected TSDB instance */
	protected TSDB tsdb = null;
	/** The injected TSDB instance's config */
	protected Config config = null;
	/** The JMXMP connector servers */
	protected Map<String, JMXConnectorServer> connectorServers = new HashMap<String, JMXConnectorServer>();
	/**
	 * Creates a new JMXRPC
	 */
	public JMXRPC() {
		LOG.info("Created JMXRPC plugin instance");
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RpcPlugin#initialize(net.opentsdb.core.TSDB)
	 */
	@Override
	public void initialize(final TSDB tsdb) {
		LOG.info("Initializing JMXRPC plugin....");
		this.tsdb = tsdb;
		config = tsdb.getConfig();
		final Map<String, String> configMap = config.getMap();
		StringBuilder b= new StringBuilder();
		for(Map.Entry<String, String> entry: configMap.entrySet()) {
			if(entry.getKey().trim().toLowerCase().startsWith(CONFIG_JMX_URLS)) {
				final String url = configMap.get(entry.getValue().trim());
				final JMXServiceURL serviceURL = toJMXServiceURL(url);
				final JMXConnectorServer connectorServer = connector(serviceURL);
				connectorServers.put(url, connectorServer);
				b.append("\n\t").append(url);
			}
		}
		startJMXServers();
		LOG.info("Initialized JMXRPC plugin. JMXConnectorServers installed and started at:{}", b.toString());
	}
	
	private void startJMXServers() {
		final AtomicBoolean started = new AtomicBoolean(false);
		final Thread t = new Thread("JMXConnectorServerStarterDaemon") {
			public void run() {
				for(Map.Entry<String, JMXConnectorServer> entry: new HashMap<String, JMXConnectorServer>(connectorServers).entrySet()){
					try {
						entry.getValue().start();
						LOG.info("Started JMXConnectorServer: [{}]", entry.getValue().getAddress());
					} catch (Exception ex) {
						LOG.error("Failed to start JMXConnectorServer [{}]", entry.getKey(), ex);
						connectorServers.remove(entry.getKey());
					}
				}
				started.set(true);
			}
		};
		t.setDaemon(true);
		t.start();
		try {
			t.join(5000);
		} catch (InterruptedException e) {
			/* No Op */
		}
		if(!started.get()) {
			LOG.warn("Not all JMXConnectorServers Started yet");
		}
	}
	
	private JMXServiceURL toJMXServiceURL(final String url) {
		try {
			return new JMXServiceURL(url);
		} catch (Exception ex) {
			throw new IllegalArgumentException("Invalid JMXServiceURL: [" + url + "]", ex);
		}		
	}
	
	private JMXConnectorServer connector(final JMXServiceURL serviceURL) {
		try {
			return JMXConnectorServerFactory.newJMXConnectorServer(serviceURL, null, ManagementFactory.getPlatformMBeanServer());
		} catch (Exception ex) {
			throw new IllegalArgumentException("Unable to create connector server for JMXServiceURL: [" + serviceURL+ "]", ex);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RpcPlugin#shutdown()
	 */
	@Override
	public Deferred<Object> shutdown() {
		for(JMXConnectorServer server: connectorServers.values()) {
			try { server.stop(); } catch (Exception x) {/* No Op */}
		}
		connectorServers.clear();
		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RpcPlugin#version()
	 */
	@Override
	public String version() {
		return "2.1.0";
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RpcPlugin#collectStats(net.opentsdb.stats.StatsCollector)
	 */
	@Override
	public void collectStats(final StatsCollector collector) {
		// TODO Auto-generated method stub

	}

//	/**
//	 * @return
//	 * @see net.opentsdb.core.TSDB#getClient()
//	 */
//	public final HBaseClient getClient() {
//		tsdb.getClient().stats().atomicIncrements()
//		return tsdb.getClient();
//	}

	/**
	 * @return
	 * @see net.opentsdb.core.TSDB#getConfig()
	 */
	public final Config getConfig() {
		return tsdb.getConfig();
	}

	/**
	 * @param type
	 * @param uid
	 * @return
	 * @see net.opentsdb.core.TSDB#getUidName(net.opentsdb.uid.UniqueId.UniqueIdType, byte[])
	 */
	public Deferred<String> getUidName(UniqueIdType type, byte[] uid) {
		return tsdb.getUidName(type, uid);
	}

	/**
	 * @param type
	 * @param name
	 * @return
	 * @see net.opentsdb.core.TSDB#getUID(net.opentsdb.uid.UniqueId.UniqueIdType, java.lang.String)
	 */
	public byte[] getUID(UniqueIdType type, String name) {
		return tsdb.getUID(type, name);
	}

	/**
	 * @return
	 * @see net.opentsdb.core.TSDB#uidCacheHits()
	 */
	public int uidCacheHits() {
		return tsdb.uidCacheHits();
	}

	/**
	 * @return
	 * @see net.opentsdb.core.TSDB#uidCacheMisses()
	 */
	public int uidCacheMisses() {
		return tsdb.uidCacheMisses();
	}

	/**
	 * @return
	 * @see net.opentsdb.core.TSDB#uidCacheSize()
	 */
	public int uidCacheSize() {
		return tsdb.uidCacheSize();
	}

	/**
	 * @param metric
	 * @param timestamp
	 * @param value
	 * @param tags
	 * @return
	 * @see net.opentsdb.core.TSDB#addPoint(java.lang.String, long, double, java.util.Map)
	 */
	public Deferred<Object> addPoint(String metric, long timestamp,
			double value, Map<String, String> tags) {
		return tsdb.addPoint(metric, timestamp, value, tags);
	}

	/**
	 * @return
	 * @throws HBaseException
	 * @see net.opentsdb.core.TSDB#flush()
	 */
	public Deferred<Object> flush() throws HBaseException {
		return tsdb.flush();
	}

	/**
	 * @param search
	 * @return
	 * @see net.opentsdb.core.TSDB#suggestMetrics(java.lang.String)
	 */
	public List<String> suggestMetrics(String search) {
		return tsdb.suggestMetrics(search);
	}

	/**
	 * @param search
	 * @return
	 * @see net.opentsdb.core.TSDB#suggestTagNames(java.lang.String)
	 */
	public List<String> suggestTagNames(String search) {
		return tsdb.suggestTagNames(search);
	}

	/**
	 * @param search
	 * @param max_results
	 * @return
	 * @see net.opentsdb.core.TSDB#suggestTagNames(java.lang.String, int)
	 */
	public List<String> suggestTagNames(String search, int max_results) {
		return tsdb.suggestTagNames(search, max_results);
	}

	/**
	 * @param search
	 * @return
	 * @see net.opentsdb.core.TSDB#suggestTagValues(java.lang.String)
	 */
	public List<String> suggestTagValues(String search) {
		return tsdb.suggestTagValues(search);
	}

	/**
	 * @param search
	 * @param max_results
	 * @return
	 * @see net.opentsdb.core.TSDB#suggestTagValues(java.lang.String, int)
	 */
	public List<String> suggestTagValues(String search, int max_results) {
		return tsdb.suggestTagValues(search, max_results);
	}

	/**
	 * 
	 * @see net.opentsdb.core.TSDB#dropCaches()
	 */
	public void dropCaches() {
		tsdb.dropCaches();
	}

	/**
	 * @param type
	 * @param name
	 * @return
	 * @see net.opentsdb.core.TSDB#assignUid(java.lang.String, java.lang.String)
	 */
	public byte[] assignUid(String type, String name) {
		return tsdb.assignUid(type, name);
	}

	/**
	 * @param tsuid
	 * @see net.opentsdb.core.TSDB#deleteTSMeta(java.lang.String)
	 */
	public void deleteTSMeta(String tsuid) {
		tsdb.deleteTSMeta(tsuid);
	}

	/**
	 * @param meta
	 * @see net.opentsdb.core.TSDB#deleteUIDMeta(net.opentsdb.meta.UIDMeta)
	 */
	public void deleteUIDMeta(UIDMeta meta) {
		tsdb.deleteUIDMeta(meta);
	}

	/**
	 * @param note
	 * @see net.opentsdb.core.TSDB#deleteAnnotation(net.opentsdb.meta.Annotation)
	 */
	public void deleteAnnotation(Annotation note) {
		tsdb.deleteAnnotation(note);
	}

}
