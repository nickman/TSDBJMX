# TSDBJMX
Simple TSDB JMX Remoting for OpenTSDB

## Maven Build
1. Build OpenTSDB JAR
 1.  git clone https://github.com/OpenTSDB/opentsdb.git
 2.  cd opentsdb
 3.  ./build.sh pom.xml
 4.  mvn -DskipTests -Dgpg.skip=true clean install
2. Build heliosutils
3. Build TSDBJMX
 1. git clone https://github.com/nickman/TSDBJMX.git
 2. mvn -DskipTests clean install
4. Set plugin classpath in opentsdb.conf if not already set:
     `tsd.core.plugin_path=<your plugin directory>`
5. Copy TSDBJMX-1.0-SNAPSHOT.jar to &lt;your plugin directory&gt;
6. Add the class name of the plugin in opentsdb.conf:
     `tsd.rpc.plugins=<class-name>`
