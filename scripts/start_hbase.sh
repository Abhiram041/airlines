#!/usr/bin/env bash
JAVA_BIN=$(readlink -f $(which java 2>/dev/null || echo "/usr/lib/jvm/java-11-openjdk-amd64/bin/java"))
export JAVA_HOME=$(dirname $(dirname "$JAVA_BIN"))
export HBASE_HOME=/opt/hbase
export PATH=$PATH:/opt/hbase/bin:$JAVA_HOME/bin

echo "Starting/Restarting Apache HBase via systemd..."
systemctl restart hbase
sleep 5

echo "Checking running JVM processes (jps):"
jps

echo "Checking HBase Shell status:"
echo "status" | /opt/hbase/bin/hbase shell -n
