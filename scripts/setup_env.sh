#!/usr/bin/env bash
set -e

echo "=== 1. Updating APT and installing system packages ==="
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y openjdk-11-jdk maven python3 python3-pip python3-venv curl wget unzip pandoc

echo "=== 2. Configuring OpenJDK 11 as default Java ==="
JAVA_11_BIN=$(update-alternatives --list java | grep "java-11" | head -n 1)
if [ -n "$JAVA_11_BIN" ]; then
    update-alternatives --set java "$JAVA_11_BIN"
fi
JAVA_BIN=$(readlink -f $(which java))
JAVA_DIR=$(dirname $(dirname "$JAVA_BIN"))
echo "Detected JAVA_HOME: $JAVA_DIR"

echo "=== 3. Setting environment variables in /root/.bashrc ==="
grep -q "JAVA_HOME" /root/.bashrc || echo "export JAVA_HOME=$JAVA_DIR" >> /root/.bashrc
grep -q "HBASE_HOME" /root/.bashrc || echo "export HBASE_HOME=/opt/hbase" >> /root/.bashrc
grep -q "/opt/hbase/bin" /root/.bashrc || echo "export PATH=\$PATH:/opt/hbase/bin:\$JAVA_HOME/bin" >> /root/.bashrc

export JAVA_HOME="$JAVA_DIR"
export HBASE_HOME=/opt/hbase
export PATH=$PATH:/opt/hbase/bin:$JAVA_HOME/bin

echo "=== 4. Setting up Python virtual environment ==="
VENV_PATH="/root/bda_env"
if [ ! -d "$VENV_PATH" ]; then
    python3 -m venv "$VENV_PATH"
fi
"$VENV_PATH/bin/pip" install --upgrade pip
"$VENV_PATH/bin/pip" install pandas

echo "=== 5. Downloading and installing Apache HBase 2.5.x ==="
HBASE_VER="2.5.16"
if [ ! -d "/opt/hbase" ] || [ ! -f "/opt/hbase/bin/hbase" ]; then
    mkdir -p /opt/hbase
    TAR_NAME="hbase-${HBASE_VER}-bin.tar.gz"
    DOWNLOAD_URL="https://dlcdn.apache.org/hbase/${HBASE_VER}/${TAR_NAME}"
    echo "Downloading HBase from fast CDN: $DOWNLOAD_URL ..."
    curl -fSL "$DOWNLOAD_URL" -o "/tmp/${TAR_NAME}" || {
        echo "Fallback to archive..."
        curl -fSL "https://archive.apache.org/dist/hbase/${HBASE_VER}/${TAR_NAME}" -o "/tmp/${TAR_NAME}"
    }
    tar -xzf "/tmp/${TAR_NAME}" -C /opt/hbase --strip-components=1
    rm -f "/tmp/${TAR_NAME}"
fi

echo "=== 6. Configuring HBase ==="
mkdir -p /root/hbase-data /root/zookeeper-data /root/hbase-tmp

# Set JAVA_HOME in hbase-env.sh
sed -i "s|^# export JAVA_HOME=.*|export JAVA_HOME=${JAVA_DIR}|" /opt/hbase/conf/hbase-env.sh
if ! grep -q "export JAVA_HOME=${JAVA_DIR}" /opt/hbase/conf/hbase-env.sh; then
    echo "export JAVA_HOME=${JAVA_DIR}" >> /opt/hbase/conf/hbase-env.sh
fi

# Configure hbase-site.xml
cat << 'EOF' > /opt/hbase/conf/hbase-site.xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>hbase.cluster.distributed</name>
    <value>false</value>
  </property>
  <property>
    <name>hbase.tmp.dir</name>
    <value>/root/hbase-tmp</value>
  </property>
  <property>
    <name>hbase.rootdir</name>
    <value>file:///root/hbase-data</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.dataDir</name>
    <value>/root/zookeeper-data</value>
  </property>
  <property>
    <name>hbase.unsafe.stream.capability.enforce</name>
    <value>false</value>
  </property>
</configuration>
EOF

echo "=== Setup Environment Completed Successfully ==="
