#!/usr/bin/env bash
echo "=== 1. JAVA VERSION ==="
java -version

echo -e "\n=== 2. MAVEN VERSION ==="
mvn -v

echo -e "\n=== 3. PYTHON & PANDAS VERSION ==="
python3 --version
/root/bda_env/bin/python3 -c "import pandas as pd; print('Pandas version:', pd.__version__)"

echo -e "\n=== 4. APACHE HBASE VERSION ==="
/opt/hbase/bin/hbase version

echo -e "\n=== 5. RUNNING JVM PROCESSES (jps) ==="
jps

echo -e "\n=== 6. HBASE CLUSTER STATUS VIA SHELL ==="
echo "status" | /opt/hbase/bin/hbase shell -n
