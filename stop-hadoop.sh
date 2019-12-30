#! /bin/sh
echo "stopping name node"
/usr/local/hadoop/bin/hdfs --daemon stop namenode
echo "stopping secondarynamenode"
/usr/local/hadoop/bin/hdfs --daemon stop secondarynamenode
echo "stopping datanode"
/usr/local/hadoop/bin/hdfs --daemon stop datanode
echo "stopping resource manager"
/usr/local/hadoop/bin/yarn --daemon stop resourcemanager
echo "stopping nodemanager"
/usr/local/hadoop/bin/yarn --daemon stop nodemanager
