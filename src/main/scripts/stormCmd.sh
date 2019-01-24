#!/usr/bin/env bash

storm=/usr/leap/3.4.4.0/storm/usr/lib/storm/bin/storm

$storm jar storm-project-1.1.0-release.jar com.test.two.FirstStorm

$storm jar storm-project-1.1.0-release.jar com.test.one.HelloTopology
#本地模式运行命令
$storm jar storm-project-1.1.0-release.jar com.test.three.WordCountTopology Top_1 loc /home/wangjj17/data.txt redis_path /home/wangjj17/out.log
#集群模式运行命令
$storm jar storm-project-1.1.0-release.jar com.test.three.WordCountTopology Top_1 srv /home/wangjj17/data.txt redis_path /home/wangjj17/out.log
#查看集群中topology
$storm list
#手动结束集群中topology
$storm kill TopologyName
