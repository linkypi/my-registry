

#### controller选举问题待解决
目前controller选举是一种参考zk的简单选举算法, 后续可使用 [sofa-jraft](https://github.com/sofastack/sofa-jraft) 基于Raft协议的具体实现替代
1. 选举过程中必须三个及以上节点才可以产生leader, 而zookeeper在仅有两个节点时也可以产生leader.
另外, zk 各个节点之间是如何发起新一轮选举, 是否有必要通知其他各节点同步当前选举轮次?
2. 各个master节点互联成功后, 是否有必要发送心跳给对方, 已确保master在线?
