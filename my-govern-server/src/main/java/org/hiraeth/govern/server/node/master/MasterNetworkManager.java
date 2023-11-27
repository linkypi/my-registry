package org.hiraeth.govern.server.node.master;

/**
 * @author: lynch
 * @description: 集群内部节点之间进行网络通信的组件
 * @date: 2023/11/27 20:05
 */
public class MasterNetworkManager {

    /**
     * 主动连接比自身node.id 更小的 maser 节点
     * 为防止连接过多，所有master节点的连接顺序为 1 <- 2 <- 3,
     * 即只有node.id较大的节点主动连接id更小的节点
     * @return
     */
    public boolean connectBeforeMasterNodes(){
        return  false;
    }

    /**
     * 等待 node.id 比当前节点 node.id 大节点连接
     */
    public void waitAfterMasterNodeConnect(){

    }
}
