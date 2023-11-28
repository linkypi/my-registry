package org.hiraeth.govern.server.node.master;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/27 17:27
 */
public class MasterNode extends Node{

    private MasterNetworkManager masterNetworkManager;

    public MasterNode(){
        this.masterNetworkManager = new MasterNetworkManager();
    }

    @Override
    public void start() {
        // 启动线程监听 id 比当前节点id 大的master节点的连接请求
        masterNetworkManager.waitGreaterIdMasterNodeConnect();
        // 主动连接 id 比当前节点id 较小的master节点
        if(!masterNetworkManager.connectLowerIdMasterNodes()){
            return;
        }
        // 等待其他master节点都连接完成
        masterNetworkManager.waitAllTheOtherNodesConnected();
    }
}
