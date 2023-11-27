package org.hiraeth.govern.server.node.master;

import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.server.node.NodeStatus;

/**
 * 实例节点
 * @author: lynch
 * @description:
 * @date: 2023/11/27 17:38
 */
@Getter
@Setter
public class Node {
    private NodeStatus nodeStatus;
    private String ip;
    private int port;
    private int port2;
    /**
     * controller候选者
     */
    private ControllerCandidate controllerCandidate;
    /**
     * 集群内部节点之间进行网络通信的组件
     */
    private MasterNetworkManager masterNetworkManager;

    public Node(){
        this.nodeStatus = NodeStatus.INITIALIZING;
        this.controllerCandidate = new ControllerCandidate(masterNetworkManager);
    }

    protected void start(){

        // 初始化所有Master节点直接的网络连接
    }
}
