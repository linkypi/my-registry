package org.hiraeth.govern.server.node.server;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.entity.MasterRole;
import org.hiraeth.govern.server.node.master.ControllerCandidate;
import org.hiraeth.govern.server.node.master.MasterNetworkManager;
import org.hiraeth.govern.server.node.master.RemoteNodeManager;
import org.hiraeth.govern.server.node.entity.RemoteNode;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/27 17:27
 */
@Slf4j
public class MasterNodeServer extends NodeServer {

    private MasterNetworkManager masterNetworkManager;
    /**
     * controller候选者
     */
    protected ControllerCandidate controllerCandidate;

    /**
     * 远程master节点管理组件
     */
    private final RemoteNodeManager remoteNodeManager;

    public MasterNodeServer(){
        this.remoteNodeManager = new RemoteNodeManager();
        this.masterNetworkManager = new MasterNetworkManager(remoteNodeManager);
        this.controllerCandidate = new ControllerCandidate(masterNetworkManager, remoteNodeManager);
    }

    @Override
    public void start() {

        Configuration configuration = Configuration.getInstance();
        RemoteNode remoteNode = new RemoteNode(configuration.getNodeId(),
                configuration.getNodeType(), configuration.isControllerCandidate());
        remoteNodeManager.addRemoteMasterNode(remoteNode);

        // 启动线程监听 id 比当前节点id 大的master节点的连接请求
        masterNetworkManager.waitGreaterIdMasterNodeConnect();
        // 主动连接 id 比当前节点id 较小的master节点
        if(!masterNetworkManager.connectLowerIdMasterNodes()){
            return;
        }
        // 等待其他master节点都连接完成
        masterNetworkManager.waitAllTheOtherNodesConnected();

        // 投票选举 Controller
        boolean isControllerCandidate = configuration.isControllerCandidate();
        if(isControllerCandidate) {
            MasterRole masterRole = controllerCandidate.voteForControllerElection();

        }
        // 监听slave节点发起的请求
        masterNetworkManager.waitSlaveNodeConnect();
    }
}
