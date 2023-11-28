package org.hiraeth.govern.server.node.master;

import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.MasterRole;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/27 17:27
 */
public class MasterNode extends Node{

    private MasterNetworkManager masterNetworkManager;
    /**
     * controller候选者
     */
    protected ControllerCandidate controllerCandidate;

    /**
     * 远程master节点管理组件
     */
    private final RemoteMasterNodeManager remoteMasterNodeManager;

    public MasterNode(){
        this.remoteMasterNodeManager = new RemoteMasterNodeManager();
        this.masterNetworkManager = new MasterNetworkManager(remoteMasterNodeManager);
        this.controllerCandidate = new ControllerCandidate(masterNetworkManager, remoteMasterNodeManager);
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

        // 投票选举 Controller
        Configuration configuration = Configuration.getInstance();
        boolean isControllerCandidate = configuration.isControllerCandidate();
        if(isControllerCandidate) {
            MasterRole masterRole = controllerCandidate.voteForControllerElection();

        }
    }
}
