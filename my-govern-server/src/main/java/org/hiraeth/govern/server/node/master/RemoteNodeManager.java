package org.hiraeth.govern.server.node.master;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.entity.RemoteNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 远程节点管理组件
 * @author: lynch
 * @description:
 * @date: 2023/11/28 18:40
 */
@Slf4j
public class RemoteNodeManager {
    private static final Map<Integer, RemoteNode> remoteMasterNodes = new ConcurrentHashMap<>();
    private static final Map<Integer, RemoteNode> remoteSlaveNodes = new ConcurrentHashMap<>();

    public void addRemoteMasterNode(RemoteNode masterNode){
        remoteMasterNodes.put(masterNode.getNodeId(), masterNode);
        log.info("add remote master node: {}", JSON.toJSONString(masterNode));
    }

    public void addRemoteSlaveNode(RemoteNode slaveNode){
        remoteSlaveNodes.put(slaveNode.getNodeId(), slaveNode);
        log.info("add remote slave node: {}", JSON.toJSONString(slaveNode));
    }

    public int getTotalCandidate(){
        return remoteMasterNodes.size();
    }

    public List<RemoteNode> getOtherControllerCandidates(){
        Configuration configuration = Configuration.getInstance();
        int nodeId = configuration.getNodeId();

        List<RemoteNode> otherControllerCandidates = new ArrayList<>();
        for (RemoteNode node: remoteMasterNodes.values()){
            if(node.isControllerCandidate() && node.getNodeId() != nodeId){
                otherControllerCandidates.add(node);
            }
        }
        return otherControllerCandidates;
    }

    public int getQuorum(){
        // 定义 quorum 数量，如若controller候选节点有三个，则quorum = 3 / 2 + 1 = 2
        return remoteMasterNodes.size() / 2 + 1;
    }

}
