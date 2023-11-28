package org.hiraeth.govern.server.node.master;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.master.entity.RemoteMasterNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 远程Master管理组件
 * @author: lynch
 * @description:
 * @date: 2023/11/28 18:40
 */
@Slf4j
public class RemoteMasterNodeManager {
    private static final Map<Integer, RemoteMasterNode> remoteMasterNodes = new ConcurrentHashMap<>();

    public void addRemoteMasterNode(RemoteMasterNode masterNode){
        remoteMasterNodes.put(masterNode.getNodeId(), masterNode);
        log.info("add remote master node: {}", JSON.toJSONString(masterNode));
    }

    public List<RemoteMasterNode> getOtherControllerCandidates(){
        Configuration configuration = Configuration.getInstance();
        int nodeId = configuration.getNodeId();

        List<RemoteMasterNode> otherControllerCandidates = new ArrayList<>();
        for (RemoteMasterNode node: remoteMasterNodes.values()){
            if(node.isControllerCandidate() && node.getNodeId() != nodeId){
                otherControllerCandidates.add(node);
            }
        }
        return otherControllerCandidates;
    }

}
