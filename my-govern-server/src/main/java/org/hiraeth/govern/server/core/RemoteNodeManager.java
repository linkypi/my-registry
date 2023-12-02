package org.hiraeth.govern.server.core;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.ServerAddress;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.entity.RemoteServer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 远程节点管理组件
 * @author: lynch
 * @description:
 * @date: 2023/11/28 18:40
 */
@Slf4j
public class RemoteNodeManager {
    private static final Map<String, RemoteServer> remoteServerNodes = new ConcurrentHashMap<>();

    public List<ServerAddress> getAllOnlineServerAddresses(){
        Configuration configuration = Configuration.getInstance();
        List<ServerAddress> addresses = configuration.getControllerServers().values().stream().filter(
                (a) -> remoteServerNodes.containsKey(a.getNodeId())).collect(Collectors.toList());
        return addresses;
    }

    public void addRemoteServerNode(RemoteServer serverNode){
        remoteServerNodes.put(serverNode.getNodeId(), serverNode);
        log.info("add remote server node: {}", JSON.toJSONString(serverNode));
    }

    public int getTotalCandidate(){
        return remoteServerNodes.size();
    }

    public List<RemoteServer> getAllRemoteServers(){
        return new ArrayList<>(remoteServerNodes.values());
    }

    public List<RemoteServer> getOtherControllerCandidates(){
        Configuration configuration = Configuration.getInstance();
        String nodeId = configuration.getNodeId();

        List<RemoteServer> otherControllerCandidates = new ArrayList<>();
        for (RemoteServer node: remoteServerNodes.values()){
            if(node.isControllerCandidate() && !Objects.equals(node.getNodeId(), nodeId)){
                otherControllerCandidates.add(node);
            }
        }
        return otherControllerCandidates;
    }

    public int getQuorum(){
        // 定义 quorum 数量，如若controller候选节点有三个，则quorum = 3 / 2 + 1 = 2
        return remoteServerNodes.size() / 2 + 1;
    }

}
