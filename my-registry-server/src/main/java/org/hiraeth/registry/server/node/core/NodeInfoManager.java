package org.hiraeth.registry.server.node.core;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.registry.common.domain.NodeSlotInfo;
import org.hiraeth.registry.server.config.Configuration;
import org.hiraeth.registry.server.entity.request.ElectionResult;
import org.hiraeth.registry.server.entity.ServerRole;
import org.hiraeth.registry.server.entity.NodeStatus;

import java.util.Objects;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/27 22:51
 */
@Getter
@Setter
@Slf4j
public class NodeInfoManager {
    private NodeInfoManager() {
    }

    public static class Singleton {
        private static final NodeInfoManager instance = new NodeInfoManager();
    }

    public static NodeInfoManager getInstance() {
        return Singleton.instance;
    }

    private volatile NodeStatus nodeStatus;

    /**
     * 当前controller所在机器id
     */
    private String controllerId;
    private int epoch;

    private ElectionStage.ELStage stage = ElectionStage.ELStage.ELECTING;
    // 新leader开始统领的时间
    private long startTimestamp;
    // 当前机器的角色
    private ServerRole serverRole;

    private NodeSlotInfo nodeSlotInfo;

    public void updateStatus(ElectionResult electionResult, ElectionStage.ELStage elStage){
        this.controllerId = electionResult.getControllerId();
        this.epoch = electionResult.getEpoch();
        this.startTimestamp = electionResult.getTimestamp();
        this.serverRole = electionResult.getServerRole();
        this.stage = elStage;
    }

    public ServerRole updateToLeading(ElectionResult electionResult) {
        ElectionStage.setStatus(ElectionStage.ELStage.LEADING);
        String leaderId = electionResult.getControllerId();
        ServerRole serverRole = ServerRole.Candidate;
        String currentNodeId = Configuration.getInstance().getNodeId();
        if (Objects.equals(currentNodeId, leaderId)) {
            serverRole = ServerRole.Controller;
            log.info("leader start on current node, epoch {} !!!", electionResult.getEpoch());
        }
        electionResult.setServerRole(serverRole);

        // update current node status
        NodeInfoManager nodeInfoManager = NodeInfoManager.getInstance();
        nodeInfoManager.updateStatus(electionResult, ElectionStage.ELStage.LEADING);
        return serverRole;
    }

    public void setElectingStage(){
        this.stage = ElectionStage.ELStage.ELECTING;
    }

    public static NodeStatus getNodeStatus() {
        return getInstance().nodeStatus;
    }

    public static void setNodeStatus(NodeStatus nodeStatus) {
        getInstance().nodeStatus = nodeStatus;
    }

    public static boolean isRunning() {
        return getNodeStatus() == NodeStatus.RUNNING;
    }

    public static void setFatal() {
        setNodeStatus(NodeStatus.FATAL);
    }

    public static boolean isFatal() {
        return getNodeStatus() == NodeStatus.FATAL;
    }
}
