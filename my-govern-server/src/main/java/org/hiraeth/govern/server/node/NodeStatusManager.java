package org.hiraeth.govern.server.node;

import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.server.node.entity.ElectionResult;
import org.hiraeth.govern.server.node.entity.MasterRole;
import org.hiraeth.govern.server.node.entity.NodeStatus;
import org.hiraeth.govern.common.domain.SlotRang;
import org.hiraeth.govern.server.node.master.ElectionStage;

import java.util.Map;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/27 22:51
 */
@Getter
@Setter
public class NodeStatusManager {
    private NodeStatusManager() {
    }

    public static class Singleton {
        private static final NodeStatusManager instance = new NodeStatusManager();
    }

    public static NodeStatusManager getInstance() {
        return Singleton.instance;
    }

    private volatile NodeStatus nodeStatus;

    /**
     * 当前controller所在机器id
     */
    private int controllerId;
    private int epoch;

    private ElectionStage.ELStage stage = ElectionStage.ELStage.ELECTING;
    // 新leader开始统领的时间
    private long startTimestamp;
    // 当前机器的角色
    private MasterRole masterRole;
    private Map<Integer, SlotRang> slots;

    public void updateStatus(ElectionResult electionResult){
        this.controllerId = electionResult.getControllerId();
        this.epoch = electionResult.getEpoch();
        this.startTimestamp = electionResult.getTimestamp();
        this.masterRole = electionResult.getMasterRole();
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
