package org.hiraeth.govern.server.node;

import lombok.Getter;
import lombok.Setter;

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
