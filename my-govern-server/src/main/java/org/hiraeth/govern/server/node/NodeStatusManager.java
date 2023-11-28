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
    private NodeStatusManager(){}
    public static class Singleton{
        private static NodeStatusManager instance = new NodeStatusManager();
    }

    public static NodeStatusManager getInstance(){
        return Singleton.instance;
    }

    private volatile NodeStatus nodeStatus;
}
