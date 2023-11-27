package org.hiraeth.govern.server.node.master;

import lombok.Getter;
import lombok.Setter;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/27 17:49
 */
@Getter
@Setter
public class ControllerCandidate {
    /**
     * 集群内部节点之间进行网络通信的组件
     */
    private MasterNetworkManager masterNetworkManager;

    public ControllerCandidate(MasterNetworkManager masterNetworkManager){
        this.masterNetworkManager = masterNetworkManager;
    }
}
