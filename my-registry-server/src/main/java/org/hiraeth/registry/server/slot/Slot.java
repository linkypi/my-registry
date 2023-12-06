package org.hiraeth.registry.server.slot;

import lombok.Getter;
import lombok.Setter;
import org.hiraeth.registry.common.domain.ServiceInstanceInfo;
import org.hiraeth.registry.server.slot.registry.ServiceRegistry;

import java.util.List;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 17:18
 */
@Getter
@Setter
public class Slot {
    private int index;
    // 是否是副本槽位
    private boolean isReplica;
    private String nodeId;
    private ServiceRegistry serviceRegistry;

    public Slot(){
    }

    public Slot(int index, boolean isReplica, String nodeId){
        this.index = index;
        this.isReplica = isReplica;
        this.nodeId = nodeId;
        this.serviceRegistry = new ServiceRegistry();
    }

    public void registerServiceInstance(ServiceInstanceInfo serviceInstanceInfo){
        serviceRegistry.register(serviceInstanceInfo);
    }

    public void heartbeat(ServiceInstanceInfo serviceInstanceInfo) {
        serviceRegistry.heartbeat(serviceInstanceInfo);
    }

    public List<ServiceInstanceInfo> subscribe(String clientConnectionId, String serviceName) {
        return serviceRegistry.subscribe(clientConnectionId, serviceName);
    }
}
