package org.hiraeth.govern.server.entity;

import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.domain.ServiceInstanceInfo;
import org.hiraeth.govern.server.registry.ServiceRegistry;

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
    private ServiceRegistry serviceRegistry;

    public Slot(){
    }

    public Slot(int index){
        this.index = index;
        this.serviceRegistry = ServiceRegistry.getInstance();
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
