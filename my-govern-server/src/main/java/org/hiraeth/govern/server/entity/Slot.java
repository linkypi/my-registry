package org.hiraeth.govern.server.entity;

import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.domain.ServiceInstance;

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

    public void registerServiceInstance(ServiceInstance serviceInstance){
        serviceRegistry.register(serviceInstance);
    }
}
