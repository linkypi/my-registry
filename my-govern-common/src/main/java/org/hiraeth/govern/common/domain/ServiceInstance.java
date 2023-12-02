package org.hiraeth.govern.common.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/2 16:09
 */
@Getter
@Setter
@AllArgsConstructor
public class ServiceInstance {
    private String serviceName;
    private String serviceInstanceIp;
    private int serviceInstancePort;
    private volatile Long latestHeartbeatTime;

    public ServiceInstance(String serviceName, String serviceInstanceIp, int serviceInstancePort){
        this.serviceInstanceIp = serviceInstanceIp;
        this.serviceInstancePort = serviceInstancePort;
        this.serviceName = serviceName;
    }

    public String getServiceInstanceId() {
        return serviceName + "/" + serviceInstanceIp + "/" + serviceInstancePort;
    }


}
