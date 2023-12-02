package org.hiraeth.govern.common.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceInstance that = (ServiceInstance) o;
        return serviceInstancePort == that.serviceInstancePort
                && serviceName.equals(that.serviceName)
                && serviceInstanceIp.equals(that.serviceInstanceIp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, serviceInstanceIp, serviceInstancePort);
    }

    public ServiceInstance(String serviceName, String serviceInstanceIp, int serviceInstancePort){
        this.serviceInstanceIp = serviceInstanceIp;
        this.serviceInstancePort = serviceInstancePort;
        this.serviceName = serviceName;
    }

    public String getServiceInstanceId() {
        return serviceName + "/" + serviceInstanceIp + "/" + serviceInstancePort;
    }


}
