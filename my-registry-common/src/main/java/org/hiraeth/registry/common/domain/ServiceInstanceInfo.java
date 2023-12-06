package org.hiraeth.registry.common.domain;

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
public class ServiceInstanceInfo {
    private String serviceName;
    private String instanceIp;
    private int instancePort;
    private volatile Long latestHeartbeatTime;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceInstanceInfo that = (ServiceInstanceInfo) o;
        return instancePort == that.instancePort
                && serviceName.equals(that.serviceName)
                && instanceIp.equals(that.instanceIp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, instanceIp, instancePort);
    }

    public ServiceInstanceInfo(String serviceName, String instanceIp, int instancePort){
        this.instanceIp = instanceIp;
        this.instancePort = instancePort;
        this.serviceName = serviceName;
    }

    public String getServiceInstanceId() {
        return serviceName + "/" + instanceIp + "/" + instancePort;
    }


}
