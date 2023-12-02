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

}
