package com.hiraeth.govern.client.entity;

import org.hiraeth.govern.common.domain.ServiceInstanceInfo;

import java.util.List;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/2 20:19
 */
public interface ServiceInstanceChangeListener {
    void onChange(List<ServiceInstanceInfo> addressList);
}
