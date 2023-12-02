package org.hiraeth.govern.server.registry;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.ServiceInstance;
import org.hiraeth.govern.common.util.CollectionUtil;
import org.hiraeth.govern.server.config.Configuration;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/2 17:30
 */
@Slf4j
public class HeartbeatThread extends Thread {

    public HeartbeatThread() {
    }

    @Override
    public void run() {
        Configuration configuration = Configuration.getInstance();
        int heartbeatCheckInterval = configuration.getHeartbeatCheckInterval();
        int heartbeatTimeoutPeriod = configuration.getHeartbeatTimeoutPeriod();
        ServiceRegistry registry = ServiceRegistry.getInstance();
        while (true) {
            try {
                long now = new Date().getTime();

                List<String> instanceIds = new ArrayList<>();
                Map<String, ServiceInstance> serviceInstances = registry.getServiceInstances();
                for (ServiceInstance instance : serviceInstances.values()) {
                    if (now - instance.getLatestHeartbeatTime() >= heartbeatTimeoutPeriod * 1000L) {
                        List<ServiceInstance> instances = registry.getServiceRegistry().get(instance.getServiceName());
                        if (instances != null) {
                            instances.remove(instance);
                        }
                        instanceIds.add(instance.getServiceInstanceId());
                    }
                }

                if (CollectionUtil.notEmpty(instanceIds)) {
                    for (String key : instanceIds) {
                        serviceInstances.remove(key);
                        log.info("heartbeat timeout, remove service instance: {}", key);
                    }
                }

                Thread.sleep(heartbeatCheckInterval * 1000L);
            } catch (Exception ex) {
                log.error("service instance registry heartbeat thread occur error", ex);
            }
        }
    }
}
