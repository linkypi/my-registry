package org.hiraeth.govern.server.slot.registry;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.ServiceInstanceInfo;
import org.hiraeth.govern.common.util.CollectionUtil;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.slot.Slot;
import org.hiraeth.govern.server.slot.SlotManager;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/2 17:30
 */
@Slf4j
public class SlotHeartbeatThread extends Thread {

    private SlotManager slotManager;

    public SlotHeartbeatThread(SlotManager slotManager) {
        this.slotManager = slotManager;
    }

    @Override
    public void run() {
        Configuration configuration = Configuration.getInstance();
        int heartbeatCheckInterval = configuration.getHeartbeatCheckInterval();
        int heartbeatTimeoutPeriod = configuration.getHeartbeatTimeoutPeriod();

        while (true) {
            try {
                long now = new Date().getTime();

                List<String> instanceIds = new ArrayList<>();
                List<Slot> slots = slotManager.getSlots().stream().filter(a->!a.isReplica()).collect(Collectors.toList());
                for (Slot slot : slots) {
                    Map<String, ServiceInstanceInfo> serviceInstances = slot.getServiceRegistry().getServiceInstances();
                    for (ServiceInstanceInfo instance : serviceInstances.values()) {
                        if (instance.getLatestHeartbeatTime() == null) {
                            instance.setLatestHeartbeatTime(new Date().getTime());
                            continue;
                        }
                        if (now - instance.getLatestHeartbeatTime() >= heartbeatTimeoutPeriod * 1000L) {
                            slot.getServiceRegistry().remove(instance);
                            instanceIds.add(instance.getServiceInstanceId());
                        }
                    }

                    if (CollectionUtil.notEmpty(instanceIds)) {
                        for (String key : instanceIds) {
                            serviceInstances.remove(key);
                            log.info("heartbeat timeout, remove service instance: {}", key);
                        }
                    }
                }

                Thread.sleep(heartbeatCheckInterval * 1000L);
            } catch (Exception ex) {
                log.error("service instance registry heartbeat thread occur error", ex);
            }
        }
    }
}
