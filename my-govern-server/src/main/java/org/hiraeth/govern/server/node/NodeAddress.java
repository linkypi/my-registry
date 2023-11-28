package org.hiraeth.govern.server.node;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/27 21:49
 */
@Slf4j
@Getter
@Setter
public class NodeAddress {
    private int nodeId;
    // 主机或IP
    private String hostname;
    private int masterMasterPort;
    private int masterSlavaPort;
    /**
     * 外部通信地址
     */
    private int externalPort;

    public NodeAddress(String address) {
        // 1:localhost:2156:2356:2556
        String[] arr = address.split(":");
        if (arr.length == 0) {
            log.warn("address is empty, cannot create node address: {}", address);
            return;
        }
        this.hostname = arr[1];
        this.nodeId = Integer.parseInt(arr[0]);
        this.masterMasterPort = Integer.parseInt(arr[2]);
        this.masterSlavaPort = Integer.parseInt(arr[3]);
        this.externalPort = Integer.parseInt(arr[4]);
    }
}
