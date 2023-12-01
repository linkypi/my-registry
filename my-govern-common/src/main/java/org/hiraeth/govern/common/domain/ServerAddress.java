package org.hiraeth.govern.common.domain;

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
public class ServerAddress {

    private String host;
    /**
     * 内部通信的端口
     */
    private int internalPort;
    /**
     * 客户端通信的端口
     */
    private int clientTcpPort;
    private int clientHttpPort;

    public String getNodeId() {
        return host + ":" + internalPort;
    }

    public ServerAddress(){
    }

    public ServerAddress(String address) {
        parseAddress(address);
    }

    public void parseAddress(String address){
        // 127.0.0.1:2156
        String[] arr = address.split(":");
        if (arr.length == 0) {
            log.warn("address is empty, cannot create master address: {}", address);
            return;
        }
        this.host = arr[0];
        this.internalPort = Integer.parseInt(arr[1]);
    }
}
