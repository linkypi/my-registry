package org.hiraeth.registry.common.domain;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
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

    private int port;

    public String getNodeId() {
        return host + ":" + internalPort;
    }

    public String getClientNodeId() {
        return host + ":" + clientTcpPort;
    }

    public ServerAddress(){
    }

    public ServerAddress(String address) {
        parseAddress(address);
    }

    public ServerAddress(String ip, int internalPort, int clientHttpPort, int clientTcpPort) {
        this.host = ip;
        this.internalPort = internalPort;
        this.clientHttpPort = clientHttpPort;
        this.clientTcpPort = clientTcpPort;
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
        this.port = internalPort;
    }

    @Override
    public String toString(){
        return getHost() + ":" + getPort();
    }
}
