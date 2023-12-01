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
public class MasterAddress {

    private int nodeId;
    private String host;
    /**
     * master节点之间通信的端口
     */
    private int masterPort;
    /**
     * slave与master直接通信的端口
     */
    private int slavaPort;
    /**
     * 外部通信地址
     */
    private int externalPort;

    public MasterAddress(){
    }

    public MasterAddress(String address, boolean isServerAddr) {
        if(isServerAddr) {
            parseServerAddress(address);
        }else{
            parseClientAddress(address);
        }
    }

    private void parseServerAddress(String address) {
        // 1:127.0.0.1:2156:2356:2556
        String[] arr = address.split(":");
        if (arr.length == 0) {
            log.warn("address is empty, cannot create node address: {}", address);
            return;
        }
        this.host = arr[1];
        this.nodeId = Integer.parseInt(arr[0]);
        this.masterPort = Integer.parseInt(arr[2]);
        this.slavaPort = Integer.parseInt(arr[3]);
        this.externalPort = Integer.parseInt(arr[4]);
    }

    public void parseClientAddress(String address){
        // 127.0.0.1:2156
        String[] arr = address.split(":");
        if (arr.length == 0) {
            log.warn("address is empty, cannot create master address: {}", address);
            return;
        }
        this.host = arr[0];
        this.externalPort = Integer.parseInt(arr[1]);
    }
}
