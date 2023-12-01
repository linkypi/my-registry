package com.hiraeth.govern.client.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 18:30
 */
@Setter
@Getter
@Slf4j
@AllArgsConstructor
public class MasterAddress {
    private String host;
    private int port;

    public MasterAddress(String address){
        // 127.0.0.1:2156
        String[] arr = address.split(":");
        if (arr.length == 0) {
            log.warn("address is empty, cannot create master address: {}", address);
            return;
        }
        this.host = arr[0];
        this.port = Integer.parseInt(arr[1]);
    }

    @Override
    public String toString() {
        return this.host + ":" + port;
    }
}
