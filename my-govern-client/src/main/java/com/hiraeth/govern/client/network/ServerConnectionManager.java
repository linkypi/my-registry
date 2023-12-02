package com.hiraeth.govern.client.network;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/1 1:24
 */
@Getter
@Setter
@Slf4j
public class ServerConnectionManager {

    private Map<String, ServerConnection> connections = new ConcurrentHashMap<>();

    public void add(ServerConnection connection){
        connections.put(connection.getAddress(), connection);
    }
    public void remove(ServerConnection connection){
        connections.remove(connection.getAddress());
    }

    public ServerConnection get(String key){
        return connections.get(key);
    }
}
