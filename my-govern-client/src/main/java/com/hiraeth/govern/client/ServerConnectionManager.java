package com.hiraeth.govern.client;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

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
        connections.put(connection.getConnectionId(), connection);
    }
    public void remove(ServerConnection connection){
        try {
            connection.getSocketChannel().close();
            log.error("server disconnected, connection id: {}", connection.getConnectionId());
        } catch (IOException e) {
            log.error("close server socket channel occur error, connection id: {}", connection.getConnectionId());
        }
        connections.remove(connection.getConnectionId());
    }

}
