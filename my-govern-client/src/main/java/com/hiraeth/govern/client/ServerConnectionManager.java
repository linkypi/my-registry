package com.hiraeth.govern.client;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
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

    private List<ServerConnection> connections = new CopyOnWriteArrayList<>();

    public void add(ServerConnection connection){
        connections.add(connection);
    }

}
