package org.hiraeth.govern.server.network;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.MessageReader;
import org.hiraeth.govern.common.domain.Message;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.UUID;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 20:29
 */
@Getter
@Setter
@Slf4j
public class ClientConnection extends MessageReader {

    private SelectionKey selectionKey;
    private String connectionId;

    public ClientConnection(SocketChannel socketChannel, SelectionKey selectionKey){
        this.selectionKey = selectionKey;
        this.socketChannel = socketChannel;
        this.connectionId = UUID.randomUUID().toString().replace("-","");
    }

    public Message doReadIO() throws IOException {
        return super.doReadIOInternal();
    }
}
