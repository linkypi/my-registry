package com.hiraeth.govern.client.network;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.MessageReader;
import org.hiraeth.govern.common.domain.BaseRequest;
import org.hiraeth.govern.common.domain.BaseResponse;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.UUID;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/1 1:24
 */
@Getter
@Setter
@Slf4j
public class ServerConnection extends MessageReader {

    private SelectionKey selectionKey;
    private String connectionId;
    private String address; // ip:port

    public ServerConnection(SelectionKey selectionKey, SocketChannel socketChannel) throws IOException {
        this.selectionKey = selectionKey;
        this.socketChannel = socketChannel;
        this.connectionId = UUID.randomUUID().toString().replace("-","");
        this.address = socketChannel.getRemoteAddress().toString().replace("/", "");
    }

    public ServerConnection(SocketChannel socketChannel){
        this.connectionId = UUID.randomUUID().toString().replace("-","");
        InetAddress inetAddress = socketChannel.socket().getInetAddress();
        this.address = inetAddress.toString().replace("/","") +":"+ socketChannel.socket().getPort();
    }

    @Override
    protected Object build(ByteBuffer buffer) {
        return BaseResponse.parseFromBuffer(buffer);
    }

    public BaseResponse doReadIO() throws IOException {
        return (BaseResponse)super.doReadIOInternal();
    }
}
