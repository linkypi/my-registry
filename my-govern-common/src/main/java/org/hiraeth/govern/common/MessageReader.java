package org.hiraeth.govern.common;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.BaseRequest;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/1 1:55
 */
@Getter
@Setter
@Slf4j
public class MessageReader {

    // 客户端长连接
    protected SocketChannel socketChannel;

    private boolean hasReadRequestLen = false;
    private boolean hasReadRequestPayload = false;
    private ByteBuffer requestLengthBuffer = ByteBuffer.allocate(4);
    private ByteBuffer requestPayloadBuffer = null;

    protected Object build(ByteBuffer buffer) {
        return null;
    }

    protected Object doReadIOInternal() {
        try {
            if (!hasReadRequestLen) {
                socketChannel.read(requestLengthBuffer);
                // 数据没有读完, 返回继续等待
                if (requestLengthBuffer.hasRemaining()) {
                    return null;
                }
                hasReadRequestLen = true;
            }

            // 请求长度读取完成, 构建请求
            if (requestPayloadBuffer == null) {
                requestLengthBuffer.flip();
                int length = requestLengthBuffer.getInt();
                requestPayloadBuffer = ByteBuffer.allocate(length);
                log.info("read payload to buffer, capacity: {}", length);
            }

            if (!hasReadRequestPayload) {
                socketChannel.read(requestPayloadBuffer);
                // 数据没有读完, 返回继续等待
                if (requestPayloadBuffer.hasRemaining()) {
                    return null;
                }
                hasReadRequestPayload = true;
            }

            requestPayloadBuffer.flip();

            Object request = build(requestPayloadBuffer);

            requestLengthBuffer.clear();
            requestPayloadBuffer = null;

            hasReadRequestLen = false;
            hasReadRequestPayload = false;
            return request;
        } catch (Exception ex) {
            log.error("do read IO error", ex);
        }

        return null;
    }
}
