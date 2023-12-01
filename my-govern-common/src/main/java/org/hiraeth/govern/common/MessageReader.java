package org.hiraeth.govern.common;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static org.hiraeth.govern.common.constant.Constant.REQUEST_HEADER_LENGTH;

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

    private boolean hasReadLen = false;
    private boolean hasReadPayload = false;
    private ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
    private ByteBuffer payloadBuffer = null;

    protected Object build(ByteBuffer buffer) {
        return null;
    }

    protected Object doReadIOInternal() throws IOException {
        try {
            if (!hasReadLen) {
                socketChannel.read(lengthBuffer);
                // 数据没有读完, 返回继续等待
                if (lengthBuffer.hasRemaining()) {
                    return null;
                }
                hasReadLen = true;
            }

            // 请求长度读取完成, 构建请求
            if (payloadBuffer == null) {
                lengthBuffer.flip();
                int length = lengthBuffer.getInt();
                // 需要减去头部4直接的长度空间
                payloadBuffer = ByteBuffer.allocate(length - REQUEST_HEADER_LENGTH);
                log.info("read payload to buffer, capacity: {}", length);
            }

            if (!hasReadPayload) {
                int read = socketChannel.read(payloadBuffer);
                log.info("read payload {} bytes.", read);

                // 数据没有读完, 返回继续等待
                if (payloadBuffer.hasRemaining()) {
                    return null;
                }
                hasReadPayload = true;

                payloadBuffer.flip();
                Object request = build(payloadBuffer);

                reset();

                return request;
            }
        }catch (IOException ex){
            throw ex;
        }
        catch (Exception ex) {
            log.error("do read IO error", ex);
        }

        return null;
    }

    private void reset(){
        lengthBuffer.clear();
        payloadBuffer = null;

        hasReadLen = false;
        hasReadPayload = false;
    }
}
