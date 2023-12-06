package org.hiraeth.registry.common;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.registry.common.domain.Message;
import org.hiraeth.registry.common.domain.request.Request;
import org.hiraeth.registry.common.domain.response.Response;
import org.hiraeth.registry.common.domain.MessageType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static org.hiraeth.registry.common.constant.Constant.REQUEST_HEADER_LENGTH;

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

    /**
     * 首先读取头部消息长度，然后再读取实际消息
     * @return
     * @throws IOException
     */
    protected Message doReadIOInternal() throws IOException {
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
//                log.info("read payload to buffer, capacity: {}", length);
            }

            if (!hasReadPayload) {
                socketChannel.read(payloadBuffer);

                // 数据没有读完, 返回继续等待
                if (payloadBuffer.hasRemaining()) {
                    return null;
                }
                hasReadPayload = true;

                payloadBuffer.flip();

                int msgType = payloadBuffer.getInt();
                MessageType messageType = MessageType.of(msgType);
                Message message = null;
                if(messageType == MessageType.REQUEST){
                    message = Request.toRequest(payloadBuffer);
                    message.setMessageType(MessageType.REQUEST);
                }else if(messageType == MessageType.RESPONSE){
                    message = Response.toResponse(payloadBuffer);
                    message.setMessageType(MessageType.RESPONSE);
                }else{
                    log.error("unknown message type: {}.", msgType);
                }

                reset();

                return message;
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
