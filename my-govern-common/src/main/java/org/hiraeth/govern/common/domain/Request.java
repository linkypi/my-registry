package org.hiraeth.govern.common.domain;

import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.snowflake.SnowFlakeIdUtil;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 22:29
 */
@Getter
@Setter
public class Request extends Message {
    public Request(){
        super();
        messageType = MessageType.REQUEST;
        this.requestId = SnowFlakeIdUtil.getNextId();
    }

    public void buildBuffer() {
        buildBufferInternal(0);
    }

    public static Request toRequest(ByteBuffer buffer) {
        int type = buffer.getInt();
        RequestType requestType = RequestType.of(type);

        Request request = new Request();
        request.requestId = buffer.getLong();
        request.timestamp = buffer.getLong();
        request.buffer = buffer;
        request.requestType = requestType;
        return request;
    }

}
