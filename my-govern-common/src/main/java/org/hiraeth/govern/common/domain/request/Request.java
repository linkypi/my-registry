package org.hiraeth.govern.common.domain.request;

import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.domain.Message;
import org.hiraeth.govern.common.domain.MessageType;
import org.hiraeth.govern.common.domain.RequestType;
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

        Request request = new Request();
        request.requestId = buffer.getLong();
        request.timestamp = buffer.getLong();
        request.buffer = buffer;
        request.requestType = type;
        return request;
    }

}
