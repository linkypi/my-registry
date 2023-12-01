package org.hiraeth.govern.common.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 22:44
 */
@Getter
@Setter
@AllArgsConstructor
public class Request {
    private RequestType requestType;
    private long requestId;
    private ByteBuffer buffer;
}
