package org.hiraeth.govern.server.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 15:40
 */
@Getter
@Setter
@AllArgsConstructor
public class Message {
    private MessageType messageType;
    private byte[] buffer;
}