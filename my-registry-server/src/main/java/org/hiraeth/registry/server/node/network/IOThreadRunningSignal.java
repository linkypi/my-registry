package org.hiraeth.registry.server.node.network;

import lombok.Getter;
import lombok.Setter;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/5 9:42
 */
@Getter
@Setter
public class IOThreadRunningSignal {
    private volatile Boolean isRunning;

    public IOThreadRunningSignal(boolean running){
        this.isRunning = running;
    }
}
