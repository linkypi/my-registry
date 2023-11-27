package org.hiraeth.govern.server.node;


/**
 * @author: lynch
 * @description:
 * @date: 2023/11/27 17:33
 */
public enum NodeStatus {
    INITIALIZING(0),
    RUNNING(1),
    SHUTDOWN(2);
    private int value;
    NodeStatus(int val){
        this.value = val;
    }


}