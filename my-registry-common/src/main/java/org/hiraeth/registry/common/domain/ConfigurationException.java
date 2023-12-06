package org.hiraeth.registry.common.domain;

/**
 * @author: leo
 * @description:
 * @ClassName: org.hiraeth.govern.server.config
 * @date: 2023/11/27 12:15
 */
public class ConfigurationException extends Exception {
    public ConfigurationException(String msg) {
        super(msg);
    }

    public ConfigurationException(String msg, Exception ex) {
        super(msg, ex);
    }
}
