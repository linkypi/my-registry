package org.hiraeth.govern.server;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.constant.NodeType;
import org.hiraeth.govern.common.util.StringUtil;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.config.ConfigurationException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * 服务治理平台 Server 端
 * @author: leo
 * @description:
 * @ClassName: org.hiraeth.govern.server
 * @date: 2023/11/27 11:54
 */
@Slf4j
@SpringBootApplication
public class GovernServer {

    public static void main(String[] args) {
        try {

            ConfigurableApplicationContext context = SpringApplication.run(GovernServer.class);
            Configuration configuration = context.getBean(Configuration.class);

            String configPath = args[0];
            if (StringUtil.isEmpty(configPath)) {
                throw new ConfigurationException("config file path cannot be empty.");
            }

            configuration.parse(configPath);

        } catch (ConfigurationException ex) {
            log.error("config file not found", ex);
            System.exit(2);
        }catch (Exception ex){
            log.error("start govern server occur error", ex);
            System.exit(1);
        }
    }

}
