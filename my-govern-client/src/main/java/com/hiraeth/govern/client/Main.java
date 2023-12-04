package com.hiraeth.govern.client;

import com.beust.jcommander.JCommander;
import com.hiraeth.govern.client.config.Configuration;
import lombok.extern.slf4j.Slf4j;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/1 10:16
 */
@Slf4j
public class Main {
    public static void main(String[] args) throws Exception {
        Configuration configuration = Configuration.getInstance();
        JCommander.newBuilder()
                .addObject(configuration)
                .build()
                .parse(args);

        configuration.parse();

        ServiceInstance serviceInstance = new ServiceInstance();
        serviceInstance.init();
        Thread.sleep(30000000);
//        serviceInstance.subscribe("DEFAULT");
    }


}
