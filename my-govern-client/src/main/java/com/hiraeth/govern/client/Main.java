package com.hiraeth.govern.client;

import com.beust.jcommander.JCommander;
import com.hiraeth.govern.client.config.Configuration;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/1 10:16
 */
public class Main {
    public static void main(String[] args) throws Exception {
        Configuration configuration = Configuration.getInstance();
        JCommander.newBuilder()
                .addObject(configuration)
                .build()
                .parse(args);

        configuration.parse();

        new ClientServer().init();
    }
}
