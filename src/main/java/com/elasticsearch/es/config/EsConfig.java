package com.elasticsearch.es.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @program: es-demo
 * @description: es配置类
 * @author: 01
 * @create: 2018-06-28 22:32
 **/
@Configuration
public class EsConfig {

    @Bean
    public TransportClient client() throws UnknownHostException {
        // 9300是es的tcp服务端口
        InetSocketTransportAddress node = new InetSocketTransportAddress(
                InetAddress.getByName("172.16.205.45"),
                9300);

        // 设置es节点的配置信息
        Settings settings = Settings.builder()
//                .put("cluster.name", "es")     这里如果没有 不要设这个，如果设置的会报错https://stackoverflow.com/questions/25912572/java-elasticsearch-none-of-the-configured-nodes-are-available
                .build();

        // 实例化es的客户端对象
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(node);

        return client;
    }
}
