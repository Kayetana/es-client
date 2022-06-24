package com;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

class AddContent extends Thread {
    public void run() {
        ElasticConnector elasticConnector = new ElasticConnector();
        elasticConnector.initialize();
        if (elasticConnector.client == null) {
            System.out.println("error with connection");
            return;
        }
        elasticConnector.AddIndex("news");
        elasticConnector.AddMapping("news");

        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            factory.setUsername("rabbitmq");
            factory.setPassword("rabbitmq");
            factory.setPort(5672);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare("CONTENTS", false, false, true, null);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String json = new String(delivery.getBody(), "UTF-8");  // get json from queue CONTENTS
                try {
                    elasticConnector.AddDoc(json);   // add json to es
                } finally {
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };
            boolean autoAck = false;
            channel.basicConsume("CONTENTS", autoAck, deliverCallback, consumerTag -> { });
        }
        catch (Exception e) {
            System.out.println("error: " + e);
        }
    }
}