package com;

public class Main {
    public static void main(String args []) {

        ElasticConnector connector = new ElasticConnector();
        connector.initialize();

        try {
            AddContent t1 = new AddContent();
            AddContent t2 = new AddContent();  // get json from queue CONTENTS and add it to es
            t1.start();
            t2.start();
        } catch (Exception e) {
            System.out.println("error: " + e.toString());
        }

        //connector.getSomeDataAll();
        //connector.getSomeData();
        //connector.getSomeDataList();
        System.out.println(connector.SearchNews("news", "Text", "электросталь"));
    }
}