package com.ncr.eda.apollo.sceptor;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class Application {

    public static void main(final String[] args) throws IOException {


        Map<String, String> scripts = new LinkedHashMap<>();
        String jsonPath = "/Users/dc185246/Desktop/demo/json";

        SchemaMapper schemaMapper = new SchemaMapper(jsonPath);
        schemaMapper.generateStagingSchema();
        //schemaMapper.generateHubSchema();
        //schemaMapper.getHubSchema();
        schemaMapper.getStagingSchema();

        DDL ddl = new DDL();
        //DML dml = new DML();

        ddl.getDDLs(schemaMapper).forEach((k, v) -> scripts.put(k.toString(), v.toString()));

        //dml.getDMLs(schemaMapper).forEach((k, v) -> scripts.put(k.toString(), v.toString()));


        for (Map.Entry<String, String> e : scripts.entrySet()) {
//            if (//e.getKey().contains("northwind_orders.hql") ||
//                    //e.getKey().contains("northwind_H_orders.hql") ||
//                    //e.getKey().contains("northwind_S_orders.hql")
//                    e.getKey().contains("northwind_L_orders.hql")
//                    ) {
//                System.out.println(e.getValue()+"\n");
//                }
            System.out.println(e.getValue() + "\n");

        }


    }
}
