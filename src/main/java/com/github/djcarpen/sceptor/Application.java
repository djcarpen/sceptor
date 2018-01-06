package com.github.djcarpen.sceptor;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Utils.JsonDeserializer;

import java.io.IOException;

public class Application {

    public static void main(final String[] args) throws IOException {

        String jsonPath = "/Users/dc185246/Desktop/demo/json";
        String ddlBastPath = "/Users/dc185246/Desktop/demo/ddl";

        JsonDeserializer jsonDeserializer = new JsonDeserializer();

        DataDictionary dataDictionary;
        dataDictionary = jsonDeserializer.generateSourceSchema(jsonPath);

        DDLGenerator ddlGenerator = new DDLGenerator();
        ddlGenerator.generateSchemas(dataDictionary);

        ddlGenerator.generateFiles(Zone.STAGING, ddlBastPath + "/staging");
        ddlGenerator.generateFiles(Zone.TRANSIENT, ddlBastPath + "/transient");
        ddlGenerator.generateFiles(Zone.RDW, ddlBastPath + "/rdw");
        ddlGenerator.generateFiles(Zone.BDW, ddlBastPath + "/bdw");
    }
}
