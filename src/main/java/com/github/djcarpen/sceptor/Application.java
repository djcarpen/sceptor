package com.github.djcarpen.sceptor;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Utils.HiveScriptGenerator;
import com.github.djcarpen.sceptor.Utils.JsonDeserializer;

import java.io.IOException;

import static com.github.djcarpen.sceptor.Zone.RDW;

public class Application {

    public static void main(final String[] args) throws IOException {

        String jsonPath = "/Users/dc185246/Desktop/demo/json";
        String ddlBastPath = "/Users/dc185246/Desktop/demo/ddl";

        JsonDeserializer jsonDeserializer = new JsonDeserializer();

        DataDictionary dataDictionary;
        dataDictionary = jsonDeserializer.generateDataDictionary(jsonPath);

        HiveScriptGenerator hiveScriptGenerator = new HiveScriptGenerator();
        hiveScriptGenerator.generateSchemas(dataDictionary);

//        hiveScriptGenerator.generateDDLFiles(STAGING, ddlBastPath + "/staging");
//        hiveScriptGenerator.generateDDLFiles(TRANSIENT, ddlBastPath + "/transient");
//        hiveScriptGenerator.generateDDLFiles(RDW, ddlBastPath + "/rdw");
//        hiveScriptGenerator.generateDDLFiles(BDW, ddlBastPath + "/bdw");
//
//        hiveScriptGenerator.generateDMLFiles(STAGING,"");
//        hiveScriptGenerator.generateDMLFiles(TRANSIENT,"");
        hiveScriptGenerator.generateDMLFiles(RDW, "");
    }
}
