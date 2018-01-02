package com.github.djcarpen.sceptor;

import com.github.djcarpen.sceptor.DDL.DDL;
import com.github.djcarpen.sceptor.DDL.RawDDL;
import com.github.djcarpen.sceptor.DDL.RawDataWarehouseDDL;
import com.github.djcarpen.sceptor.Schema.DataVault.RawDataWarehouseSchema;
import com.github.djcarpen.sceptor.Schema.RawSchema;
import com.github.djcarpen.sceptor.Schema.Schema;
import com.github.djcarpen.sceptor.Schema.SourceSchema;
import com.github.djcarpen.sceptor.Utils.JsonDeserializer;

import java.io.IOException;

public class Application {

    public static void main(final String[] args) throws IOException {

        String jsonPath = "/Users/dc185246/Desktop/Demo/json";
        String ddlPathRDW = "/Users/dc185246/Desktop/Demo/rdw";

        JsonDeserializer jsonDeserializer = new JsonDeserializer();

        SourceSchema sourceSchema;
        sourceSchema = jsonDeserializer.generateSourceSchema(jsonPath);

        Schema rawDataWarehouseSchema = new RawDataWarehouseSchema();
        rawDataWarehouseSchema.generateTables(sourceSchema);

        Schema rawSchema = new RawSchema();
        rawSchema.generateTables(sourceSchema);

        DDL rawDataWarehouseDDL = new RawDataWarehouseDDL();
        rawDataWarehouseDDL.writeDDL(Zone.RDW,rawDataWarehouseSchema,ddlPathRDW);
        rawDataWarehouseDDL.writeDDL(Zone.BDW,rawDataWarehouseSchema,ddlPathRDW);

        DDL rawDDL = new RawDDL();
        rawDDL.writeDDL(Zone.STAGING, rawSchema, ddlPathRDW);
        rawDDL.writeDDL(Zone.TRANSIENT, rawSchema, ddlPathRDW);
    }
}
