package com.github.djcarpen.sceptor;

import com.github.djcarpen.sceptor.Schema.RDW.HubSchema;
import com.github.djcarpen.sceptor.Schema.RDW.LinkSchema;
import com.github.djcarpen.sceptor.Schema.RDW.SatelliteSchema;
import com.github.djcarpen.sceptor.Schema.StagingSchema;
import com.github.djcarpen.sceptor.Schema.TransientSchema;
import com.github.djcarpen.sceptor.Utils.JsonDeserializer;

import java.io.IOException;

public class SchemaMapper {

    private String jsonPath;
    private DataDictionary dataDictionary;
    private StagingSchema stagingSchema;
    private TransientSchema transientSchema;
    private HubSchema hubSchema;
    private SatelliteSchema satelliteSchema;
    private LinkSchema linkSchema;

    public SchemaMapper(String jsonPath) throws IOException {
        this.jsonPath = jsonPath;
        JsonDeserializer jsonDeserializer = new JsonDeserializer();
        dataDictionary = jsonDeserializer.generateDataDictionary(jsonPath);
        generateSchemas(dataDictionary);
    }

    public StagingSchema getStagingSchema() {
        return stagingSchema;
    }

    public TransientSchema getTransientSchema() {
        return transientSchema;
    }

    public HubSchema getHubSchema() {
        return hubSchema;
    }

    public SatelliteSchema getSatelliteSchema() {
        return satelliteSchema;
    }

    public LinkSchema getLinkSchema() {
        return linkSchema;
    }

    private void generateSchemas(DataDictionary dataDictionary) throws IOException {
        stagingSchema = new StagingSchema();
        transientSchema = new TransientSchema();
        hubSchema = new HubSchema();
        satelliteSchema = new SatelliteSchema();
        linkSchema = new LinkSchema();


        stagingSchema.generateTables(dataDictionary);
        transientSchema.generateTables(dataDictionary);
        hubSchema.generateTables(dataDictionary);
        satelliteSchema.generateTables(dataDictionary);
        linkSchema.generateTables(dataDictionary);
    }

}
