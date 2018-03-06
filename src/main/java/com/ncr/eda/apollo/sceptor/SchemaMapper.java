package com.ncr.eda.apollo.sceptor;

import com.ncr.eda.apollo.sceptor.Schema.RDW.HubSchema;
import com.ncr.eda.apollo.sceptor.Schema.RDW.LinkSchema;
import com.ncr.eda.apollo.sceptor.Schema.RDW.SatelliteSchema;
import com.ncr.eda.apollo.sceptor.Schema.StagingSchema;
import com.ncr.eda.apollo.sceptor.Schema.TransientSchema;
import com.ncr.eda.apollo.sceptor.Utils.JsonDeserializer;

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

    private void generateAllSchemas() {
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

    public void generateStagingSchema() {
        stagingSchema = new StagingSchema();
        stagingSchema.generateTables(dataDictionary);
    }

    public void generateTransientSchema() {
        transientSchema = new TransientSchema();
        transientSchema.generateTables(dataDictionary);
    }

    public void generateHubSchema() {
        hubSchema = new HubSchema();
        hubSchema.generateTables(dataDictionary);
    }

    public void generateSatelliteSchema() {
        satelliteSchema = new SatelliteSchema();
        satelliteSchema.generateTables(dataDictionary);
    }

    public void generateLinkSchema() {
        linkSchema = new LinkSchema();
        linkSchema.generateTables(dataDictionary);
    }

}
