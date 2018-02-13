package com.github.djcarpen.sceptor;

import com.github.djcarpen.sceptor.DDL.BDWDDL;
import com.github.djcarpen.sceptor.DDL.RDWDDL;
import com.github.djcarpen.sceptor.DDL.StagingDDL;
import com.github.djcarpen.sceptor.DDL.TransientDDL;
import com.github.djcarpen.sceptor.DML.RDWDML;
import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.DataVault.HubSchema;
import com.github.djcarpen.sceptor.Schema.DataVault.LinkSchema;
import com.github.djcarpen.sceptor.Schema.DataVault.SatelliteSchema;
import com.github.djcarpen.sceptor.Schema.RawSchema;
import com.github.djcarpen.sceptor.Schema.Schema;
import com.github.djcarpen.sceptor.Utils.JsonDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Application {

    public static void main(final String[] args) throws IOException {

        String jsonPath = "/Users/dc185246/Desktop/demo/json";
        String ddlBastPath = "/Users/dc185246/Desktop/demo/ddl";

        JsonDeserializer jsonDeserializer = new JsonDeserializer();

        DataDictionary dataDictionary;
        dataDictionary = jsonDeserializer.generateDataDictionary(jsonPath);


///////////////////// DDLs ///////////////////
        //************* Staging and Transient ****************
        Schema rawSchema = new RawSchema();
        rawSchema.generateTables(dataDictionary);

        List<Schema> rawSchemas = new ArrayList<>();
        rawSchemas.add(rawSchema);

        StagingDDL stagingDDL = new StagingDDL();
        Map<String, String> stagingDDLMap = stagingDDL.getDDLs(rawSchemas);
        for (Map.Entry<String, String> entry : stagingDDLMap.entrySet()) {
            //System.out.println("***** Staging DDL: "+entry.getKey() + " *****\n\n" + entry.getValue());
        }

        TransientDDL transientDDL = new TransientDDL();
        Map<String, String> transientDDLMap = transientDDL.getDDLs(rawSchemas);
        for (Map.Entry<String, String> entry : transientDDLMap.entrySet()) {
            //System.out.println("***** Transient DDL: "+entry.getKey() + " *****\n\n" + entry.getValue());
        }
        //************* RDW and BDW ****************
        Schema hubSchema = new HubSchema();
        hubSchema.generateTables(dataDictionary);

        Schema satelliteSchema = new SatelliteSchema();
        satelliteSchema.generateTables(dataDictionary);

        Schema linkSchema = new LinkSchema();
        linkSchema.generateTables(dataDictionary);
        List<Schema> rdwSchemas = new ArrayList<>();
        rdwSchemas.add(hubSchema);
        rdwSchemas.add(satelliteSchema);
        rdwSchemas.add(linkSchema);

        RDWDDL rdwDDL = new RDWDDL();
        Map<String, String> rdwDDLMap = rdwDDL.getDDLs(rdwSchemas);
        for (Map.Entry<String, String> entry : rdwDDLMap.entrySet()) {
            //System.out.println("***** RDW DDL: "+entry.getKey() + " *****\n\n" + entry.getValue());
        }

        BDWDDL bdwDDL = new BDWDDL();
        Map<String, String> bdwDDLMap = bdwDDL.getDDLs(rdwSchemas);
        for (Map.Entry<String, String> entry : bdwDDLMap.entrySet()) {
            //System.out.println("***** BDW DDL: "+entry.getKey() + " *****\n\n" + entry.getValue());
        }
        System.out.println("\n\n");
        System.out.println("Staging DDLs generated: " + stagingDDLMap.size());
        System.out.println("Transient DDLs generated: " + transientDDLMap.size());
        System.out.println("RDW DDLs generated: " + rdwDDLMap.size());
        System.out.println("BDW DDLs generated: " + bdwDDLMap.size());

///////////////////// DMLs ///////////////////
////staging to transient dml
//
////transient to rdw hub dml
        RDWDML rdwDML = new RDWDML();
        Map<String, String> rdwDMLMap = rdwDML.getDMLs(rdwSchemas);
        for (Map.Entry<String, String> entry : rdwDMLMap.entrySet()) {

            //System.out.println("***** RDW DML: " + entry.getKey() + " *****\n\n" + entry.getValue());
        }

//transient to rdw satellite dml
//transient to rdw link dml
//rdw to bdw hub dml
//rdw to bdw satellite dml
//rdw to bdw link dml












    }
}
