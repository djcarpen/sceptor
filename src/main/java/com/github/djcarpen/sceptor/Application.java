package com.github.djcarpen.sceptor;

import com.github.djcarpen.sceptor.DDL.BDWDDL;
import com.github.djcarpen.sceptor.DDL.RDWDDL;
import com.github.djcarpen.sceptor.DDL.StagingDDL;
import com.github.djcarpen.sceptor.DDL.TransientDDL;
import com.github.djcarpen.sceptor.DML.RDWDML;
import com.github.djcarpen.sceptor.DML.StagingDML;
import com.github.djcarpen.sceptor.DML.TransientDML;
import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.DataVault.HubSchema;
import com.github.djcarpen.sceptor.Schema.DataVault.LinkSchema;
import com.github.djcarpen.sceptor.Schema.DataVault.SatelliteSchema;
import com.github.djcarpen.sceptor.Schema.RawSchema;
import com.github.djcarpen.sceptor.Schema.Schema;
import com.github.djcarpen.sceptor.Utils.JsonDeserializer;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

public class Application {

    public static void main(final String[] args) throws IOException {

        OutputStream os = null;
        Properties prop = new Properties();
        prop.setProperty("stagingDatabaseName", "stg_community_appcode_modulecode_northwind");
        prop.setProperty("stagingHdfsPath", "hdfs:///data/demo_community/public/stg/demo_app_code/demo_module_code/northwind");
        prop.setProperty("stagingFieldsTerminatedBy", "\\u001F");
        prop.setProperty("stagingStoredAs", "TEXTFILE");
        prop.setProperty("transientDatabaseName", "raw_community_appcode_modulecode_northwind");
        prop.setProperty("transientHdfsPath", "hdfs:///lake/trans/demo_community/public/northwind");
        prop.setProperty("transientStoredAs", "ORC");
        prop.setProperty("rdwDatabaseName", "rdw_northwind");
        prop.setProperty("rdwHdfsPath", "hdfs:///lake/rdw/demo_community/public/northwind");
        prop.setProperty("bdwDatabaseName", "bdw_northwind");
        prop.setProperty("rdwStoredAs", "ORC");
        prop.setProperty("bdwHdfsPath", "hdfs:///lake/bdw/demo_community/public/northwind");
        prop.setProperty("bdwStoredAs", "ORC");
        try {
            os = new FileOutputStream("Runtime.properties");
            prop.store(os, "Runtime Property File");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        String jsonPath = "/Users/dc185246/Desktop/demo/json";


        JsonDeserializer jsonDeserializer = new JsonDeserializer();

        DataDictionary dataDictionary;
        dataDictionary = jsonDeserializer.generateDataDictionary(jsonPath);

        Map<String, String> scripts = new LinkedHashMap<>();

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
            scripts.put("Staging " + entry.getKey(), entry.getValue());
        }

        TransientDDL transientDDL = new TransientDDL();
        Map<String, String> transientDDLMap = transientDDL.getDDLs(rawSchemas);
        for (Map.Entry<String, String> entry : transientDDLMap.entrySet()) {
            //System.out.println("***** Transient DDL: "+entry.getKey() + " *****\n\n" + entry.getValue());
            scripts.put("Transient " + entry.getKey(), entry.getValue());
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
            scripts.put("RDW " + entry.getKey(), entry.getValue());
        }

        BDWDDL bdwDDL = new BDWDDL();
        Map<String, String> bdwDDLMap = bdwDDL.getDDLs(rdwSchemas);
        for (Map.Entry<String, String> entry : bdwDDLMap.entrySet()) {
            //System.out.println("***** BDW DDL: "+entry.getKey() + " *****\n\n" + entry.getValue());
            scripts.put("BDW " + entry.getKey(), entry.getValue());
        }


///////////////////// DMLs ///////////////////
        StagingDML stagingDML = new StagingDML();
        Map<String, String> stagingDMLMap = stagingDML.getDMLs(rawSchemas);
        for (Map.Entry<String, String> entry : stagingDMLMap.entrySet()) {
            //System.out.println("***** LoadStaging: "+entry.getKey() + " *****\n\n" + entry.getValue());
            scripts.put("HDFS2Staging " + entry.getKey(), entry.getValue());
        }

////staging to transient dml
        TransientDML transientDML = new TransientDML();
        Map<String, String> transientDMLMap = transientDML.getDMLs(rawSchemas);
        for (Map.Entry<String, String> entry : transientDMLMap.entrySet()) {
            //System.out.println("***** Staging2Transient: "+entry.getKey() + " *****\n\n" + entry.getValue());
            scripts.put("Staging2Transient " + entry.getKey(), entry.getValue());
        }
////transient to rdw hub dml
        RDWDML rdwDML = new RDWDML();
        Map<String, String> rdwDMLMap = rdwDML.getDMLs(rdwSchemas);
        for (Map.Entry<String, String> entry : rdwDMLMap.entrySet()) {
            //System.out.println("***** RDW DML: " + entry.getKey() + " *****\n\n" + entry.getValue());{
            scripts.put("Transient2RDW " + entry.getKey(), entry.getValue());
        }

//transient to rdw satellite dml
//transient to rdw link dml
//rdw to bdw hub dml
//rdw to bdw satellite dml
//rdw to bdw link dml


        System.out.println("\n\n");
        System.out.println("Staging DDLs generated: " + stagingDDLMap.size());
        System.out.println("Transient DDLs generated: " + transientDDLMap.size());
        System.out.println("RDW DDLs generated: " + rdwDDLMap.size());
        System.out.println("BDW DDLs generated: " + bdwDDLMap.size());
        //System.out.println("Staging2Transient DMLs generated: " + stagingDDLMap.size());
        System.out.println("Transient2RDW DMLs generated: " + rdwDMLMap.size());
        //System.out.println("RDW2BDW DMLs generated" + rdwDDLMap.size());


        for (Map.Entry<String, String> e : scripts.entrySet()) {
            if (e.getKey().contains("H_orders.hql") || e.getKey().contains("S_orders.hql") || e.getKey().contains("L_orders.hql") || e.getKey().contains("create_orders.hql") || e.getKey().contains("load_orders.hql")) {
                System.out.println(e.getKey() + "\n\n" + e.getValue() + "\n\n");
            }

        }





    }
}
