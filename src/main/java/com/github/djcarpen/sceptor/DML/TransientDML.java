package com.github.djcarpen.sceptor.DML;

import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.RawSchema;
import com.github.djcarpen.sceptor.Schema.Schema;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class TransientDML {

    private static final String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();

    private Map<String, String> dmlMap;
    private Map<String, String> dmlMapTransient;
    private Properties Props = new Properties();

    public TransientDML() {
        try {
            Props.load(new FileInputStream("Runtime.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map getDMLs(List<Schema> schemas) {

        dmlMap = new LinkedHashMap<>();
        dmlMap.putAll(getTransientDML(schemas.get(0)));
        return dmlMap;
    }

    public Map getTransientDML(Schema schema) {
        dmlMapTransient = new LinkedHashMap<>();
        for (HiveTable t : ((RawSchema) schema).getTables()) {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO TABLE ");
            sb.append(Props.getProperty("transientDatabaseName"));
            sb.append(".");
            sb.append(t.getTableName());
            sb.append("\npartition (");
            StringJoiner partitionJoiner = new StringJoiner(",");
            for (HiveTable.HiveColumn c : t.getPartitionColumns()) {
                partitionJoiner.add(c.getColumnName());
            }
            sb.append(partitionJoiner.toString());
            sb.append(")\nSELECT ");
            StringJoiner columnJoiner = new StringJoiner(",\n");
            for (HiveTable.HiveColumn c : t.getColumns()) {
                columnJoiner.add("\t" + c.getColumnName());
            }
            sb.append(columnJoiner.toString());
            sb.append(",\n");
            StringJoiner partitionColumnsJoiner = new StringJoiner(",\n");
            for (HiveTable.HiveColumn c : t.getPartitionColumns()) {
                partitionColumnsJoiner.add("\t" + c.getColumnName());
            }
            sb.append(partitionColumnsJoiner.toString());
            sb.append(")\n");
            sb.append("FROM ");
            sb.append(Props.getProperty("stagingDatabaseName"));
            sb.append(".");
            sb.append(t.getTableName());
            sb.append("\n");
            sb.append("WHERE edl_ingest_time = ${hivevar:edlIngestTime} and edl_ingest_channel = ${hivevar:edlIngestChannel};\n\n");

        }
        return dmlMapTransient;
    }
}
