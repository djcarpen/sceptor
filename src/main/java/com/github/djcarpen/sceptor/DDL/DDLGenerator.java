package com.github.djcarpen.sceptor.DDL;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.DataVault.RawDataWarehouseSchema;
import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.RawSchema;
import com.github.djcarpen.sceptor.Schema.Schema;
import com.github.djcarpen.sceptor.Zone;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class DDLGenerator {

    List<Schema> schemaList;

    //private Map<String, String> ddlMap = new HashMap<>();

    public void generateSchemas(DataDictionary dataDictionary) {
        schemaList = new ArrayList<>();

        Schema rawSchema = new RawSchema();
        rawSchema.generateTables(dataDictionary);
        schemaList.add(rawSchema);

        Schema rawDataWarehouseSchema = new RawDataWarehouseSchema();
        rawDataWarehouseSchema.generateTables(dataDictionary);
        schemaList.add(rawDataWarehouseSchema);


    }

    public void getDDL(Zone zone) {
        for (Schema schema : schemaList) {
            for (HiveTable t : schema.getTables()) {
                StringBuilder sb = new StringBuilder();
                if (zone.equals(Zone.STAGING)) {
                    sb.append("CREATE TABLE stg_" + t.getDatabaseName() + "." + t.getTableName() + "(\n");
                }
                if (zone.equals(Zone.TRANSIENT)) {
                    sb.append("CREATE TABLE raw_" + t.getDatabaseName() + "." + t.getTableName() + "(\n");

                }
                if (zone.equals(Zone.RDW)) {
                    sb.append("CREATE TABLE rdw_" + t.getDatabaseName() + "." + t.getTableName() + "(\n");
                } else if (zone.equals(Zone.BDW)) {
                    sb.append("CREATE TABLE bdw_" + t.getDatabaseName() + "." + t.getTableName() + "(\n");
                }
                StringJoiner columnJoiner = new StringJoiner(",\n");
                for (HiveTable.HiveColumn c : t.getColumns()) {
                    columnJoiner.add("\t" + c.getColumnName() + " " + c.getDataType());
                }
                sb.append(columnJoiner.toString());
                sb.append(")\n");
                if (zone.equals(Zone.STAGING) || zone.equals(Zone.TRANSIENT)) {
                    sb.append("PARTIONED BY (");
                    StringJoiner partitionJoiner = new StringJoiner(",");
                    for (HiveTable.HiveColumn c : t.getPartitionColumns()) {
                        partitionJoiner.add(c.getColumnName() + " " + c.getDataType());
                    }
                    sb.append(partitionJoiner.toString());
                    sb.append(")\n");
                    if (zone.equals(Zone.STAGING)) {
                        sb.append("ROW FORMAT DELIMITED\n");
                        sb.append("FIELDS TERMINATED BY '\\u001F'\n");
                        sb.append("STORED AS TEXTFILE\n");
                        sb.append("LOCATION '");
                        sb.append(t.getHdfsLocation());
                        sb.append("';\n");
                    } else {
                        sb.append("STORED AS ORC;\n");
                    }
                } else if (zone.equals(Zone.BDW)) {
                    sb.append(") \nCLUSTERED BY (");
                    sb.append(t.getColumns().get(0).getColumnName());
                    sb.append(") INTO 1 BUCKETS\n");
                    sb.append("STORED AS ORC\n");
                    sb.append("TBLPROPERTIES (\"TRANSACTIONAL\"=\"TRUE\");");
                } else {
                    sb.append("STORED AS ORC;\n");
                }
                sb.append("\n\n");
                System.out.println(sb.toString());
            }


        }

    }


}