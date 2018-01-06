package com.github.djcarpen.sceptor;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.DataVault.RawDataWarehouseSchema;
import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.RawSchema;
import com.github.djcarpen.sceptor.Schema.Schema;
import com.github.djcarpen.sceptor.Utils.FileWriter;

import java.util.StringJoiner;

import static com.github.djcarpen.sceptor.Zone.*;

public class DDLGenerator {

    //    List<Schema> schemaList;
    Schema rawSchema;
    Schema rawDataWarehouseSchema;

    //private Map<String, String> ddlMap = new HashMap<>();

    public void generateSchemas(DataDictionary dataDictionary) {
//        schemaList = new ArrayList<>();

        rawSchema = new RawSchema();
        rawSchema.generateTables(dataDictionary);
//        schemaList.add(rawSchema);

        rawDataWarehouseSchema = new RawDataWarehouseSchema();
        rawDataWarehouseSchema.generateTables(dataDictionary);
//        schemaList.add(rawDataWarehouseSchema);


    }

    public void generateFiles(Zone zone, String ddlPath) {
        Schema schema;
        if (zone.equals(STAGING) || zone.equals(TRANSIENT)) schema = rawSchema;
        else if (zone.equals(RDW) || zone.equals(BDW)) schema = rawDataWarehouseSchema;
        else schema = null;
            for (HiveTable t : schema.getTables()) {
                StringBuilder sb = new StringBuilder();
                if (zone.equals(STAGING)) {
                    sb.append("CREATE TABLE stg_" + t.getDatabaseName() + "." + t.getTableName() + "(\n");
                }
                if (zone.equals(TRANSIENT)) {
                    sb.append("CREATE TABLE raw_" + t.getDatabaseName() + "." + t.getTableName() + "(\n");

                }
                if (zone.equals(RDW)) {
                    sb.append("CREATE TABLE rdw_" + t.getDatabaseName() + "." + t.getTableName() + "(\n");
                } else if (zone.equals(BDW)) {
                    sb.append("CREATE TABLE bdw_" + t.getDatabaseName() + "." + t.getTableName() + "(\n");
                }
                StringJoiner columnJoiner = new StringJoiner(",\n");
                for (HiveTable.HiveColumn c : t.getColumns()) {
                    columnJoiner.add("\t" + c.getColumnName() + " " + c.getDataType());
                }
                sb.append(columnJoiner.toString());
                sb.append(")\n");
                if (zone.equals(STAGING) || zone.equals(TRANSIENT)) {
                    sb.append("PARTIONED BY (");
                    StringJoiner partitionJoiner = new StringJoiner(",");
                    for (HiveTable.HiveColumn c : t.getPartitionColumns()) {
                        partitionJoiner.add(c.getColumnName() + " " + c.getDataType());
                    }
                    sb.append(partitionJoiner.toString());
                    sb.append(")\n");
                    if (zone.equals(STAGING)) {
                        sb.append("ROW FORMAT DELIMITED\n");
                        sb.append("FIELDS TERMINATED BY '\\u001F'\n");
                        sb.append("STORED AS TEXTFILE\n");
                        sb.append("LOCATION '");
                        sb.append(t.getHdfsLocation());
                        sb.append("';\n");
                    } else {
                        sb.append("STORED AS ORC;\n");
                    }
                } else if (zone.equals(BDW)) {
                    sb.append(") \nCLUSTERED BY (");
                    sb.append(t.getColumns().get(0).getColumnName());
                    sb.append(") INTO 1 BUCKETS\n");
                    sb.append("STORED AS ORC\n");
                    sb.append("TBLPROPERTIES (\"TRANSACTIONAL\"=\"TRUE\");");
                } else {
                    sb.append("STORED AS ORC;\n");
                }
                sb.append("\n\n");
                FileWriter fileWriter = new FileWriter();
                fileWriter.writeFile(sb.toString(), ddlPath, "create_" + t.getTableName() + ".hql");
                System.out.println(sb.toString());
            }


        }


}