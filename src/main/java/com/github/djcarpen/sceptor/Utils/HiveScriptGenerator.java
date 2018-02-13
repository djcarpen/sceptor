package com.github.djcarpen.sceptor.Utils;

import com.github.djcarpen.sceptor.Schema.Schema;

public class HiveScriptGenerator {

    //private Schema rawSchema;
    //private Schema rawDataWarehouseSchema;
    private Schema HubSchema;
    //private Schema DataVaultSchema;

//    public void generateDMLFiles(Zone zone, String ddlPath) {
//        DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
//        Date dateobj = new Date();
//        String load_dt = df.format(dateobj);
//        Schema schema = null;
//
//        if (zone.equals(STAGING) || zone.equals(TRANSIENT)) {
//            schema = rawSchema;
//            for (HiveTable t : schema.getTables()) {
//                StringBuilder sb = new StringBuilder();
//                if (zone.equals(STAGING)) {
//                    sb.append("ALTER TABLE stg_");
//                    sb.append(t.getDatabaseName());
//                    sb.append(".");
//                    sb.append(t.getTableName());
//                    sb.append(" ADD PARTITION (");
//                    StringJoiner partitionJoiner = new StringJoiner(",");
//                    for (HiveTable.HiveColumn c : t.getPartitionColumns()) {
//                        partitionJoiner.add(c.getColumnName() + " " + c.getDataType());
//                    }
//                    sb.append(partitionJoiner.toString());
//                    sb.append(") LOCATION '");
//                    sb.append(t.getHdfsLocation());
//                    sb.append("edl_ingest_channel=${hivevar:edlIngestChannel}/edl_ingest_time=${hivevar:edlIngestTime}';");
//                    System.out.println(sb.toString());
//                }
//                if (zone.equals(TRANSIENT)) {
//                    sb.append("INSERT INTO TABLE ");
//                    sb.append(t.getDatabaseName());
//                    sb.append(".");
//                    sb.append(t.getTableName());
//                    sb.append("\npartition (");
//                    StringJoiner partitionJoiner = new StringJoiner(",");
//                    for (HiveTable.HiveColumn c : t.getPartitionColumns()) {
//                        partitionJoiner.add(c.getColumnName());
//                    }
//                    sb.append(partitionJoiner.toString());
//                    sb.append(")\nSELECT ");
//                    StringJoiner columnJoiner = new StringJoiner(",\n");
//                    for (HiveTable.HiveColumn c : t.getColumns()) {
//                        columnJoiner.add("\t" + c.getColumnName());
//                    }
//                    sb.append(columnJoiner.toString());
//                    sb.append(",\n");
//                    StringJoiner partitionColumnsJoiner = new StringJoiner(",\n");
//                    for (HiveTable.HiveColumn c : t.getPartitionColumns()) {
//                        partitionColumnsJoiner.add("\t" + c.getColumnName());
//                    }
//                    sb.append(partitionColumnsJoiner.toString());
//                    sb.append(")\n");
//                    sb.append("FROM stg_");
//                    sb.append(t.getDatabaseName());
//                    sb.append(".");
//                    sb.append(t.getTableName());
//                    sb.append("\n");
//                    sb.append("WHERE edl_ingest_time = ${hivevar:edlIngestTime} and edl_ingest_channel = ${hivevar:edlIngestChannel};\n\n");
//                    System.out.println(sb.toString());
//
//                }
//            }
//        } else if (zone.equals(RDW)) {
//            schema = HubSchema;
//            getDMLs(RDW,schema);
//
//        }
//
//    }
//
//
//
//
//
//
//
//
//
//
//
//    public void generateDDLFiles(Zone zone, String ddlPath) {
//        Schema schema;
//        if (zone.equals(STAGING) || zone.equals(TRANSIENT)) schema = rawSchema;
//        else if (zone.equals(RDW) || zone.equals(BDW)) schema = rawDataWarehouseSchema;
//        else schema = null;
//        assert schema != null;
//        for (HiveTable t : schema.getTables()) {
//            StringBuilder sb = new StringBuilder();
//            if (zone.equals(STAGING)) {
//                sb.append("CREATE TABLE stg_").append(t.getDatabaseName()).append(".").append(t.getTableName()).append("(\n");
//            }
//            if (zone.equals(TRANSIENT)) {
//                sb.append("CREATE TABLE raw_").append(t.getDatabaseName()).append(".").append(t.getTableName()).append("(\n");
//
//            }
//            if (zone.equals(RDW)) {
//                sb.append("CREATE TABLE rdw_").append(t.getDatabaseName()).append(".").append(t.getTableName()).append("(\n");
//            } else if (zone.equals(BDW)) {
//                sb.append("CREATE TABLE bdw_").append(t.getDatabaseName()).append(".").append(t.getTableName()).append("(\n");
//            }
//            StringJoiner columnJoiner = new StringJoiner(",\n");
//            for (HiveTable.HiveColumn c : t.getColumns()) {
//                columnJoiner.add("\t" + c.getColumnName() + " " + c.getDataType());
//            }
//            sb.append(columnJoiner.toString());
//            sb.append(")\n");
//            if (zone.equals(STAGING) || zone.equals(TRANSIENT)) {
//                sb.append("PARTITIONED BY (");
//                StringJoiner partitionJoiner = new StringJoiner(",");
//                for (HiveTable.HiveColumn c : t.getPartitionColumns()) {
//                    partitionJoiner.add(c.getColumnName() + " " + c.getDataType());
//                }
//                sb.append(partitionJoiner.toString());
//                sb.append(")\n");
//                if (zone.equals(STAGING)) {
//                    sb.append("ROW FORMAT DELIMITED\n");
//                    sb.append("FIELDS TERMINATED BY '\\u001F'\n");
//                    sb.append("STORED AS TEXTFILE\n");
//                    sb.append("LOCATION '");
//                    sb.append(t.getHdfsLocation());
//                    sb.append("';\n");
//                } else {
//                    sb.append("STORED AS ORC;\n");
//                }
//            } else if (zone.equals(BDW)) {
//                sb.append(") \nCLUSTERED BY (");
//                sb.append(t.getColumns().get(0).getColumnName());
//                sb.append(") INTO 1 BUCKETS\n");
//                sb.append("STORED AS ORC\n");
//                sb.append("TBLPROPERTIES (\"TRANSACTIONAL\"=\"TRUE\");");
//            } else {
//                sb.append("STORED AS ORC;\n");
//            }
//            sb.append("\n\n");
//            FileWriter fileWriter = new FileWriter();
//            fileWriter.writeFile(sb.toString(), ddlPath, "create_" + t.getTableName() + ".hql");
//            System.out.println(sb.toString());
//        }
//    }
//
//
//    public void generateSchemas(DataDictionary dataDictionary) {
//        rawSchema = new RawSchema();
//        rawSchema.generateTables(dataDictionary);
//
//        HubSchema = new HubSchema();
//        HubSchema.generateTables(dataDictionary);
//    }

}