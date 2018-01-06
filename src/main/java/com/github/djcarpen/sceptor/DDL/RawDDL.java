package com.github.djcarpen.sceptor.DDL;

import com.github.djcarpen.sceptor.Schema.Schema;
import com.github.djcarpen.sceptor.Zone;

public class RawDDL {


    public void writeDDL(Zone zone, Schema schema, String ddlPath) {
//        for (HiveTable t : schema.getTables()) {
//            StringBuilder sb = new StringBuilder();
//            sb.append("CREATE EXTERNAL TABLE ");
//            if (zone.equals(Zone.STAGING)) {
//                sb.append("stg_");
//            } else if (zone.equals(Zone.TRANSIENT)) {
//                sb.append("transient_");
//            }
//            sb.append(t.getDatabaseName());
//            sb.append(".");
//            sb.append(t.getTableName()).append(" (\n");
//            StringJoiner columnJoiner = new StringJoiner(",\n");
//            for (HiveTable.HiveColumn c : t.getColumns()) {
//                columnJoiner.add("\t" + c.getColumnName() + " " + c.getDataType());
//            }
//            sb.append(columnJoiner.toString());
//            sb.append(") \n");
//            sb.append(" PARTIONED BY (");
//            StringJoiner partitionJoiner = new StringJoiner(",");
//            for (HiveTable.HiveColumn c : t.getPartitionColumns()) {
//                partitionJoiner.add(c.getColumnName() + " " + c.getDataType());
//            }
//            sb.append(partitionJoiner.toString());
//            sb.append(")\n");
//            if (zone.equals(Zone.STAGING)) {
//                sb.append("ROW FORMAT DELIMITED\n");
//                sb.append("FIELDS TERMINATED BY '\\u001F'\n");
//                sb.append("STORED AS TEXTFILE\n");
//                sb.append("LOCATION '");
//                sb.append(t.getHdfsLocation());
//                sb.append("';");
//            } else if (zone.equals(Zone.TRANSIENT)) {
//                sb.append("STORED AS ORC;");
//            }
//            sb.append("\n\n");
//            System.out.println(sb.toString());
//        }
    }


}
