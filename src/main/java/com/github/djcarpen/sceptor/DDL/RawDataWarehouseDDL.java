package com.github.djcarpen.sceptor.DDL;

import com.github.djcarpen.sceptor.Schema.Schema;
import com.github.djcarpen.sceptor.Zone;

public class RawDataWarehouseDDL {

    public void writeDDL(Zone zone, Schema schema, String ddlPath) {
//        for (HiveTable t : schema.getTables()) {
//            StringBuilder sb = new StringBuilder();
//            sb.append("CREATE TABLE ");
//            if (zone.equals(Zone.RDW)) {
//                sb.append("rdw_");
//            } else if (zone.equals(Zone.BDW)) {
//                sb.append("bdw_");
//            }
//            sb.append(t.getDatabaseName());
//            sb.append(".");
//            sb.append(t.getTableName()).append(" (\n");
//            StringJoiner joiner = new StringJoiner(",\n");
//            for (HiveTable.HiveColumn c : t.getColumns()) {
//                joiner.add("\t" + c.getColumnName() + " " + c.getDataType());
//            }
//            sb.append(joiner.toString());
//            if (zone.equals(Zone.RDW)) {
//                sb.append(") \nSTORED AS ORC;");
//            } else if (zone.equals(Zone.BDW)) {
//                sb.append(") \nCLUSTERED BY (");
//                sb.append(t.getColumns().get(0).getColumnName());
//                sb.append(") INTO 1 BUCKETS\n");
//                sb.append("STORED AS ORC\n");
//                sb.append("TBLPROPERTIES (\"TRANSACTIONAL\"=\"TRUE\");");
//            }
//            sb.append("\n\n");
//            System.out.println(sb.toString());
//        }
    }
}
