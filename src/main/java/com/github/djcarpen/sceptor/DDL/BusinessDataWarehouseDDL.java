package com.github.djcarpen.sceptor.DDL;

public class BusinessDataWarehouseDDL {

//    public Map<String, String> getDDLs(Schema schema) {
//        Map<String,String> ddlMap = new MultiKeyMap();
//        for (HiveTable t : schema.getTables()) {
//            StringBuilder sb = new StringBuilder();
//            sb.append("CREATE TABLE ");
//            sb.append("bdw_");
//            sb.append(t.getDatabaseName());
//            sb.append(".");
//            sb.append(t.getTableName()).append(" (\n");
//            StringJoiner joiner = new StringJoiner(",\n");
//            for (HiveTable.HiveColumn c : t.getColumns()) {
//                joiner.add("\t" + c.getColumnName() + " " + c.getDataType());
//            }
//            sb.append(joiner.toString());
//            sb.append(") \nCLUSTERED BY (");
//            sb.append(t.getColumns().get(0).getColumnName());
//            sb.append(") INTO 1 BUCKETS\n");
//            sb.append("STORED AS ORC\n");
//            sb.append("TBLPROPERTIES (\"TRANSACTIONAL\"=\"TRUE\");");
//            sb.append("\n\n");
//            System.out.println(sb.toString());
//            ddlMap.put(t.getTableName(),sb.toString());
//        }
//        return ddlMap;
//    }
}
