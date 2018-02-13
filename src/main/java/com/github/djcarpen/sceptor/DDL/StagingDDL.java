package com.github.djcarpen.sceptor.DDL;

import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.RawSchema;
import com.github.djcarpen.sceptor.Schema.Schema;

import java.util.*;

public class StagingDDL implements DDL {

    private Map<String, String> ddlMap;
    private Map<String, String> ddlMapStaging;


    public Map getDDLs(List<Schema> schemas) {

        ddlMap = new LinkedHashMap<>();
        ddlMap.putAll(getStagingDDLS(schemas.get(0)));
        return ddlMap;
    }

    public Map getStagingDDLS(Schema schema) {
        ddlMapStaging = new HashMap<>();
        for (HiveTable t : ((RawSchema) schema).getTables()) {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE stg_").append(t.getDatabaseName()).append(".").append(t.getTableName()).append("(\n");
            StringJoiner columnJoiner = new StringJoiner(",\n");
            for (HiveTable.HiveColumn c : t.getColumns()) {
                columnJoiner.add("\t" + c.getColumnName() + " " + c.getDataType());
            }
            sb.append(columnJoiner.toString());
            sb.append(")\n");
            sb.append("PARTITIONED BY (");
            StringJoiner partitionJoiner = new StringJoiner(",");
            for (HiveTable.HiveColumn c : t.getPartitionColumns()) {
                partitionJoiner.add(c.getColumnName() + " " + c.getDataType());
            }
            sb.append(partitionJoiner.toString());
            sb.append(")\n");
            sb.append("ROW FORMAT DELIMITED\n");
            sb.append("FIELDS TERMINATED BY '\\u001F'\n");
            sb.append("STORED AS TEXTFILE\n");
            sb.append("LOCATION '");
            sb.append(t.getHdfsLocation());
            sb.append("';\n\n\n");
            ddlMap.put("create_" + t.getTableName() + ".hql", sb.toString());
        }
        return ddlMapStaging;
    }


}
