package com.github.djcarpen.sceptor.DML;

import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.RawSchema;
import com.github.djcarpen.sceptor.Schema.Schema;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class StagingDML {

    private static final String hubKeyDelimiter = "-";
    private static final String datePattern = "MM/dd/yyyy HH:mm:ss";
    private static final DateFormat df = new SimpleDateFormat(datePattern);
    private static final Date dateobj = new Date();
    private static final String load_dt = df.format(dateobj);

    private Map<String, String> dmlMap;
    private Map<String, String> dmlMapStaging;

    public Map getDMLs(List<Schema> schemas) {

        dmlMap = new LinkedHashMap<>();
        dmlMap.putAll(getStagingDML(schemas.get(0)));
        return dmlMap;
    }

    public Map getStagingDML(Schema schema) {
        dmlMapStaging = new LinkedHashMap<>();
        for (HiveTable t : ((RawSchema) schema).getTables()) {
            StringBuilder sb = new StringBuilder();
            sb.append("ALTER TABLE stg_");
            sb.append(t.getDatabaseName());
            sb.append(".");
            sb.append(t.getTableName());
            sb.append(" ADD PARTITION (");
            StringJoiner partitionJoiner = new StringJoiner(",");
            for (HiveTable.HiveColumn c : t.getPartitionColumns()) {
                partitionJoiner.add(c.getColumnName() + " " + c.getDataType());
            }
            sb.append(partitionJoiner.toString());
            sb.append(") LOCATION '");
            sb.append(t.getHdfsLocation());
            sb.append("edl_ingest_channel=${hivevar:edlIngestChannel}/edl_ingest_time=${hivevar:edlIngestTime}';");
            dmlMapStaging.put("load_" + t.getTableName() + ".hql", sb.toString());
        }
        return dmlMapStaging;
    }
}
