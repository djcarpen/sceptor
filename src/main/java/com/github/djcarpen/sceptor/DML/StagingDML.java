package com.github.djcarpen.sceptor.DML;

import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.RawSchema;
import com.github.djcarpen.sceptor.Schema.Schema;
import com.google.common.base.CaseFormat;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class StagingDML {
    private Properties Props = new Properties();

    private static final String hubKeyDelimiter = "-";
    private static final String datePattern = "MM/dd/yyyy HH:mm:ss";
    private static final DateFormat df = new SimpleDateFormat(datePattern);
    private static final Date dateobj = new Date();
    private static final String load_dt = df.format(dateobj);

    public StagingDML() {
        try {
            Props.load(new FileInputStream("Runtime.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
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
            sb.append("ALTER TABLE ");
            sb.append(Props.getProperty("transientDatabaseName"));
            sb.append(".");
            sb.append(t.getTableName());
            sb.append(" ADD PARTITION (");
            StringJoiner partitionJoiner = new StringJoiner(",");
            for (HiveTable.HiveColumn c : t.getPartitionColumns()) {
                partitionJoiner.add(c.getColumnName() + "=${hivevar:" + CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, c.getColumnName()) + "}");
            }
            sb.append(partitionJoiner.toString());
            sb.append(") LOCATION '");
            sb.append(Props.getProperty("transientHdfsPath"));
            sb.append(partitionJoiner.toString().replace(",", "/")).append("';").append("\n");
            //sb.append("edl_ingest_channel=${hivevar:edlIngestChannel}/edl_ingest_time=${hivevar:edlIngestTime}';");
            dmlMapStaging.put("load_" + t.getTableName() + ".hql", sb.toString());
        }
        return dmlMapStaging;
    }
}
