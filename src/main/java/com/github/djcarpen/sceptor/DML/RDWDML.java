package com.github.djcarpen.sceptor.DML;

import com.github.djcarpen.sceptor.Schema.DataVault.HubKey;
import com.github.djcarpen.sceptor.Schema.DataVault.HubSchema;
import com.github.djcarpen.sceptor.Schema.DataVault.LinkSchema;
import com.github.djcarpen.sceptor.Schema.DataVault.SatelliteSchema;
import com.github.djcarpen.sceptor.Schema.Schema;
import com.github.djcarpen.sceptor.Utils.RuleFormatter;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class RDWDML {
    private static final String hubKeyDelimiter = "-";
    private static final String datePattern = "MM/dd/yyyy HH:mm:ss";
    private static final DateFormat df = new SimpleDateFormat(datePattern);
    private static final Date dateobj = new Date();
    private static final String load_dt = df.format(dateobj);

    private HubSchema hubSchema = new HubSchema();

    private Map<String, String> dmlMap;
    private Map<String, String> dmlMapHubs;
    private Map<String, String> dmlMapSatellites;
    private Map<String, String> dmlMapLinks;

    private Map<HubSchema.HubTable, String> hubKeyMap;

    public Map getDMLs(List<Schema> schemas) {

        hubSchema = ((HubSchema) schemas.get(0));






        dmlMap = new LinkedHashMap<>();
        dmlMap.putAll(getHubDMLS(schemas.get(0)));
        dmlMap.putAll(getSatelliteDMLS(schemas.get(1)));
        dmlMap.putAll(getLinkDMLs(schemas.get(2)));


        return dmlMap;
    }


    public Map getHubDMLS(Schema schema) {


        dmlMapHubs = new LinkedHashMap<>();
        for (HubSchema.HubTable h : ((HubSchema) schema).getTables()) {
            StringBuilder sb = new StringBuilder();
            StringJoiner hubKeyJoiner = new StringJoiner(", ");
            StringJoiner businessKeyJoiner = new StringJoiner(",\n");
            RuleFormatter ruleFormatter = new RuleFormatter();
            for (HubSchema.HubTable.HiveColumn c : h.getBusinessKeys()) {
                hubKeyJoiner.add(ruleFormatter.getFormattedColumnDefinition("raw." + c.getSourceColumnName(), c.getDataType()));
                if (c.getDataType().equals("STRING")) {
                    businessKeyJoiner.add("\t" + ruleFormatter.getFormattedColumnDefinition("raw." + c.getSourceColumnName(), c.getDataType()) + " " + c.getColumnName());
                } else {
                    businessKeyJoiner.add("\t" + c.getSourceColumnName() + " " + c.getColumnName());
                }

            }
            sb.append("INSERT INTO TABLE rdw_").append(h.getDatabaseName()).append(".").append(h.getTableName()).append("\n");
            sb.append("SELECT DISTINCT\n");
            sb.append("\tupper(concat_ws(\"").append(hubKeyDelimiter).append("\",").append(hubKeyJoiner.toString()).append(")) ").append(h.getHubKey().getColumnName()).append(",\n");
            sb.append(businessKeyJoiner.toString() + "\n");
            sb.append("\t").append("unix_timestamp('").append(load_dt.toString()).append("', '").append(datePattern).append("') ").append(h.getLoadDate().getColumnName()).append("\n");
            sb.append("FROM raw_").append(h.getDatabaseName()).append(".").append(h.getSourceTableName()).append(" raw\n");
            sb.append("WHERE NOT EXISTS (\tSELECT 1 FROM ").append(h.getDatabaseName()).append(".").append(h.getTableName()).append(" hub\n");
            sb.append("\t\t\t\t\tWHERE ").append("\tupper(concat_ws(\"").append(hubKeyDelimiter).append("\",").append(hubKeyJoiner.toString()).append(")) = hub.").append(h.getHubKey().getColumnName()).append(")\n");
            sb.append("AND edl_ingest_time >= ${hivevar:edlIngestTime}\n");
            sb.append("AND edl_ingest_channel = ${hivevar:edlIngestChannel};\n");

            dmlMapHubs.put("load_" + h.getTableName() + ".hql", sb.toString());
        }
        return dmlMapHubs;
    }


    public Map getSatelliteDMLS(Schema schema) {
        dmlMapSatellites = new LinkedHashMap<>();
        for (SatelliteSchema.SatelliteTable s : ((SatelliteSchema) schema).getTables()) {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO TABLE rdw_").append(s.getDatabaseName()).append(".").append(s.getTableName()).append("\n");
            sb.append("SELECT \n");
            StringJoiner columnJoiner = new StringJoiner(",\n");
            for (SatelliteSchema.SatelliteTable.HiveColumn c : s.getColumns()) {
                if (c.getColumnName().equals(s.getHubKey().getColumnName())) {
                    columnJoiner.add("\tupper(concat_ws(\"" + hubKeyDelimiter + "\"," + s.getHubKeyDefinition() + s.getHubKey().getColumnName());
                } else if (c.getColumnName().equals(s.getLoadDate().getColumnName())) {
                    columnJoiner.add("\tunix_timestamp('" + load_dt.toString() + "', '" + datePattern + "') " + s.getLoadDate().getColumnName());

                } else {
                    columnJoiner.add("\t" + c.getColumnName() + " " + c.getColumnName());
                }
            }
            sb.append(columnJoiner.toString() + "\n");
            sb.append("FROM raw_").append(s.getDatabaseName()).append(".").append(s.getDatabaseName()).append(" raw\n");
            sb.append("WHERE raw.edl_ingest_time >= ${hivevar:edlIngestTime}\n");
            sb.append("AND raw.edl_ingest_channel = ${hivevar:edlIngestChannel};\n");

            dmlMapSatellites.put("load_" + s.getTableName() + ".hql", sb.toString());
        }
        return dmlMapSatellites;
    }

    public Map getLinkDMLs(Schema schema) {

        hubKeyMap = new LinkedHashMap<>();
        for (HubSchema.HubTable h : hubSchema.getTables()) {
            HubKey hubKey = new HubKey();
            hubKeyMap.put(h, hubKey.getHubKey(h, "justin"));
        }

        dmlMapLinks = new LinkedHashMap<>();
        for (LinkSchema.LinkTable l : ((LinkSchema) schema).getTables()) {

            if (l.getColumns().size() > 2) {


                StringBuilder sb = new StringBuilder();
                StringJoiner columnJoiner = new StringJoiner(", \n");
                RuleFormatter ruleFormatter = new RuleFormatter();
                StringJoiner linkKeyJoiner = new StringJoiner(", ");
                StringJoiner tableJoiner = new StringJoiner("\n");
//                for (Map.Entry<HubSchema.HubTable,String> entry : hubKeyMap.entrySet())
//                {
//                    if (entry.getKey().getTableName().equals(l.getSourceTableName())) {
//                        linkKeyJoiner.add(entry.getValue());
//                        columnJoiner.add("\t" + entry.getValue() + " hk_" + entry.getKey().getTableName());
//                    }
//                }

                for (HubSchema.HubTable h : hubSchema.getTables()) {
                    if (h.getSourceTableName().equals(l.getSourceTableName())) {
                        HubKey hubKey = new HubKey();
                        String hubKeyDefinition = hubKey.getHubKey(h, h.getSourceTable().getTableName());
                        linkKeyJoiner.add(hubKeyDefinition);
                        columnJoiner.add("\t" + hubKeyDefinition + " hk_" + h.getSourceTable().getTableName());
                    }
                }
                for (LinkSchema.LinkTable.HiveColumn c : l.getColumns()) {

                    if (!c.equals(l.getLinkKey()) && !c.equals(l.getLoadDate())) {
                        for (Map.Entry<HubSchema.HubTable, String> entry : hubKeyMap.entrySet()) {
                            if (entry.getKey().getTableName().equals(c.getForeignKeyTable())) {


                                //System.out.println("entry.getKey().getTableName():  " + entry.getKey().getTableName());
                                //System.out.println("entry.getValue():  " + entry.getValue());


                                linkKeyJoiner.add(entry.getValue());
                                columnJoiner.add("\t" + entry.getValue() + " " + c.getColumnName());
                                tableJoiner.add("LEFT JOIN " + entry.getKey().getTableName() + " " + entry.getKey().getTableName() + "_" + c.getColumnName() + " on " + l.getSourceTableName() + ".");


                            }

                        }
                    }
                }

                sb.append("INSERT INTO TABLE rdw_").append(l.getDatabaseName()).append(".").append(l.getTableName()).append("\n");
                sb.append("SELECT DISTINCT\n");
                sb.append("\tupper(concat_ws(\"").append(hubKeyDelimiter).append("\",").append(linkKeyJoiner).append(" ").append(l.getLinkKey().getColumnName()).append("\n");
                sb.append(columnJoiner.toString() + "\n");
                sb.append("FROM ").append(l.getSourceTableName()).append("\n");
                sb.append(tableJoiner.toString()).append("\n");
                sb.append("AND edl_ingest_time >= ${hivevar:edlIngestTime}\n");
                sb.append("AND edl_ingest_channel = ${hivevar:edlIngestChannel};\n");

                dmlMapLinks.put("load_" + l.getTableName() + ".hql", sb.toString());
            }
        }
        return dmlMapLinks;
    }

//    private Map getHubKeyMap(Schema schema) {
//        Map<HubSchema.HubTable, String> myhubKeyMap = new LinkedHashMap<>();
//
//        for (HubSchema.HubTable h : ((HubSchema) schema).getTables()) {
//            myhubKeyMap.put(h, h.getHubKeyDefinition());
//
//        }
//        return myhubKeyMap;
//    }
}
