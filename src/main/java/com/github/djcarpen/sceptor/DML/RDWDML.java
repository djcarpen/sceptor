package com.github.djcarpen.sceptor.DML;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.DataVault.HubSchema;
import com.github.djcarpen.sceptor.Schema.DataVault.LinkSchema;
import com.github.djcarpen.sceptor.Schema.DataVault.SatelliteSchema;
import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.Schema;
import com.github.djcarpen.sceptor.Utils.RuleFormatter;
import com.google.common.base.CaseFormat;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class RDWDML {
    private static final List<String> hiveParameterColumnNames = new ArrayList<>(Arrays.asList("edl_ingest_channel", "edl_ingest_time"));
    private static final String hubKeyDelimiter = "-";
    private static final String datePattern = "MM/dd/yyyy HH:mm:ss";
    private static final DateFormat df = new SimpleDateFormat(datePattern);
    private static final Date dateobj = new Date();
    private static final String load_dt = df.format(dateobj);
    private Properties Props = new Properties();

    private HubSchema hubSchema = new HubSchema();

    private Map<String, String> dmlMap;
    private Map<String, String> dmlMapHubs;
    private Map<String, String> dmlMapSatellites;
    private Map<String, String> dmlMapLinks;

    public RDWDML() {
        try {
            Props.load(new FileInputStream("Runtime.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }





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
            String tableAlias = h.getSourceTable().getTableName();
            StringJoiner columnJoiner = new StringJoiner(",\n");
            String hubKeyDefinition = null;
            StringJoiner whereClauseJoiner = new StringJoiner("\nAND ");
            String loadDateDefinition = null;
            RuleFormatter ruleFormatter = new RuleFormatter();
            for (HubSchema.HubTable hubs : hubSchema.getTables()) {
                if (hubs.getSourceTable().equals(h.getSourceTable())) {
                    hubKeyDefinition = getHubKey(hubs, hubs.getSourceTable().getTableName());
                }

            }
//            for (HubSchema.HubTable.HiveColumn c : h.getBusinessKeys()) {
//
//                hubKeyJoiner.add(ruleFormatter.getFormattedColumnDefinition(h.getSourceTable().getTableName()+"."+ c.getSourceColumnName(), c.getDataType()));
//                if (c.getDataType().equals("STRING")) {
//                    businessKeyJoiner.add("\t" + ruleFormatter.getFormattedColumnDefinition(h.getSourceTable().getTableName()+"." + c.getSourceColumnName(), c.getDataType()) + " " + c.getColumnName());
//                } else {
//                    businessKeyJoiner.add("\t" + c.getSourceColumnName() + " " + c.getColumnName());
//                }
//
//            }

            for (HubSchema.HubTable.HiveColumn c : h.getColumns()) {
                //System.out.println("*********"+c.getColumnName());

                if (h.getBusinessKeys().contains(c)) {
                    businessKeyJoiner.add("\t" + ruleFormatter.getFormattedColumnDefinition(h.getSourceTable().getTableName() + "." + c.getSourceColumnName(), c.getDataType()));
                } else if (h.getLoadDate().equals(c)) {
                    loadDateDefinition = "\tunix_timestamp('" + load_dt.toString() + "', '" + datePattern + "') " + h.getLoadDate().getColumnName();
                }
            }

            for (String hiveParameterColumnName : hiveParameterColumnNames) {
                whereClauseJoiner.add(tableAlias + "." + hiveParameterColumnName + " = ${hivevar:" + CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, hiveParameterColumnName) + "}");
            }


            sb.append("INSERT INTO TABLE ").append(Props.getProperty("rdwDatabaseName")).append(".").append(h.getTableName()).append(" \n");
            sb.append("SELECT DISTINCT\n");
            sb.append("\tupper(concat_ws(\"").append(hubKeyDelimiter).append("\",").append(hubKeyDefinition).append(")) ").append(h.getHubKey().getColumnName()).append(",\n");
            //sb.append(hubKeyJoiner.toString() + "\n");
            sb.append(businessKeyJoiner.toString() + "\n");
            sb.append(loadDateDefinition).append("\n");
            //sb.append("\t").append("unix_timestamp('").append(load_dt.toString()).append("', '").append(datePattern).append("') ").append(h.getLoadDate().getColumnName()).append("\n");
            sb.append("FROM ").append(Props.getProperty("transientDatabaseName")).append(".").append(h.getSourceTable().getTableName()).append(" ").append(h.getSourceTable().getTableName()).append("\n");
            sb.append("WHERE NOT EXISTS (\tSELECT 1 FROM ").append(Props.getProperty("rdwDatabaseName")).append(".").append(h.getTableName()).append("\n");
            sb.append("\t\t\t\t\tWHERE ").append("upper(concat_ws(\"").append(hubKeyDelimiter).append("\",").append(hubKeyDefinition).append(")) = ").append(h.getTableName()).append(".").append(h.getHubKey().getColumnName()).append(")\n");
            sb.append("AND ").append(whereClauseJoiner).append(";\n");

            dmlMapHubs.put("load_" + h.getTableName() + ".hql", sb.toString());
        }
        return dmlMapHubs;
    }


    public Map getSatelliteDMLS(Schema schema) {
        dmlMapSatellites = new LinkedHashMap<>();
        for (SatelliteSchema.SatelliteTable s : ((SatelliteSchema) schema).getTables()) {
            StringBuilder sb = new StringBuilder();
            StringJoiner columnJoiner = new StringJoiner(",\n");
            String tableAlias = s.getSourceTable().getTableName();
            String hubKeyDefinition = null;
            StringJoiner whereClauseJoiner = new StringJoiner("\nAND ");

            for (HubSchema.HubTable h : hubSchema.getTables()) {
                if (h.getSourceTable().equals(s.getSourceTable())) {
                    hubKeyDefinition = getHubKey(h, h.getSourceTable().getTableName());
                }

            }

            for (SatelliteSchema.SatelliteTable.HiveColumn c : s.getColumns()) {

                if (c.getColumnName().equals(s.getHubKey().getColumnName())) {
                    columnJoiner.add("\tupper(concat_ws(\"" + hubKeyDelimiter + "\"," + hubKeyDefinition + " " + s.getHubKey().getColumnName());
                } else if (c.getColumnName().equals(s.getLoadDate().getColumnName())) {
                    columnJoiner.add("\tunix_timestamp('" + load_dt.toString() + "', '" + datePattern + "') " + s.getLoadDate().getColumnName());

                } else {
                    for (String hiveParameterColumnName : hiveParameterColumnNames) {
                        if (c.getColumnName().toLowerCase().equals(hiveParameterColumnName)) {
                            whereClauseJoiner.add(tableAlias + "." + c.getColumnName() + " = ${hivevar:" + CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, c.getColumnName()) + "}");
                        }
                    }
                    columnJoiner.add("\t" + tableAlias + "." + c.getColumnName() + " " + c.getColumnName());
                }
            }
            sb.append("INSERT INTO TABLE ").append(Props.getProperty("rdwDatabaseName")).append(".").append(s.getTableName()).append("\n");
            sb.append("SELECT \n");
            sb.append(columnJoiner.toString() + "\n");
            sb.append("FROM ").append(Props.getProperty("transientDatabaseName")).append(".").append(s.getSourceTable().getTableName()).append(" ").append(tableAlias).append(" \n");
            sb.append("WHERE ").append(whereClauseJoiner).append(";\n");

            dmlMapSatellites.put("load_" + s.getTableName() + ".hql", sb.toString());
        }
        return dmlMapSatellites;
    }

    public Map getLinkDMLs(Schema schema) {

        dmlMapLinks = new LinkedHashMap<>();
        for (LinkSchema.LinkTable l : ((LinkSchema) schema).getTables()) {

            if (l.getColumns().size() > 2) {


                StringBuilder sb = new StringBuilder();
                StringJoiner columnJoiner = new StringJoiner(", \n");
                RuleFormatter ruleFormatter = new RuleFormatter();
                StringJoiner linkKeyJoiner = new StringJoiner(", ");
                StringJoiner tableJoiner = new StringJoiner("\n");

                for (LinkSchema.LinkTable.HiveColumn c : l.getColumns()) {

                    if (!c.equals(l.getLinkKey()) && !c.equals(l.getLoadDate())) {
                        System.out.println(l.getTableName() + "  " + c.getColumnName() + "********");
                        for (HubSchema.HubTable h : hubSchema.getTables()) {
                            if (h.getTableName().equals(c.getForeignKeyTable()) || c.getColumnName().equals(h.getHubKey().getColumnName())) {

                                //System.out.println("ColumnName: "+ c.getColumnName() + " HubKey: "+h.getHubKey().getColumnName());
                                String hubKeyDefinition = getHubKey(h, h.getSourceTable().getTableName() + "_" + c.getColumnName());

                                linkKeyJoiner.add(hubKeyDefinition);
                                columnJoiner.add("\t" + hubKeyDefinition + " " + h.getHubKey().getColumnName());
                                tableJoiner.add("LEFT JOIN " + Props.getProperty("transientDatabaseName") + "." + h.getSourceTable().getTableName() + " " + h.getSourceTable().getTableName() + "_" + c.getColumnName());
                            }

                        }
                    }
                }
                //System.out.println("**************"+l.getTableName()+"\n"+columnJoiner+"\n\n");
                sb.append("INSERT INTO TABLE ").append(Props.getProperty("rdwDatabaseName")).append(".").append(l.getTableName()).append("\n");
                sb.append("SELECT DISTINCT\n");
                sb.append("\tupper(concat_ws(\"").append(hubKeyDelimiter).append("\",").append(linkKeyJoiner).append(" ").append(l.getLinkKey().getColumnName()).append("\n");
                sb.append(columnJoiner.toString() + "\n");
                sb.append("FROM ").append(Props.getProperty("transientDatabaseName")).append("\n");
                sb.append(tableJoiner.toString()).append("\n");
                sb.append("AND edl_ingest_time >= ${hivevar:edlIngestTime}\n");
                sb.append("AND edl_ingest_channel = ${hivevar:edlIngestChannel};\n");

                dmlMapLinks.put("load_" + l.getTableName() + ".hql", sb.toString());
            }
        }
        return dmlMapLinks;
    }

    private String getHubKey(HubSchema.HubTable table, String alias) {
        RuleFormatter ruleFormatter = new RuleFormatter();
        StringJoiner hubKeyJoiner = new StringJoiner(", ");
        List<HiveTable.HiveColumn> businessKeys = new ArrayList<>();
        for (DataDictionary.Table.Column c : table.getSourceTable().getColumns()) {
            if (c.getIsBusinessKey()) {
                HiveTable.HiveColumn businessKey = new HiveTable.HiveColumn();
                businessKey.setColumnName(c.getColumnName());
                businessKey.setDataType(c.getDataType());
                businessKeys.add(businessKey);
            }
        }
        for (HubSchema.HubTable.HiveColumn c : businessKeys) {
            hubKeyJoiner.add(ruleFormatter.getFormattedColumnDefinition(alias + "." + c.getColumnName(), c.getDataType()));
        }
        return hubKeyJoiner.toString();
    }
}
