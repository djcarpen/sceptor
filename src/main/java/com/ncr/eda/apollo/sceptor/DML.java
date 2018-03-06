package com.ncr.eda.apollo.sceptor;

import com.google.common.base.CaseFormat;
import com.ncr.eda.apollo.sceptor.Schema.RDW.HubSchema;
import com.ncr.eda.apollo.sceptor.Schema.RDW.LinkSchema;
import com.ncr.eda.apollo.sceptor.Schema.RDW.SatelliteSchema;
import com.ncr.eda.apollo.sceptor.Schema.StagingSchema;
import com.ncr.eda.apollo.sceptor.Schema.TransientSchema;

import java.util.*;

public class DML {

    StagingSchema stagingSchema;
    TransientSchema transientSchema;
    HubSchema hubSchema;
    SatelliteSchema satelliteSchema;
    LinkSchema linkSchema;

    public Map getDMLs(SchemaMapper schemaMapper) {
        Map<String, String> dmlMap = new LinkedHashMap<>();

        if (schemaMapper.getStagingSchema() != null) {
            stagingSchema = schemaMapper.getStagingSchema();
            stagingSchema.getTables().forEach(t -> dmlMap.put("create." + t.getDatabaseName() + "_" + t.getTableName() + ".hql", getStagingDML(t)));
        }
        if (schemaMapper.getTransientSchema() != null) {
            transientSchema = schemaMapper.getTransientSchema();
            transientSchema.getTables().forEach(t -> dmlMap.put("create." + t.getDatabaseName() + "_" + t.getTableName() + ".hql", getTransientDML(t)));
        }
        if (schemaMapper.getHubSchema() != null) {
            hubSchema = schemaMapper.getHubSchema();
            hubSchema.getTables().forEach(t -> dmlMap.put("create." + t.getDatabaseName() + "_" + t.getTableName() + ".hql", getHubDML(t)));
        }
        if (schemaMapper.getSatelliteSchema() != null) {
            satelliteSchema = schemaMapper.getSatelliteSchema();
            satelliteSchema.getTables().forEach(t -> dmlMap.put("create." + t.getDatabaseName() + "_" + t.getTableName() + ".hql", getSatelliteDML(t)));
        }
        if (schemaMapper.getLinkSchema() != null) {
            linkSchema = schemaMapper.getLinkSchema();
            linkSchema.getTables().forEach(t -> dmlMap.put("create." + t.getDatabaseName() + "_" + t.getTableName() + ".hql", getLinkDML(t)));
        }

        return dmlMap;
    }

    public String getStagingDML(HiveTable t) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ");
        sb.append(t.getDatabaseName());
        sb.append(".");
        sb.append(t.getTableName());
        sb.append(" ADD PARTITION (");
        StringJoiner partitionJoiner = new StringJoiner(",");
        for (HiveTable.HiveColumn c : t.getPartitionColumns()) {
            partitionJoiner.add(c.getColumnName() + "=${hivevar:" + CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, c.getColumnName()) + "}");
        }
        sb.append(partitionJoiner.toString());
        sb.append(") LOCATION '");
        sb.append(t.getHdfsLocation()).append("/");
        sb.append(partitionJoiner.toString().replace(",", "/")).append("';");
        return sb.toString();
    }

    public String getTransientDML(HiveTable t) {
        StringBuilder sb = new StringBuilder();
        StringJoiner partitionJoiner = new StringJoiner(",");
        StringJoiner columnJoiner = new StringJoiner(",\n");
        StringJoiner hiveParameterJoiner = new StringJoiner("\nAND ");
        StringJoiner columnNames = new StringJoiner(",\n\t");
        for (HiveTable stagingTable : stagingSchema.getTables()) {
            if (stagingTable.getSourceTableName().equals(t.getSourceTableName())) {
                stagingTable.getPartitionColumns().forEach(stagingPartitionColumn -> {
                    hiveParameterJoiner.add(stagingPartitionColumn.getColumnName() + " = ${hivevar:" + CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, stagingPartitionColumn.getColumnName()) + "}");
                });
            }
        }

        for (HiveTable.HiveColumn c : t.getColumns()) {
            columnNames.add(c.getColumnName());
            columnJoiner.add("\t" + c.getColumnName());
        }
        for (HiveTable.HiveColumn c : t.getPartitionColumns()) {
            columnNames.add(c.getColumnName());
            columnJoiner.add("\t${hivevar:" + CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, c.getColumnName()) + "} " + c.getColumnName());
            partitionJoiner.add(c.getColumnName());
        }

        sb.append("INSERT INTO ").append(t.getDatabaseName()).append(".").append(t.getTableName()).append("\n");
        sb.append("PARTITION (").append(partitionJoiner.toString()).append(")\n\t(").append(columnNames.toString()).append(")\n");
        sb.append("SELECT").append("\n");
        sb.append(columnJoiner.toString()).append("\n");
        sb.append("FROM ").append(t.getSourceDatabaseName()).append(".").append(t.getTableName()).append("\n");
        sb.append("WHERE ").append(hiveParameterJoiner);
        return sb.toString();
    }

    public String getHubDML(HiveTable h) {
        StringBuilder sb = new StringBuilder();
        StringBuilder hubKeyJoiner = new StringBuilder("");
        StringJoiner columnJoiner = new StringJoiner(",\n");
        String hubKeyDefinition = null;
        StringJoiner hiveParameterJoiner = new StringJoiner("\nAND ");
        StringJoiner columnNames = new StringJoiner(",\n\t");
        StringJoiner partitionJoiner = new StringJoiner(",");
        for (HiveTable transientTable : transientSchema.getTables()) {
            if (transientTable.getSourceTableName().equals(h.getSourceTableName())) {
                transientTable.getPartitionColumns().forEach(transientPartitionColumn -> {
                    hiveParameterJoiner.add(h.getSourceTable().getTableName() + "." + transientPartitionColumn.getColumnName() + " = ${hivevar:" + CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, transientPartitionColumn.getColumnName()) + "}");
                });
            }
        }

        for (HiveTable hubs : hubSchema.getTables()) {
            if (hubs.getSourceTable().equals(h.getSourceTable())) {
                hubKeyDefinition = getHubKey(hubs, hubs.getSourceTable().getTableName());
            }
        }

        for (HiveTable.HiveColumn c : h.getColumns()) {

            h.getColumns().forEach(col -> {
                if (c.isHubKey()) {
                    hubKeyJoiner.append(col.getColumnName());
                } else if (c.isBusinessKey()) {
                columnJoiner.add("\t" + getFormattedColumnDefinition(h.getSourceTable().getTableName() + "." + c.getSourceColumnName(), c.getDataType()) + " " + c.getColumnName());
                } else if (c.isLoadDate()) {
                columnJoiner.add("\t" + h.getSourceTable().getTableName() + "." + c.getColumnName());
            }
            columnNames.add(c.getColumnName());
            });
        }

        for (HiveTable.HiveColumn cols : h.getPartitionColumns()) {
            partitionJoiner.add(cols.getColumnName());
        }

        sb.append("INSERT INTO ").append(h.getDatabaseName()).append(".").append(h.getTableName()).append(" \n");
        sb.append("PARTITION (").append(partitionJoiner.toString()).append(")\n\t(").append(columnNames.toString()).append(")\n");
        sb.append("SELECT DISTINCT\n");
        sb.append("\tupper(concat_ws(\"").append(h.getHubKeyDelimiter()).append("\",").append(hubKeyDefinition).append(")) ").append(hubKeyJoiner).append(",\n");
        sb.append(columnJoiner.toString() + "\n");
        sb.append("FROM ").append(h.getSourceDatabaseName()).append(".").append(h.getSourceTable().getTableName()).append(" ").append(h.getSourceTable().getTableName()).append("\n");
        sb.append("WHERE NOT EXISTS (\tSELECT 1 FROM ").append(h.getDatabaseName()).append(".").append(h.getTableName()).append("\n");
        sb.append("\t\t\t\t\tWHERE ").append("upper(concat_ws(\"").append(h.getHubKeyDelimiter()).append("\",").append(hubKeyDefinition).append(")) = ").append(h.getTableName()).append(".").append(hubKeyJoiner).append(")\n");
        sb.append("AND ").append(hiveParameterJoiner).append(";\n");

        return sb.toString();
    }

    public String getSatelliteDML(SatelliteSchema.SatelliteTable s) {
        StringBuilder sb = new StringBuilder();
        /*
        String hubKeyDelimiter = null;
        StringJoiner columnJoiner = new StringJoiner(",\n");
        String tableAlias = s.getSourceTable().getTableName();
        String hubKeyDefinition = null;
        StringJoiner columnNames = new StringJoiner(",\n\t");
        StringJoiner partitionJoiner = new StringJoiner(",");
        StringJoiner hiveParameterJoiner = new StringJoiner("\nAND ");
        for (HiveTable transientTable : transientSchema.getTables()) {
            if (transientTable.getSourceTableName().equals(s.getSourceTableName())) {
                transientTable.getPartitionColumns().forEach(transientPartitionColumn -> {
                    hiveParameterJoiner.add(s.getSourceTable().getTableName() + "." + transientPartitionColumn.getColumnName() + " = ${hivevar:" + CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, transientPartitionColumn.getColumnName()) + "}");
                    //System.out.println(hiveParameterJoiner);
                });
            }
        }
        for (HubSchema.HubTable h : hubSchema.getTables()) {
            if (h.getSourceTable().equals(s.getSourceTable())) {
                hubKeyDefinition = getHubKey(h, h.getSourceTable().getTableName());
                hubKeyDelimiter = h.getHubKeyDelimiter();
            }
        }
        for (SatelliteSchema.SatelliteTable.HiveColumn c : s.getColumns()) {

            if (c.getColumnName().equals(s.getHubKey().getColumnName())) {
                columnJoiner.add("\tupper(concat_ws(\"" + hubKeyDelimiter + "\"," + hubKeyDefinition + "))  " + s.getHubKey().getColumnName());
            } else if (c.getColumnName().equals(s.getLoadDate().getColumnName())) {
                columnJoiner.add("\t" + s.getSourceTable().getTableName() + "." + c.getColumnName());
            } else {
                columnJoiner.add("\t" + tableAlias + "." + c.getColumnName());
            }
            columnNames.add(c.getColumnName());
        }
        for (SatelliteSchema.SatelliteTable.HiveColumn p : s.getPartitionColumns()) {
            partitionJoiner.add(p.getColumnName());
        }
        sb.append("INSERT INTO ").append(s.getDatabaseName()).append(".").append(s.getTableName()).append("\n");
        sb.append("PARTITION (").append(partitionJoiner.toString()).append(")\n\t(").append(columnNames.toString()).append(")\n");
        sb.append("SELECT \n");
        sb.append(columnJoiner.toString() + "\n");
        sb.append("FROM ").append(s.getSourceDatabaseName()).append(".").append(s.getSourceTable().getTableName()).append(" ").append(tableAlias).append(" \n");
        sb.append("WHERE ").append(hiveParameterJoiner).append(";\n");
*/
        return sb.toString();
    }

    public String getLinkDML(LinkSchema.LinkTable l) {
        StringBuilder sb = new StringBuilder();
        /*
        String hubKeyDelimiter = null;
        StringJoiner columnNames = new StringJoiner(",\n\t");
        if (l.getColumns().size() > 2) {
            StringJoiner columnJoiner = new StringJoiner(", \n");
            StringJoiner linkKeyJoiner = new StringJoiner(", ");
            StringJoiner tableJoiner = new StringJoiner("\nLEFT JOIN ");
            StringJoiner hiveParameterJoiner = new StringJoiner("\nAND ");
            StringJoiner partitionJoiner = new StringJoiner(",");
            for (LinkSchema.LinkTable.HiveColumn c : l.getPartitionColumns()) {
                partitionJoiner.add(c.getColumnName());
            }
            for (HiveTable transientTable : transientSchema.getTables()) {
                if (transientTable.getSourceTableName().equals(l.getSourceTableName())) {
                    transientTable.getPartitionColumns().forEach(transientPartitionColumn -> {
                        hiveParameterJoiner.add(l.getSourceTable().getTableName() + "." + transientPartitionColumn.getColumnName() + " = ${hivevar:" + CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, transientPartitionColumn.getColumnName()) + "}");
                    });
                }
            }
            for (LinkSchema.LinkTable.HiveColumn c : l.getColumns()) {
                if (!c.equals(l.getLinkKey()) && !c.equals(l.getLoadDate())) {
                    for (HubSchema.HubTable h : hubSchema.getTables()) {
                        if (h.getSourceTable().equals(l.getSourceTable())) {

                            hubKeyDelimiter = h.getHubKeyDelimiter();
                        }
                        if (h.getTableName().equals(c.getForeignKeyTable()) || c.getColumnName().equals(h.getHubKey().getColumnName())) {

                            String hubKeyDefinition = getHubKey(h, h.getSourceTable().getTableName() + "_" + c.getColumnName());
                            linkKeyJoiner.add(hubKeyDefinition);
                            columnJoiner.add("\t" + hubKeyDefinition + " " + h.getHubKey().getColumnName());
                            if (!l.getSourceTableName().equals(h.getSourceTable().getTableName())) {
                                tableJoiner.add(l.getSourceDatabaseName() + "." + h.getSourceTable().getTableName() + " " + h.getSourceTable().getTableName() + "_" + c.getColumnName()
                                        + " ON " + h.getSourceTable().getTableName() + "_" + c.getColumnName() + "." + c.getSourceColumnName() + " = " + h.getSourceTable().getTableName() + "." + c.getSourceColumnName());


                            }

                        }
                    }
                } else if (c.equals(l.getLoadDate())) {
                    columnJoiner.add("\t" + l.getSourceTable().getTableName() + "." + c.getColumnName());
                }
                columnNames.add(c.getColumnName());
            }
            sb.append("INSERT INTO ").append(l.getDatabaseName()).append(".").append(l.getTableName()).append("\n");
            sb.append("PARTITION (").append(partitionJoiner.toString()).append(")\n\t(").append(columnNames.toString()).append(")\n");
            sb.append("SELECT DISTINCT\n");
            sb.append("\tupper(concat_ws(\"").append(hubKeyDelimiter).append("\",").append(linkKeyJoiner).append(" ").append(l.getLinkKey().getColumnName()).append("\n");
            sb.append(columnJoiner.toString() + "\n");
            sb.append("FROM ").append(l.getSourceDatabaseName()).append(".").append(l.getSourceTable().getTableName()).append(" ").append(l.getSourceTable().getTableName()).append(" \n");
            sb.append("LEFT JOIN " + tableJoiner.toString()).append("\n");
            sb.append("WHERE ").append(hiveParameterJoiner).append(";\n");
        }
        */

        return sb.toString();
    }

    private String getHubKey(HiveTable table, String alias) {
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
        for (HiveTable.HiveColumn c : businessKeys) {
            hubKeyJoiner.add(getFormattedColumnDefinition(alias + "." + c.getColumnName(), c.getDataType()));
        }
        return hubKeyJoiner.toString();
    }

    private String getFormattedColumnDefinition(String columnName, String dataType) {
        String formattedColumnDefinition = "";
        if (dataType.equals("STRING")) {
            formattedColumnDefinition = "upper(nvl(regexp_replace(" + columnName + ",'\"',''),''))";
        } else {
            formattedColumnDefinition = "nvl(cast(" + columnName + " as STRING),'')";
        }
        return formattedColumnDefinition;
    }
}
