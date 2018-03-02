package com.github.djcarpen.sceptor;

import com.github.djcarpen.sceptor.Schema.RDW.HubSchema;
import com.github.djcarpen.sceptor.Schema.RDW.LinkSchema;
import com.github.djcarpen.sceptor.Schema.RDW.SatelliteSchema;
import com.github.djcarpen.sceptor.Schema.StagingSchema;
import com.github.djcarpen.sceptor.Schema.TransientSchema;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;


public class DDL {


    StagingSchema stagingSchema;
    TransientSchema transientSchema;
    HubSchema hubSchema;
    SatelliteSchema satelliteSchema;
    LinkSchema linkSchema;

    public Map getDDLs(SchemaMapper schemaMapper) {
        stagingSchema = schemaMapper.getStagingSchema();
        transientSchema = schemaMapper.getTransientSchema();
        hubSchema = schemaMapper.getHubSchema();
        satelliteSchema = schemaMapper.getSatelliteSchema();
        linkSchema = schemaMapper.getLinkSchema();

        Map<String, String> dmlMap = new LinkedHashMap<>();
        stagingSchema.getTables().forEach(t -> dmlMap.put("create." + t.getDatabaseName() + "_" + t.getTableName() + ".hql", getTableDDL(t)));
        transientSchema.getTables().forEach(t -> dmlMap.put("create." + t.getDatabaseName() + "_" + t.getTableName() + ".hql", getTableDDL(t)));
        hubSchema.getTables().forEach(t -> dmlMap.put("create." + t.getDatabaseName() + "_" + t.getTableName() + ".hql", getTableDDL(t)));
        satelliteSchema.getTables().forEach(t -> dmlMap.put("create." + t.getDatabaseName() + "_" + t.getTableName() + ".hql", getTableDDL(t)));
        linkSchema.getTables().forEach(t -> dmlMap.put("create." + t.getDatabaseName() + "_" + t.getTableName() + ".hql", getTableDDL(t)));

        return dmlMap;
    }


    private String getTableDDL(HiveTable h) {
        StringBuilder sb = new StringBuilder();

        StringJoiner columnJoiner = new StringJoiner(",\n");
        for (HiveTable.HiveColumn c : h.getColumns()) {
            columnJoiner.add("\t" + c.getColumnName() + " " + c.getDataType());
        }

        StringJoiner partitionJoiner = new StringJoiner(",");
        for (HiveTable.HiveColumn c : h.getPartitionColumns()) {
            partitionJoiner.add(c.getColumnName() + " " + c.getDataType());
        }
        sb.append("CREATE DATABASE IF NOT EXISTS ").append(h.getDatabaseName()).append(";\n");
        sb.append("CREATE ");
        if (h.isExternalTable()) {
            sb.append("EXTERNAL ");
        }
        sb.append("TABLE IF NOT EXISTS ").append(h.getDatabaseName()).append(".").append(h.getTableName()).append(" (\n");

        sb.append(columnJoiner).append(")\n");
        if (h.getPartitionColumns() != null) {
            sb.append("PARTITIONED BY (").append(partitionJoiner).append(")\n");
        }
        if (h.getFieldTerminator() != null) {
            sb.append("ROW FORMAT DELIMITED\n");
            sb.append("FIELDS TERMINATED BY '").append(h.getFieldTerminator()).append("'\n");
        }
        sb.append("STORED AS ").append(h.getStorageFormat()).append("\n");
        sb.append("LOCATION '").append(h.getHdfsLocation()).append("';");
        return sb.toString();
    }




















/*

    private Map getStagingDDLs(StagingSchema schema) {
        Map<String,String> ddlMapStaging = new LinkedHashMap<>();
        String ddl;
        for (HiveTable t : schema.getTables()) {
            StringJoiner columnJoiner = new StringJoiner(",\n");
            for (HiveTable.HiveColumn c : t.getColumns()) {
                columnJoiner.add("\t" + c.getColumnName() + " " + c.getDataType());
            }

            StringJoiner partitionJoiner = new StringJoiner(",");
            for (HiveTable.HiveColumn c : t.getPartitionColumns()) {
                partitionJoiner.add(c.getColumnName() + " " + c.getDataType());
            }

            ddl = getTableDDL(true,t.getDatabaseName(),t.getTableName(),columnJoiner.toString(),partitionJoiner.toString(),t.getFieldTerminator(),t.getStorageFormat(),t.getHdfsLocation());
            ddlMapStaging.put("create_" + t.getTableName() + ".hql", ddl);
        }
        return ddlMapStaging;
    }

    private Map getRdwHubDDLs(HubSchema schema) {
        Map<String,String> ddlMapHubs = new LinkedHashMap<>();
        for (HubSchema.HubTable t : schema.getTables()) {
            String ddl;
            StringJoiner columnJoiner = new StringJoiner(",\n");
            for (HiveTable.HiveColumn c : t.getColumns()) {
                columnJoiner.add("\t" + c.getColumnName() + " " + c.getDataType());
            }
            ddl = getTableDDL(false,t.getDatabaseName(),t.getTableName(),columnJoiner.toString(),null,null,t.getStorageFormat(),t.getHdfsLocation());
            ddlMapHubs.put("create_" + t.getTableName() + ".hql", ddl);
        }
        return ddlMapHubs;
    }

    private Map getRdwSatelliteDDLs(SatelliteSchema schema) {
        Map<String,String> ddlMapSatellites = new LinkedHashMap<>();
        String ddl;
        for (SatelliteSchema.SatelliteTable t : schema.getTables()) {
            StringJoiner columnJoiner = new StringJoiner(",\n");
            for (HiveTable.HiveColumn c : t.getColumns()) {
                columnJoiner.add("\t" + c.getColumnName() + " " + c.getDataType());
            }
            ddl = getTableDDL(false,t.getDatabaseName(),t.getTableName(),columnJoiner.toString(),null,null,t.getStorageFormat(),t.getHdfsLocation());
            ddlMapSatellites.put("create_" + t.getTableName() + ".hql", ddl);
        }
        return ddlMapSatellites;
    }

    private Map getRdwLinkDDLs(LinkSchema schema) {
        Map<String,String> ddlMapLinks = new LinkedHashMap<>();
        String ddl;
        for (LinkSchema.LinkTable t : schema.getTables()) {
            StringJoiner columnJoiner = new StringJoiner(",\n");
            for (HiveTable.HiveColumn c : t.getColumns()) {
                columnJoiner.add("\t" + c.getColumnName() + " " + c.getDataType());
            }
            ddl = getTableDDL(false,t.getDatabaseName(),t.getTableName(),columnJoiner.toString(),null,null,t.getStorageFormat(),t.getHdfsLocation());
            ddlMapLinks.put("create_" + t.getTableName() + ".hql", ddl);
        }
        return ddlMapLinks;
    }

*/


}
