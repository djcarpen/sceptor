package com.ncr.eda.apollo.sceptor;

import com.ncr.eda.apollo.sceptor.Schema.RDW.HubSchema;
import com.ncr.eda.apollo.sceptor.Schema.RDW.LinkSchema;
import com.ncr.eda.apollo.sceptor.Schema.RDW.SatelliteSchema;
import com.ncr.eda.apollo.sceptor.Schema.StagingSchema;
import com.ncr.eda.apollo.sceptor.Schema.TransientSchema;

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
        Map<String, String> ddlMap = new LinkedHashMap<>();

        if (schemaMapper.getStagingSchema() != null) {
            stagingSchema = schemaMapper.getStagingSchema();
            stagingSchema.getTables().forEach(t -> ddlMap.put("create." + t.getDatabaseName() + "_" + t.getTableName() + ".hql", getTableDDL(t)));
        }
        if (schemaMapper.getTransientSchema() != null) {
            transientSchema = schemaMapper.getTransientSchema();
            transientSchema.getTables().forEach(t -> ddlMap.put("create." + t.getDatabaseName() + "_" + t.getTableName() + ".hql", getTableDDL(t)));
        }
        if (schemaMapper.getHubSchema() != null) {
            hubSchema = schemaMapper.getHubSchema();
            hubSchema.getTables().forEach(t -> ddlMap.put("create." + t.getDatabaseName() + "_" + t.getTableName() + ".hql", getTableDDL(t)));
        }
        if (schemaMapper.getSatelliteSchema() != null) {
            satelliteSchema = schemaMapper.getSatelliteSchema();
            satelliteSchema.getTables().forEach(t -> ddlMap.put("create." + t.getDatabaseName() + "_" + t.getTableName() + ".hql", getTableDDL(t)));
        }
        if (schemaMapper.getLinkSchema() != null) {
            linkSchema = schemaMapper.getLinkSchema();
            linkSchema.getTables().forEach(t -> ddlMap.put("create." + t.getDatabaseName() + "_" + t.getTableName() + ".hql", getTableDDL(t)));
        }

        return ddlMap;
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


}
