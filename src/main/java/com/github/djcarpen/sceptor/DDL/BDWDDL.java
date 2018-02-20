package com.github.djcarpen.sceptor.DDL;

import com.github.djcarpen.sceptor.Schema.DataVault.HubSchema;
import com.github.djcarpen.sceptor.Schema.DataVault.LinkSchema;
import com.github.djcarpen.sceptor.Schema.DataVault.SatelliteSchema;
import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.Schema;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class BDWDDL implements DDL {
    private Properties Props = new Properties();

    private Map<String, String> ddlMap;
    private Map<String, String> ddlMapHubs;
    private Map<String, String> ddlMapSatellites;
    private Map<String, String> ddlMapLinks;

    public BDWDDL() {
        try {
            Props.load(new FileInputStream("Runtime.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map getDDLs(List<Schema> schemas) {

        ddlMap = new LinkedHashMap<>();
        ddlMap.putAll(getHubDDLS(schemas.get(0)));
        ddlMap.putAll(getSatelliteDDLS(schemas.get(1)));
        ddlMap.putAll(getLinkDDLs(schemas.get(2)));
        return ddlMap;
    }


    public Map getHubDDLS(Schema schema) {
        ddlMapHubs = new LinkedHashMap<>();
        for (HubSchema.HubTable t : ((HubSchema) schema).getTables()) {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE IF NOT EXISTS ").append(Props.getProperty("bdwDatabaseName")).append(".").append(t.getTableName()).append("(\n");
            StringJoiner columnJoiner = new StringJoiner(",\n");
            for (HiveTable.HiveColumn c : t.getColumns()) {
                columnJoiner.add("\t" + c.getColumnName() + " " + c.getDataType());
            }
            sb.append(columnJoiner.toString());
            sb.append(") \nCLUSTERED BY (");
            sb.append(t.getColumns().get(0).getColumnName());
            sb.append(") INTO 1 BUCKETS\n");
            sb.append("STORED AS ").append(Props.getProperty("bdwStoredAs")).append("\n");
            sb.append("LOCATION '").append(Props.getProperty("bdwHdfsPath")).append("'\n");
            sb.append("TBLPROPERTIES (\"TRANSACTIONAL\"=\"TRUE\");");
            ddlMapHubs.put("create_" + t.getTableName() + ".hql", sb.toString());
        }
        return ddlMapHubs;
    }

    public Map getSatelliteDDLS(Schema schema) {

        ddlMapSatellites = new LinkedHashMap<>();
        for (SatelliteSchema.SatelliteTable t : ((SatelliteSchema) schema).getTables()) {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE IF NOT EXISTS ").append(Props.getProperty("bdwDatabaseName")).append(".").append(t.getTableName()).append("(\n");
            StringJoiner columnJoiner = new StringJoiner(",\n");
            for (HiveTable.HiveColumn c : t.getColumns()) {
                columnJoiner.add("\t" + c.getColumnName() + " " + c.getDataType());
            }
            sb.append(columnJoiner.toString());
            sb.append(") \nCLUSTERED BY (");
            sb.append(t.getColumns().get(0).getColumnName());
            sb.append(") INTO 1 BUCKETS\n");
            sb.append("STORED AS ").append(Props.getProperty("bdwStoredAs")).append("\n");
            sb.append("LOCATION '").append(Props.getProperty("bdwHdfsPath")).append("'\n");
            sb.append("TBLPROPERTIES (\"TRANSACTIONAL\"=\"TRUE\");");
            ddlMapSatellites.put("create_" + t.getTableName() + ".hql", sb.toString());
        }
        return ddlMapSatellites;
    }

    public Map getLinkDDLs(Schema schema) {
        ddlMapLinks = new LinkedHashMap<>();
        for (LinkSchema.LinkTable t : ((LinkSchema) schema).getTables()) {
            if (t.getColumns().size() > 2) {

                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE IF NOT EXISTS ").append(Props.getProperty("bdwDatabaseName")).append(".").append(t.getTableName()).append("(\n");
                StringJoiner columnJoiner = new StringJoiner(",\n");
                for (HiveTable.HiveColumn c : t.getColumns()) {
                    columnJoiner.add("\t" + c.getColumnName() + " " + c.getDataType());
                }
                sb.append(columnJoiner.toString());
                sb.append(") \nCLUSTERED BY (");
                sb.append(t.getColumns().get(0).getColumnName());
                sb.append(") INTO 1 BUCKETS\n");
                sb.append("STORED AS ").append(Props.getProperty("bdwStoredAs")).append("\n");
                sb.append("LOCATION '").append(Props.getProperty("bdwHdfsPath")).append("'\n");
                sb.append("TBLPROPERTIES (\"TRANSACTIONAL\"=\"TRUE\");");
                ddlMapLinks.put("create_" + t.getTableName() + ".hql", sb.toString());

            }
        }
        return ddlMapLinks;

    }
}
