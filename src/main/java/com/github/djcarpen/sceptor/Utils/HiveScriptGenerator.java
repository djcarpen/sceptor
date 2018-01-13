package com.github.djcarpen.sceptor.Utils;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.DataVault.RawDataWarehouseSchema;
import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.RawSchema;
import com.github.djcarpen.sceptor.Schema.Schema;
import com.github.djcarpen.sceptor.Zone;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringJoiner;

import static com.github.djcarpen.sceptor.Zone.*;

public class HiveScriptGenerator {

    private Schema rawSchema;
    private Schema rawDataWarehouseSchema;
    private Schema DataVaultSchema;

    public void generateDMLFiles(Zone zone, String ddlPath) {
        DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        Date dateobj = new Date();
        String load_dt = df.format(dateobj);
        Schema schema = null;

        if (zone.equals(STAGING) || zone.equals(TRANSIENT)) {
            schema = rawSchema;
            for (HiveTable t : schema.getTables()) {
                StringBuilder sb = new StringBuilder();
                if (zone.equals(STAGING)) {
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
                    System.out.println(sb.toString());
                }
                if (zone.equals(TRANSIENT)) {
                    sb.append("INSERT INTO TABLE ");
                    sb.append(t.getDatabaseName());
                    sb.append(".");
                    sb.append(t.getTableName());
                    sb.append("\npartition (");
                    StringJoiner partitionJoiner = new StringJoiner(",");
                    for (HiveTable.HiveColumn c : t.getPartitionColumns()) {
                        partitionJoiner.add(c.getColumnName());
                    }
                    sb.append(partitionJoiner.toString());
                    sb.append(")\nSELECT ");
                    StringJoiner columnJoiner = new StringJoiner(",\n");
                    for (HiveTable.HiveColumn c : t.getColumns()) {
                        columnJoiner.add("\t" + c.getColumnName());
                    }
                    sb.append(columnJoiner.toString());
                    sb.append(",\n");
                    StringJoiner partitionColumnsJoiner = new StringJoiner(",\n");
                    for (HiveTable.HiveColumn c : t.getPartitionColumns()) {
                        partitionColumnsJoiner.add("\t" + c.getColumnName());
                    }
                    sb.append(partitionColumnsJoiner.toString());
                    sb.append(")\n");
                    sb.append("FROM stg_");
                    sb.append(t.getDatabaseName());
                    sb.append(".");
                    sb.append(t.getTableName());
                    sb.append("\n");
                    sb.append("WHERE edl_ingest_time = ${hivevar:edlIngestTime} and edl_ingest_channel = ${hivevar:edlIngestChannel};\n\n");
                    System.out.println(sb.toString());

                }
            }
        } else if (zone.equals(RDW)) {

            getHubDMLs(RDW, schema);

        }

    }

    public void getLinkDMLs(Zone zone, Schema schema) {


    }

    public void getHubDMLs(Zone zone, Schema schema) {
        DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        Date dateobj = new Date();
        String load_dt = df.format(dateobj);
        schema = rawDataWarehouseSchema;
        for (HiveTable h : ((RawDataWarehouseSchema) schema).getHubs()) {
            System.out.println("TableName: " + h.getTableName() + "    \nHubKey: " + getHubKey(schema, h));
            StringBuilder sb = new StringBuilder();
            List<String> businessKeyColumnNames = new ArrayList<>();
            List<String> nonBusinessKeyColumnNames = new ArrayList<>();
            String hubKeyColumnName = "";
            for (HiveTable.HiveColumn c : h.getColumns()) {
                if (!c.getColumnName().substring(0, 3).equals("hk_")) {
                    if (!c.getColumnName().equals("load_dt")) {
                        businessKeyColumnNames.add(getFormattedColumnDefinition(c.getColumnName(), c.getDataType()));
                    }
                }
            }
            StringJoiner businessKeyJoiner = new StringJoiner(", ");
            for (String s : businessKeyColumnNames) {
                businessKeyJoiner.add(s);
            }
            StringJoiner nonBusinessKeyJoiner = new StringJoiner(",\n");
            for (String s : nonBusinessKeyColumnNames) {
                nonBusinessKeyJoiner.add(s);
            }
            sb.append("INSERT INTO TABLE rdw_").append(h.getDatabaseName()).append(".").append(h.getTableName()).append("\n");
            sb.append("SELECT DISTINCT\n");
            sb.append("\tupper(concat_ws(\"-\",").append(businessKeyJoiner.toString()).append(hubKeyColumnName).append(",\n");
            sb.append(nonBusinessKeyJoiner.toString()).append("\n");
            sb.append("FROM stg_").append(h.getDatabaseName()).append(".").append(h.getDatabaseName()).append(" stg\n");
            sb.append("WHERE NOT EXIISTS (\tSELECT 1 FROM ").append(h.getDatabaseName()).append(".").append(h.getTableName()).append(" hub\n");
            sb.append("\t\t\t\t\tWHERE ").append(businessKeyJoiner.toString()).append(" = hub.").append(hubKeyColumnName).append(")\n");
            sb.append("AND edl_ingest_time >= ${hivevar:edlIngestTime}\n");
            sb.append("AND edl_ingest_channel = ${hivevar:edlIngestChannel};\n");
            sb.append("\n\n");

            //System.out.println(sb.toString());

        }

    }

    public String getHubKey(Schema schema, HiveTable hiveTable) {
        schema = rawDataWarehouseSchema;
        StringBuilder sb = new StringBuilder();
        String hubKeyColumnName = "";
        StringJoiner businessKeyJoiner = new StringJoiner(", ");
        List<String> businessKeyColumnNames = new ArrayList<>();
        for (HiveTable h : ((RawDataWarehouseSchema) schema).getHubs()) {

            if (h.getTableName().equals(hiveTable.getTableName())) {
//System.out.println(h.getTableName()+"    "+hiveTable.getTableName());


                for (HiveTable.HiveColumn c : h.getColumns()) {
                    if (!c.getColumnName().substring(0, 3).equals("hk_") && !c.getColumnName().equals("load_dt")) {

                        businessKeyColumnNames.add(getFormattedColumnDefinition(c.getColumnName(), c.getDataType()));
                        //System.out.println(c.getColumnName());
                    } else if (c.getColumnName().substring(0, 3).equals("hk_")) hubKeyColumnName = c.getColumnName();
                }
                for (String s : businessKeyColumnNames) {
                    businessKeyJoiner.add(s);
                }
                //System.out.println(businessKeyJoiner.toString());
            }

        }
        sb.append("\tupper(concat_ws(\"-\",").append(businessKeyJoiner.toString()).append(hubKeyColumnName).append(",\n");
        return sb.toString();
    }


    public String getFormattedColumnDefinition(String columnName, String dataType) {
        String formattedColumnDefinition = "";
        if (dataType.equals("STRING")) {
            formattedColumnDefinition = "upper(nvl(regexp_replace(stg." + columnName + ", '\"', ''), '')) ";
        } else {
            formattedColumnDefinition = "nvl(cast(stg." + columnName + ") as STRING), '') ";
        }
        return formattedColumnDefinition;
    }


    public void generateDDLFiles(Zone zone, String ddlPath) {
        Schema schema;
        if (zone.equals(STAGING) || zone.equals(TRANSIENT)) schema = rawSchema;
        else if (zone.equals(RDW) || zone.equals(BDW)) schema = rawDataWarehouseSchema;
        else schema = null;
        assert schema != null;
        for (HiveTable t : schema.getTables()) {
            StringBuilder sb = new StringBuilder();
            if (zone.equals(STAGING)) {
                sb.append("CREATE TABLE stg_").append(t.getDatabaseName()).append(".").append(t.getTableName()).append("(\n");
            }
            if (zone.equals(TRANSIENT)) {
                sb.append("CREATE TABLE raw_").append(t.getDatabaseName()).append(".").append(t.getTableName()).append("(\n");

            }
            if (zone.equals(RDW)) {
                sb.append("CREATE TABLE rdw_").append(t.getDatabaseName()).append(".").append(t.getTableName()).append("(\n");
            } else if (zone.equals(BDW)) {
                sb.append("CREATE TABLE bdw_").append(t.getDatabaseName()).append(".").append(t.getTableName()).append("(\n");
            }
            StringJoiner columnJoiner = new StringJoiner(",\n");
            for (HiveTable.HiveColumn c : t.getColumns()) {
                columnJoiner.add("\t" + c.getColumnName() + " " + c.getDataType());
            }
            sb.append(columnJoiner.toString());
            sb.append(")\n");
            if (zone.equals(STAGING) || zone.equals(TRANSIENT)) {
                sb.append("PARTITIONED BY (");
                StringJoiner partitionJoiner = new StringJoiner(",");
                for (HiveTable.HiveColumn c : t.getPartitionColumns()) {
                    partitionJoiner.add(c.getColumnName() + " " + c.getDataType());
                }
                sb.append(partitionJoiner.toString());
                sb.append(")\n");
                if (zone.equals(STAGING)) {
                    sb.append("ROW FORMAT DELIMITED\n");
                    sb.append("FIELDS TERMINATED BY '\\u001F'\n");
                    sb.append("STORED AS TEXTFILE\n");
                    sb.append("LOCATION '");
                    sb.append(t.getHdfsLocation());
                    sb.append("';\n");
                } else {
                    sb.append("STORED AS ORC;\n");
                }
            } else if (zone.equals(BDW)) {
                sb.append(") \nCLUSTERED BY (");
                sb.append(t.getColumns().get(0).getColumnName());
                sb.append(") INTO 1 BUCKETS\n");
                sb.append("STORED AS ORC\n");
                sb.append("TBLPROPERTIES (\"TRANSACTIONAL\"=\"TRUE\");");
            } else {
                sb.append("STORED AS ORC;\n");
            }
            sb.append("\n\n");
            FileWriter fileWriter = new FileWriter();
            fileWriter.writeFile(sb.toString(), ddlPath, "create_" + t.getTableName() + ".hql");
            System.out.println(sb.toString());
        }
    }


    public void generateSchemas(DataDictionary dataDictionary) {
        rawSchema = new RawSchema();
        rawSchema.generateTables(dataDictionary);
        rawDataWarehouseSchema = new RawDataWarehouseSchema();
        rawDataWarehouseSchema.generateTables(dataDictionary);
    }

}