package com.github.djcarpen.sceptor.Schema.DataVault;

import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.SourceSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Hub {

    private HiveTable hub;
    private List<HiveTable> hubs = new ArrayList<>();
    private HiveTable.HiveColumn businessKey;
    private HiveTable.HiveColumn hubKey;
    private HiveTable.HiveColumn loadDate;
    private List<HiveTable.HiveColumn> businessKeys;
    private List<HiveTable.HiveColumn> hubColumns;

    public List<HiveTable> getHubs() {
        return hubs;
    }

    public void generateHubs(SourceSchema sourceSchema){
        for (HiveTable t : sourceSchema.getTables()) {
            generateHubColumns(t);
            if (!hubColumns.isEmpty()) {
                hub = new HiveTable();
                hub.setDatabaseName(t.getDatabaseName());
                hub.setTableName("H_" + t.getTableName());
//            System.out.println("HUB Tablename: " + hub.getTableName());
                for (HiveTable.HiveColumn c : hubColumns) {
                    hub.addColumn(c);
//                System.out.println("      HUB Columnname: "+c.getColumnName());
                }
                hubs.add(hub);
            }
        }
    }

    public void generateHubColumns(HiveTable sourceTable) {
        hubKey = new HiveTable.HiveColumn("hk_" + sourceTable.getTableName(), "STRING"); // create hub key column
        loadDate = new HiveTable.HiveColumn("load_dt","TIMESTAMP");
        businessKeys = new ArrayList<>();
        for (HiveTable.HiveColumn c : sourceTable.getColumns()) {
            if (c.getIsBusinessKey() == true) {

                businessKey = new HiveTable.HiveColumn();
                if (c.getColumnName().equals("id")) {
                    businessKey.setColumnName(sourceTable.getTableName() + "_" + c.getColumnName()); // prepend table name to id columns
                } else {
                    businessKey.setColumnName(c.getColumnName());
                }
                businessKey.setDataType(c.getDataType());

                businessKeys.add(businessKey);
                }
            }
            hubColumns = new ArrayList<>();
            hubColumns = sortHubColumns(hubKey, businessKeys, loadDate);
        }

    public List<HiveTable.HiveColumn> sortHubColumns (HiveTable.HiveColumn hubKey, List<HiveTable.HiveColumn> businessKeys, HiveTable.HiveColumn loadDate) {
        this.hubKey = hubKey;
        this.businessKeys = businessKeys;
        this.loadDate = loadDate;
        List<HiveTable.HiveColumn> columnList = new ArrayList<>();
        columnList.add(hubKey);
        Collections.sort(businessKeys, new HiveTable.OrderByHiveColumnName());
        columnList.addAll(businessKeys);
        columnList.add(loadDate);
        for (HiveTable.HiveColumn c : columnList) {
            c.setColumnOrder(columnList.indexOf(c));
        }
        return columnList;
    }



}



