package com.github.djcarpen.sceptor.Schema.DataVault;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.HiveTable;

import java.util.ArrayList;
import java.util.List;

public class Hub implements DataVaultSchema {

    private List<HiveTable> hubTables = new ArrayList<>();
    private HiveTable.HiveColumn hubKey;
    private HiveTable.HiveColumn loadDate;
    private List<HiveTable.HiveColumn> businessKeys;
    private List<HiveTable.HiveColumn> hubColumns;

    public List<HiveTable> getTables() {
        return hubTables;
    }

    public void generateTables(DataDictionary dataDictionary) {
        for (HiveTable t : dataDictionary.getTables()) {
            generateColumns(t);
            if (!hubColumns.isEmpty()) {
                HiveTable hubTable = new HiveTable();
                hubTable.setDatabaseName(t.getDatabaseName());
                hubTable.setTableName("H_" + t.getTableName());
//            System.out.println("HUB Tablename: " + hubTable.getTableName());
                for (HiveTable.HiveColumn c : hubColumns) {
                    hubTable.addColumn(c);
//                System.out.println("      HUB Columnname: "+c.getColumnName());
                }
                hubTables.add(hubTable);
            }
        }
    }

    public void generateColumns(HiveTable sourceTable) {
        hubKey = new HiveTable.HiveColumn("hk_" + sourceTable.getTableName(), "STRING"); // create hubTable key column
        loadDate = new HiveTable.HiveColumn("load_dt", "TIMESTAMP");
        businessKeys = new ArrayList<>();
        for (HiveTable.HiveColumn c : sourceTable.getColumns()) {
            if (c.getIsBusinessKey()) {

                HiveTable.HiveColumn businessKey = new HiveTable.HiveColumn();

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
        hubColumns = sortColumns();
    }

    public List<HiveTable.HiveColumn> sortColumns() {
        List<HiveTable.HiveColumn> columnList = new ArrayList<>();
        columnList.add(hubKey);
        businessKeys.sort(new HiveTable.OrderByHiveColumnName());
        columnList.addAll(businessKeys);
        columnList.add(loadDate);
        for (HiveTable.HiveColumn c : columnList) {
            c.setColumnOrder(columnList.indexOf(c));
        }
        return columnList;
    }


}



