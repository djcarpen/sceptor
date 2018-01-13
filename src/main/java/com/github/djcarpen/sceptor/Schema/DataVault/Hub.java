package com.github.djcarpen.sceptor.Schema.DataVault;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.DataDictionary.Table;
import com.github.djcarpen.sceptor.Schema.DataDictionary.Table.Column;
import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.HiveTable.HiveColumn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.github.djcarpen.sceptor.Schema.DataVault.EntityType.HUB;

public class Hub implements DataVaultSchema {

    private List<HiveTable> hubTables = new ArrayList<>();
    public static final EntityType entityType = HUB;
    private HiveColumn hubKey;
    private HiveColumn loadDate;
    private List<HiveColumn> businessKeys;
    private List<HiveColumn> hubColumns;
    private Map<String, Map<String, HiveColumn>> hiveTableMap;

    public static EntityType getEntityType() {
        return entityType;
    }

    public List<HiveTable> getHubTables() {
        return hubTables;
    }


    public HiveColumn getLoadDate() {
        return loadDate;
    }

    public List<HiveColumn> getBusinessKeys() {
        return businessKeys;
    }

    public List<HiveColumn> getHubColumns() {
        return hubColumns;
    }

    public List<HiveTable> getTables() {
        return hubTables;
    }

    public void generateTables(DataDictionary dataDictionary) {
        for (Table t : dataDictionary.getTables()) {
            generateColumns(t);
            if (!hubColumns.isEmpty()) {
                HiveTable hubTable = new HiveTable();
                hubTable.setDatabaseName(t.getDatabaseName());
                hubTable.setTableName("H_" + t.getTableName());
                for (HiveColumn c : hubColumns) {
                    hubTable.addColumn(c);
                }
                hubTables.add(hubTable);
            }
        }
    }

    public void generateColumns(Table sourceTable) {
        hubKey = new HiveColumn("hk_" + sourceTable.getTableName(), "STRING"); // create hubTable key column
        loadDate = new HiveColumn("load_dt", "TIMESTAMP");
        businessKeys = new ArrayList<>();
        for (Column c : sourceTable.getColumns()) {
            if (c.getIsBusinessKey()) {
                HiveColumn businessKey = new HiveColumn();
                if (c.getColumnName().equals("id")) {
                    businessKey.setColumnName(sourceTable.getTableName() + "_" + c.getColumnName()); // prepend table name to id columns
                } else {
                    businessKey.setColumnName(c.getColumnName());
                }
                businessKey.setDataType(c.getDataType());
                businessKeys.add(businessKey);
//                Map<String,HiveColumn> columnMap = new HashMap<>();
//                columnMap.put("businessKey",businessKey);
//                hiveTableMap.put("H_"+sourceTable,columnMap);
            }

        }
        hubColumns = new ArrayList<>();
        hubColumns = sortColumns();
    }

    public List<HiveColumn> sortColumns() {
        List<HiveColumn> columnList;
        columnList = new ArrayList<>();
        columnList.add(hubKey);
        businessKeys.sort(new HiveTable.OrderByHiveColumnName());
        columnList.addAll(businessKeys);
        columnList.add(loadDate);
        for (HiveColumn c : columnList) {
            c.setColumnOrder(columnList.indexOf(c));
        }
        return columnList;
    }

}



