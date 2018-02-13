package com.github.djcarpen.sceptor.Schema.DataVault;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.DataDictionary.Table;
import com.github.djcarpen.sceptor.Schema.DataDictionary.Table.Column;
import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.Schema;

import java.util.ArrayList;
import java.util.List;

public class HubSchema implements Schema {

    private List<HubTable> hubTables = new ArrayList<>();


    public List<HubTable> getTables() {
        return hubTables;
    }

    public void generateTables(DataDictionary dataDictionary) {
        for (Table t : dataDictionary.getTables()) {
            HubTable hubTable = new HubTable();
            hubTable.setSourceTableName(t.getTableName());
            hubTable.setDatabaseName(t.getDatabaseName());
            hubTable.setTableName("H_" + t.getTableName());
            hubTable.setHubKey(t);
            hubTable.setHubKeyDefinition(t);
            hubTable.setLoadDate();
            hubTable.setBusinessKeys(t);
            hubTable.setColumns();
            hubTables.add(hubTable);
        }
    }


    public static class HubTable extends HiveTable {
        private HiveColumn hubKey;
        private HiveColumn loadDate;
        private List<HiveColumn> businessKeys;
        private List<HiveColumn> hubColumns;
        private String hubKeyDefinition;

        public String getHubKeyDefinition() {
            return hubKeyDefinition;
        }

        public void setHubKeyDefinition(Table table) {
            HubKey hubKey = new HubKey();
            hubKeyDefinition = hubKey.getHubKey(table);
        }

        @Override
        public List<HiveColumn> getColumns() {
            hubColumns = new ArrayList<>();
            List<HiveColumn> columnList;
            columnList = new ArrayList<>();
            columnList.add(hubKey);
            businessKeys.sort(new HiveTable.OrderByHiveColumnName());
            columnList.addAll(businessKeys);
            columnList.add(loadDate);
            for (HiveColumn c : columnList) {
                c.setColumnOrder(columnList.indexOf(c));
            }
            hubColumns = columnList;
            return hubColumns;
        }

        public HiveColumn getHubKey() {
            return hubKey;
        }

        public void setHubKey(Table sourceTable) {
            this.hubKey = hubKey;
            hubKey = new HiveColumn("hk_" + sourceTable.getTableName(), "STRING"); // create hubTable key column
        }

        public HiveColumn getLoadDate() {
            return loadDate;
        }

        public void setLoadDate() {
            this.loadDate = loadDate;
            loadDate = new HiveColumn("load_dt", "TIMESTAMP");
        }

        public List<HiveColumn> getBusinessKeys() {
            businessKeys.sort(new HiveTable.OrderByHiveColumnName());
            return businessKeys;
        }

        public void setBusinessKeys(Table sourceTable) {
            businessKeys = new ArrayList<>();
            for (Column c : sourceTable.getColumns()) {
                if (c.getIsBusinessKey()) {
                    HiveColumn businessKey = new HiveColumn();
                    if (c.getColumnName().equals("id")) {
                        businessKey.setColumnName(sourceTable.getTableName() + "_" + c.getColumnName());
                    } else {
                        businessKey.setColumnName(c.getColumnName());
                    }
                    businessKey.setSourceColumnName(c.getColumnName());
                    businessKey.setSourceTableName(sourceTable.getTableName());
                    businessKey.setDataType(c.getDataType());

                    businessKeys.add(businessKey);
                }
            }
        }

        public void setColumns() {
            hubColumns = new ArrayList<>();
            List<HiveColumn> columnList;
            columnList = new ArrayList<>();
            columnList.add(hubKey);
            businessKeys.sort(new HiveTable.OrderByHiveColumnName());
            columnList.addAll(businessKeys);
            columnList.add(loadDate);
            for (HiveColumn c : columnList) {
                c.setColumnOrder(columnList.indexOf(c));
            }
            hubColumns = columnList;
        }

    }


}



