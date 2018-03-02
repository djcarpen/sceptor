package com.github.djcarpen.sceptor.Schema.RDW;

import com.github.djcarpen.sceptor.DataDictionary;
import com.github.djcarpen.sceptor.DataDictionary.Table;
import com.github.djcarpen.sceptor.DataDictionary.Table.Column;
import com.github.djcarpen.sceptor.HiveTable;
import com.github.djcarpen.sceptor.PropertyHandler;
import com.github.djcarpen.sceptor.Schema.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HubSchema implements Schema {


    private List<HubTable> hubTables = new ArrayList<>();
    private List<String> rdwPartitionColumns = new ArrayList<>(Arrays.asList(PropertyHandler.getInstance().getValue("rdwPartitionColumns").split(",")));

    public List<HubTable> getTables() {

        return hubTables;
    }

    public void generateTables(DataDictionary dataDictionary) {
        for (DataDictionary.Table t : dataDictionary.getTables()) {
            HubTable hubTable = new HubTable();
            hubTable.setSourceTableName(t.getTableName());
            hubTable.setDatabaseName(PropertyHandler.getInstance().getValue("rdwDatabaseNamePrefix") + t.getDatabaseName());
            hubTable.setTableName(PropertyHandler.getInstance().getValue("rdwHubTablePrefix") + t.getTableName());
            hubTable.setHubKey(t);
            hubTable.setLoadDate();
            hubTable.setBusinessKeys(t);
            hubTable.setColumns();
            hubTable.setSourceTable(t);
            hubTable.setStorageFormat(PropertyHandler.getInstance().getValue("rdwStoredAs"));
            hubTable.setHdfsLocation(PropertyHandler.getInstance().getValue("rdwHdfsBasePath") + "/rdw/" + t.getCommunityName() + "/public/" + t.getDatabaseName() + "/" + PropertyHandler.getInstance().getValue("rdwHubTablePrefix") + t.getTableName());
            hubTable.setHubKeyDelimiter(PropertyHandler.getInstance().getValue("rdwHubKeyDelimiter"));
            hubTable.setSourceDatabaseName(PropertyHandler.getInstance().getValue("stagingDatabaseNamePrefix") + t.getCommunityName() + "_" + t.getAppCode() + "_" + t.getModuleCode() + "_" + t.getDatabaseName());
            hubTable.addPartitionColumn(hubTable.getLoadDate());
            hubTables.add(hubTable);
        }
    }
    public static class HubTable extends HiveTable {
        private HiveColumn hubKey;
        private HiveColumn loadDate;
        private List<HiveColumn> businessKeys;
        private List<HiveColumn> hubColumns;
        private String hubKeyDefinition;
        private DataDictionary.Table sourceTable;
        private String hubKeyDelimiter;


        public String getHubKeyDelimiter() {
            return hubKeyDelimiter;
        }

        public void setHubKeyDelimiter(String hubKeyDelimiter) {
            this.hubKeyDelimiter = hubKeyDelimiter;
        }

        public String getHubKeyDefinition() {
            return hubKeyDefinition;
        }


        public Table getSourceTable() {
            return sourceTable;
        }

        public void setSourceTable(Table sourceTable) {
            this.sourceTable = sourceTable;
        }

//        public void setHubKeyDefinition(Table table) {
//            HubKey hubKey = new HubKey();
//            hubKeyDefinition = hubKey.getHubKey(table);
//        }

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
            hubKey = new HiveColumn(PropertyHandler.getInstance().getValue("rdwHubKeyPrefix") + sourceTable.getTableName(), "STRING");
        }

        public HiveColumn getLoadDate() {
            return loadDate;
        }

        public void setLoadDate() {
            loadDate = new HiveColumn(PropertyHandler.getInstance().getValue("rdwLoadDateColumn"), "STRING");
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



