package com.ncr.eda.apollo.sceptor.Schema.RDW;

import com.ncr.eda.apollo.sceptor.DataDictionary;
import com.ncr.eda.apollo.sceptor.DataDictionary.Table;
import com.ncr.eda.apollo.sceptor.DataDictionary.Table.Column;
import com.ncr.eda.apollo.sceptor.HiveTable;
import com.ncr.eda.apollo.sceptor.PropertyHandler;
import com.ncr.eda.apollo.sceptor.Schema.Schema;

import java.util.ArrayList;
import java.util.List;


public class SatelliteSchema implements Schema {


    private List<SatelliteTable> satelliteTables;

    public List<SatelliteTable> getTables() {
        return satelliteTables;
    }


    public void generateTables(DataDictionary dataDictionary) {
        satelliteTables = new ArrayList<>();
        for (Table t : dataDictionary.getTables()) {
            SatelliteTable satelliteTable = new SatelliteTable();
            satelliteTable.setSourceTableName(t.getTableName());
            satelliteTable.setSourceDatabaseName(PropertyHandler.getInstance().getValue("stagingDatabaseNamePrefix") + t.getCommunityName() + "_" + t.getAppCode() + "_" + t.getModuleCode() + "_" + t.getDatabaseName());

            satelliteTable.setDatabaseName(PropertyHandler.getInstance().getValue("rdwDatabaseNamePrefix") + t.getDatabaseName());
            satelliteTable.setTableName(PropertyHandler.getInstance().getValue("rdwSatelliteTablePrefix") + t.getTableName());
            satelliteTable.setHubKey(t);
            satelliteTable.setSourceTable(t);
            //satelliteTable.setHubKeyDefinition(t);
            satelliteTable.setLoadDate();
            satelliteTable.setColumns(t);
            satelliteTable.addPartitionColumn(satelliteTable.getLoadDate());
            satelliteTable.setStorageFormat(PropertyHandler.getInstance().getValue("rdwStoredAs"));
            satelliteTable.setHdfsLocation(PropertyHandler.getInstance().getValue("rdwHdfsBasePath") + "/rdw/" + t.getCommunityName() + "/public/" + t.getDatabaseName() + "/" + PropertyHandler.getInstance().getValue("rdwSatelliteTablePrefix") + t.getTableName());
            satelliteTables.add(satelliteTable);
        }
    }


    public static class SatelliteTable extends HiveTable {
        private List<HiveColumn> dtlColumns;
    private List<HiveColumn> edlColumns;
        private HiveColumn satelliteKey;
        private List<HiveColumn> satelliteAttributes;
    private List<HiveColumn> satelliteColumns;
        private HiveColumn loadDate;
        private DataDictionary.Table sourceTable;

//        public void setSourceTable(Table sourceTable) {
//            this.sourceTable = sourceTable;
//        }
//
//        public Table getSourceTable() {
//            return sourceTable;
//        }

        private String hubKeyDefinition;

        public String getHubKeyDefinition() {
            return hubKeyDefinition;
        }

//        public void setHubKeyDefinition(Table table) {
//            HubKey hubKey = new HubKey();
//            hubKeyDefinition = hubKey.getHubKey(table);
//        }

        @Override
        public List<HiveColumn> getColumns() {
            return satelliteColumns;
        }

        public void setColumns(Table sourceTable) {
            dtlColumns = new ArrayList<>();
            edlColumns = new ArrayList<>();
            satelliteAttributes = new ArrayList<>();
            for (Column c : sourceTable.getColumns()) {
                if (!c.getIsBusinessKey() && !c.getIsSurrogateKey() && c.getForeignKeyColumn().isEmpty()) {
                    if (c.getColumnName().length() > 3) {
                        switch (c.getColumnName().substring(0, 4).toLowerCase()) {
                            case "edl_":
                                HiveColumn edlColumn = new HiveColumn(c.getColumnName(), c.getDataType());
                                edlColumn.setSourceColumnName(c.getColumnName());
                                edlColumn.setSourceTableName(sourceTable.getTableName());
                                edlColumns.add(edlColumn);
                                break;
                            case "dtl_":
                                HiveColumn dtlColumn = new HiveColumn(c.getColumnName(), c.getDataType());
                                dtlColumn.setSourceColumnName(c.getColumnName());
                                dtlColumn.setSourceTableName(sourceTable.getTableName());
                                dtlColumns.add(dtlColumn);
                                break;
                            default:
                                HiveColumn satelliteAttributeColumn = new HiveColumn(c.getColumnName(), c.getDataType());
                                satelliteAttributeColumn.setSourceColumnName(c.getColumnName());
                                satelliteAttributeColumn.setSourceTableName(sourceTable.getTableName());
                                satelliteAttributes.add(satelliteAttributeColumn);
                                break;
                        }
                    } else {
                        HiveColumn satelliteAttributeColumn = new HiveColumn(c.getColumnName(), c.getDataType());
                        satelliteAttributes.add(satelliteAttributeColumn);
                    }
                }
            }
            List<HiveColumn> columnList = new ArrayList<>();
            columnList.add(satelliteKey);
            columnList.add(loadDate);
            dtlColumns.sort(new HiveTable.OrderByHiveColumnName());
            columnList.addAll(dtlColumns);
            edlColumns.sort(new HiveTable.OrderByHiveColumnName());
            columnList.addAll(edlColumns);
            columnList.addAll(satelliteAttributes);
            for (HiveColumn c : columnList) {
                c.setColumnOrder(columnList.indexOf(c));
            }
            satelliteColumns = columnList;
        }

        public HiveColumn getHubKey() {
            return satelliteKey;
        }

        public void setHubKey(Table sourceTable) {
            satelliteKey = new HiveColumn(PropertyHandler.getInstance().getValue("rdwHubKeyPrefix") + sourceTable.getTableName(), "STRING"); // create hubTable key column
        }

        public HiveColumn getLoadDate() {
            return loadDate;
        }

        public void setLoadDate() {
            loadDate = new HiveColumn(PropertyHandler.getInstance().getValue("rdwLoadDateColumn"), "STRING");
        }

    }


}
