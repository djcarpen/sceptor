package com.github.djcarpen.sceptor.Schema.DataVault;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.DataDictionary.Table;
import com.github.djcarpen.sceptor.Schema.DataDictionary.Table.Column;
import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.Schema;

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
            satelliteTable.setDatabaseName(t.getDatabaseName());
            satelliteTable.setTableName("S_" + t.getTableName());
            satelliteTable.setHubKey(t);
            //satelliteTable.setHubKeyDefinition(t);
            satelliteTable.setLoadDate();
            satelliteTable.setColumns(t);
            satelliteTable.setDatabaseName(t.getDatabaseName());
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
            this.satelliteKey = satelliteKey;
            satelliteKey = new HiveColumn("hk_" + sourceTable.getTableName(), "STRING"); // create hubTable key column
        }

        public HiveColumn getLoadDate() {
            return loadDate;
        }

        public void setLoadDate() {
            this.loadDate = loadDate;
            loadDate = new HiveColumn("load_dt", "TIMESTAMP");
        }

    }


}
