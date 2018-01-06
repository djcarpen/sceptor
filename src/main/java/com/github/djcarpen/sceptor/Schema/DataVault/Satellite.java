package com.github.djcarpen.sceptor.Schema.DataVault;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.HiveTable;

import java.util.ArrayList;
import java.util.List;

public class Satellite implements DataVaultSchema {

    private List<HiveTable> satellites = new ArrayList<>();
    private HiveTable.HiveColumn hubKey;
    private HiveTable.HiveColumn loadDate;
    private List<HiveTable.HiveColumn> dtlColumns;
    private List<HiveTable.HiveColumn> edlColumns;
    private List<HiveTable.HiveColumn> satelliteAttributes;
    private List<HiveTable.HiveColumn> satelliteColumns;

    public List<HiveTable> getTables() {
        return satellites;

    }

    public void generateTables(DataDictionary dataDictionary) {
        for (HiveTable t : dataDictionary.getTables()) {
            generateColumns(t);
            if (!satelliteColumns.isEmpty()) {
                HiveTable satellite = new HiveTable();
                satellite.setDatabaseName(t.getDatabaseName());
                satellite.setTableName("S_" + t.getTableName());
                for (HiveTable.HiveColumn c : satelliteColumns) {
                    satellite.addColumn(c);
                }
                satellites.add(satellite);
            }
        }

    }

    public void generateColumns(HiveTable sourceTable) {
        hubKey = new HiveTable.HiveColumn("hk_" + sourceTable.getTableName(), "STRING"); // create hub key column
        loadDate = new HiveTable.HiveColumn("load_dt","TIMESTAMP");
        dtlColumns = new ArrayList<>();
        edlColumns = new ArrayList<>();
        satelliteAttributes = new ArrayList<>();
        for (HiveTable.HiveColumn c : sourceTable.getColumns()) {
            if (!c.getIsBusinessKey() && !c.getIsSurrogateKey() && c.getForeignKeyColumn().isEmpty()) {
                if (c.getColumnName().length() > 3) {
                    switch (c.getColumnName().substring(0, 4).toLowerCase()) {
                        case "edl_":
                            HiveTable.HiveColumn edlColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                            edlColumns.add(edlColumn);
                            break;
                        case "dtl_":
                            HiveTable.HiveColumn dtlColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                            dtlColumns.add(dtlColumn);
                            break;
                        default:
                            HiveTable.HiveColumn satelliteAttributeColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                            satelliteAttributes.add(satelliteAttributeColumn);
                            break;
                    }
                } else {
                    HiveTable.HiveColumn satelliteAttributeColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                    satelliteAttributes.add(satelliteAttributeColumn);
                }
            }
        }
        satelliteColumns = new ArrayList<>();
        satelliteColumns = sortColumns();
    }

    public List<HiveTable.HiveColumn> sortColumns() {
        List<HiveTable.HiveColumn> columnList = new ArrayList<>();
        columnList.add(hubKey);
        columnList.add(loadDate);
        dtlColumns.sort(new HiveTable.OrderByHiveColumnName());
        columnList.addAll(dtlColumns);
        edlColumns.sort(new HiveTable.OrderByHiveColumnName());
        columnList.addAll(edlColumns);
        columnList.addAll(satelliteAttributes);
        for (HiveTable.HiveColumn c : columnList) {
            c.setColumnOrder(columnList.indexOf(c));
        }
        return columnList;
    }
}
