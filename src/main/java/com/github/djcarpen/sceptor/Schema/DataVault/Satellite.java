package com.github.djcarpen.sceptor.Schema.DataVault;

import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.SourceSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Satellite {

    private HiveTable satellite;
    private List<HiveTable> satellites = new ArrayList<>();
    private HiveTable.HiveColumn hubKey;
    private HiveTable.HiveColumn loadDate;
    private List<HiveTable.HiveColumn> dtlColumns;
    private List<HiveTable.HiveColumn> edlColumns;
    private List<HiveTable.HiveColumn> satelliteAttributes;
    private List<HiveTable.HiveColumn> satelliteColumns;

    public List<HiveTable> getSatellites() {
        return satellites;
    }

    public void generateSatellites(SourceSchema sourceSchema){
        for (HiveTable t : sourceSchema.getTables()) {
            generateSatelliteColumns(t);
            if (!satelliteColumns.isEmpty()) {
                satellite = new HiveTable();
                satellite.setDatabaseName(t.getDatabaseName());
                satellite.setTableName("S_" + t.getTableName());
                for (HiveTable.HiveColumn c : satelliteColumns) {
                    satellite.addColumn(c);
                }
                satellites.add(satellite);
            }
        }

    }

    public void generateSatelliteColumns(HiveTable sourceTable) {
        hubKey = new HiveTable.HiveColumn("hk_" + sourceTable.getTableName(), "STRING"); // create hub key column
        loadDate = new HiveTable.HiveColumn("load_dt","TIMESTAMP");
        dtlColumns = new ArrayList<>();
        edlColumns = new ArrayList<>();
        satelliteAttributes = new ArrayList<>();
        for (HiveTable.HiveColumn c : sourceTable.getColumns()) {
            if (c.getIsBusinessKey() == false && c.getIsSurrogateKey() == false && c.getForeignKeyColumn().isEmpty()) {
                if (c.getColumnName().length() > 3) {
                    if (c.getColumnName().substring(0, 4).toLowerCase().equals("edl_")) {
                        HiveTable.HiveColumn edlColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                        edlColumns.add(edlColumn);
                    } else if (c.getColumnName().substring(0, 4).toLowerCase().equals("dtl_")) {
                        HiveTable.HiveColumn dtlColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                        dtlColumns.add(dtlColumn);
                    } else {
                        HiveTable.HiveColumn satelliteAttributeColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                        satelliteAttributes.add(satelliteAttributeColumn);
                    }
                } else {
                    HiveTable.HiveColumn satelliteAttributeColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                    satelliteAttributes.add(satelliteAttributeColumn);
                }
            }
        }
        satelliteColumns = new ArrayList<>();
        satelliteColumns = sortSatelliteColumns(hubKey, edlColumns, dtlColumns, satelliteAttributes, loadDate);
    }

    public List<HiveTable.HiveColumn> sortSatelliteColumns(HiveTable.HiveColumn hubKey, List<HiveTable.HiveColumn> edlColumns, List<HiveTable.HiveColumn> dtlColumns, List<HiveTable.HiveColumn> satelliteAttributes, HiveTable.HiveColumn loadDate) {
        this.hubKey = hubKey;
        this.loadDate = loadDate;
        this.edlColumns = edlColumns;
        this.dtlColumns = dtlColumns;
        this.satelliteAttributes = satelliteAttributes;
        List<HiveTable.HiveColumn> columnList = new ArrayList<>();
        columnList.add(hubKey);
        columnList.add(loadDate);
        Collections.sort(dtlColumns, new HiveTable.OrderByHiveColumnName());
        columnList.addAll(dtlColumns);
        Collections.sort(edlColumns, new HiveTable.OrderByHiveColumnName());
        columnList.addAll(edlColumns);
        columnList.addAll(satelliteAttributes);
        for (HiveTable.HiveColumn c : columnList) {
            c.setColumnOrder(columnList.indexOf(c));
        }
        return columnList;
    }
}
