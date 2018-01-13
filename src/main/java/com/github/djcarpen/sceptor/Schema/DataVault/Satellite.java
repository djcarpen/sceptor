package com.github.djcarpen.sceptor.Schema.DataVault;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.DataDictionary.Table;
import com.github.djcarpen.sceptor.Schema.DataDictionary.Table.Column;
import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.HiveTable.HiveColumn;

import java.util.ArrayList;
import java.util.List;

import static com.github.djcarpen.sceptor.Schema.DataVault.EntityType.SATELLITE;

public class Satellite implements DataVaultSchema {

    public static final EntityType entityType = SATELLITE;
    private final List<HiveTable> satellites = new ArrayList<>();
    private HiveColumn hubKey;
    private HiveColumn loadDate;
    private List<HiveColumn> dtlColumns;
    private List<HiveColumn> edlColumns;
    private List<HiveColumn> satelliteAttributes;
    private List<HiveColumn> satelliteColumns;

    public static EntityType getEntityType() {
        return entityType;
    }

    public List<HiveTable> getTables() {
        return satellites;
    }

    public void generateTables(DataDictionary dataDictionary) {
        for (Table t : dataDictionary.getTables()) {
            generateColumns(t);
            if (!satelliteColumns.isEmpty()) {
                HiveTable satellite = new HiveTable();
                satellite.setDatabaseName(t.getDatabaseName());
                satellite.setTableName("S_" + t.getTableName());
                for (HiveColumn c : satelliteColumns) {
                    satellite.addColumn(c);
                }
                satellites.add(satellite);
            }
        }

    }

    public void generateColumns(Table sourceTable) {
        hubKey = new HiveColumn("hk_" + sourceTable.getTableName(), "STRING"); // create hub key column
        loadDate = new HiveColumn("load_dt", "TIMESTAMP");
        dtlColumns = new ArrayList<>();
        edlColumns = new ArrayList<>();
        satelliteAttributes = new ArrayList<>();
        for (Column c : sourceTable.getColumns()) {
            if (!c.getIsBusinessKey() && !c.getIsSurrogateKey() && c.getForeignKeyColumn().isEmpty()) {
                if (c.getColumnName().length() > 3) {
                    switch (c.getColumnName().substring(0, 4).toLowerCase()) {
                        case "edl_":
                            HiveColumn edlColumn = new HiveColumn(c.getColumnName(), c.getDataType());
                            edlColumns.add(edlColumn);
                            break;
                        case "dtl_":
                            HiveColumn dtlColumn = new HiveColumn(c.getColumnName(), c.getDataType());
                            dtlColumns.add(dtlColumn);
                            break;
                        default:
                            HiveColumn satelliteAttributeColumn = new HiveColumn(c.getColumnName(), c.getDataType());
                            satelliteAttributes.add(satelliteAttributeColumn);
                            break;
                    }
                } else {
                    HiveColumn satelliteAttributeColumn = new HiveColumn(c.getColumnName(), c.getDataType());
                    satelliteAttributes.add(satelliteAttributeColumn);
                }
            }
        }
        satelliteColumns = new ArrayList<>();
        satelliteColumns = sortColumns();
    }

    public List<HiveColumn> sortColumns() {
        List<HiveColumn> columnList = new ArrayList<>();
        columnList.add(hubKey);
        columnList.add(loadDate);
        dtlColumns.sort(new HiveTable.OrderByHiveColumnName());
        columnList.addAll(dtlColumns);
        edlColumns.sort(new HiveTable.OrderByHiveColumnName());
        columnList.addAll(edlColumns);
        columnList.addAll(satelliteAttributes);
        for (HiveColumn c : columnList) {
            c.setColumnOrder(columnList.indexOf(c));
        }
        return columnList;
    }
}
