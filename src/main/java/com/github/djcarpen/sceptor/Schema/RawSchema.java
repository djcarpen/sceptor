package com.github.djcarpen.sceptor.Schema;

import java.util.ArrayList;
import java.util.List;

public class RawSchema implements Schema {

    private List<HiveTable> rawTables = new ArrayList<>();


    public void generateTables(DataDictionary dataDictionary) {
        for (HiveTable t : dataDictionary.getTables()) {
            HiveTable rawTable = new HiveTable();
            rawTable.setDatabaseName(t.getCommunityName() + "_" + t.getAppCode() + "_" + t.getModuleCode() + "_" + t.getDatabaseName());
            rawTable.setTableName(t.getTableName());
            rawTable.setHdfsLocation(t.getHdfsLocation());
            for (HiveTable.HiveColumn c : t.getColumns()) {
                if (c.getColumnName().equals("edl_ingest_channel")) {
                    HiveTable.HiveColumn partitionColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                    partitionColumn.setColumnOrder(0);
                    rawTable.addPartitionColumn(partitionColumn);
                } else if (c.getColumnName().equals("edl_ingest_time")) {
                    HiveTable.HiveColumn partitionColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                    partitionColumn.setColumnOrder(1);
                    rawTable.addPartitionColumn(partitionColumn);
                } else {
                    HiveTable.HiveColumn rawColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                    rawTable.addColumn(rawColumn);
                }
            }
            rawTables.add(rawTable);
        }
    }

    public List<HiveTable> getTables() {
        return rawTables;
    }

}
