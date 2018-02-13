package com.github.djcarpen.sceptor.Schema;

import java.util.ArrayList;
import java.util.List;

public class RawSchema implements Schema {


    private final List<HiveTable> rawTables = new ArrayList<>();

    public void generateTables(DataDictionary dataDictionary) {
        for (DataDictionary.Table t : dataDictionary.getTables()) {
            HiveTable rawTable = new HiveTable();
            rawTable.setSourceTableName(t.getTableName());
            rawTable.setDatabaseName(t.getCommunityName() + "_" + t.getAppCode() + "_" + t.getModuleCode() + "_" + t.getDatabaseName());
            rawTable.setTableName(t.getTableName());
            rawTable.setHdfsLocation(t.getHdfsLocation());
            for (DataDictionary.Table.Column c : t.getColumns()) {

                switch (c.getColumnName()) {
                    case "edl_ingest_channel": {
                        HiveTable.HiveColumn partitionColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                        partitionColumn.setSourceColumnName(c.getColumnName());
                        partitionColumn.setSourceTableName(t.getTableName());
                        partitionColumn.setColumnOrder(0);
                        rawTable.addPartitionColumn(partitionColumn);
                        break;
                    }
                    case "edl_ingest_time": {
                        HiveTable.HiveColumn partitionColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                        partitionColumn.setSourceColumnName(c.getColumnName());
                        partitionColumn.setSourceTableName(t.getTableName());
                        partitionColumn.setColumnOrder(1);
                        rawTable.addPartitionColumn(partitionColumn);
                        break;
                    }
                    default:
                        HiveTable.HiveColumn rawColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                        rawColumn.setSourceColumnName(c.getColumnName());
                        rawColumn.setSourceTableName(t.getTableName());
                        rawTable.addColumn(rawColumn);
                        break;
                }
            }
            rawTables.add(rawTable);
        }
    }

    public List<HiveTable> getTables() {
        return rawTables;
    }

}
