package com.github.djcarpen.sceptor.Schema;

import java.util.ArrayList;
import java.util.List;

public class RawSchema implements Schema {

    private List<HiveTable> rawTables = new ArrayList<>();
    private List<HiveTable.HiveColumn> partitionColumns;
    private List<HiveTable.HiveColumn> rawColumns;



    public void generateTables(SourceSchema sourceSchema){
        for (HiveTable t : sourceSchema.getTables()) {
            HiveTable rawTable = new HiveTable();
            //List<HiveTable.HiveColumn> columnList = new ArrayList<>();
            rawTable.setDatabaseName(t.getCommunityName() + "_" + t.getAppCode() + "_" + t.getModuleCode() + "_" + t.getDatabaseName());
            rawTable.setTableName(t.getTableName());
            rawTable.setHdfsLocation(t.getHdfsLocation());
            //partitionColumns = new ArrayList<>();
            //rawColumns = new ArrayList<>();
            for (HiveTable.HiveColumn c : t.getColumns()) {
                if (c.getColumnName().equals("edl_ingest_channel")) {
                    //System.out.println("found edl_ingest_channel" + c.getColumnName());
                    HiveTable.HiveColumn partitionColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                    partitionColumn.setColumnOrder(0);
                    //partitionColumns.add(partitionColumn);
                    rawTable.addPartitionColumn(partitionColumn);
                } else if (c.getColumnName().equals("edl_ingest_time")) {
                    HiveTable.HiveColumn partitionColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                    //System.out.println("found edl_ingest_time" + c.getColumnName());
                    partitionColumn.setColumnOrder(1);
                    //partitionColumns.add(partitionColumn);
                    rawTable.addPartitionColumn(partitionColumn);
                } else {
                    HiveTable.HiveColumn rawColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                    //rawColumns.add(rawColumn);
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
