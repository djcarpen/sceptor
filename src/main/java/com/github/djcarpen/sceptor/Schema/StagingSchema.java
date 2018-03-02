package com.github.djcarpen.sceptor.Schema;

import com.github.djcarpen.sceptor.DataDictionary;
import com.github.djcarpen.sceptor.HiveTable;
import com.github.djcarpen.sceptor.PropertyHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StagingSchema implements Schema {

    private final List<HiveTable> stagingTables = new ArrayList<>();

    public void generateTables(DataDictionary dataDictionary) {
        List<String> stagingPartitionColumns = new ArrayList<>(Arrays.asList(PropertyHandler.getInstance().getValue("stagingPartitionColumns").split(",")));
        for (DataDictionary.Table t : dataDictionary.getTables()) {
            HiveTable stagingTable = new HiveTable();
            stagingTable.setSourceTableName(t.getTableName());
            stagingTable.setDatabaseName(PropertyHandler.getInstance().getValue("stagingDatabaseNamePrefix") + t.getCommunityName() + "_" + t.getAppCode() + "_" + t.getModuleCode() + "_" + t.getDatabaseName());
            stagingTable.setTableName(t.getTableName());
            stagingTable.setHdfsLocation(PropertyHandler.getInstance().getValue("stagingHdfsBasePath") + "/" + t.getCommunityName() + "/public/stg/" + t.getAppCode() + "/" + t.getModuleCode() + "/" + t.getDatabaseName() + "/" + t.getTableName());
            stagingTable.setStorageFormat(PropertyHandler.getInstance().getValue("stagingStoredAs"));
            stagingTable.setFieldTerminator(PropertyHandler.getInstance().getValue("stagingFieldsTerminator"));
            for (DataDictionary.Table.Column c : t.getColumns()) {
                HiveTable.HiveColumn hiveColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                hiveColumn.setSourceColumnName(c.getColumnName());
                hiveColumn.setSourceTableName(t.getTableName());
                stagingPartitionColumns.forEach(parameter -> {
                    if (c.getColumnName().equals(parameter)) {
                        hiveColumn.setColumnOrder(parameter.indexOf(c.getColumnName()));
                        stagingTable.addPartitionColumn(hiveColumn);
                    }
                });
                if (!stagingTable.getPartitionColumns().contains(hiveColumn)) {
                    stagingTable.addColumn(hiveColumn);
                }

            }
            stagingTables.add(stagingTable);
        }
    }


    public List<HiveTable> getTables() {
        return stagingTables;
    }

}
