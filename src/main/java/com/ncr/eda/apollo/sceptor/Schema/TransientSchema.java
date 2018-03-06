package com.ncr.eda.apollo.sceptor.Schema;

import com.ncr.eda.apollo.sceptor.DataDictionary;
import com.ncr.eda.apollo.sceptor.HiveTable;
import com.ncr.eda.apollo.sceptor.PropertyHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TransientSchema implements Schema {

    private final List<HiveTable> transientTables = new ArrayList<>();

    public void generateTables(DataDictionary dataDictionary) {
        String transientPartitionColumnsProperty = PropertyHandler.getInstance().getValue("transientPartitionColumns");
        List<String> transientPartitionColumns;
        if (transientPartitionColumnsProperty.contains(",")) {
            transientPartitionColumns = new ArrayList<>(Arrays.asList(",".split(transientPartitionColumnsProperty)));
        } else {
            transientPartitionColumns = new ArrayList<>(Arrays.asList(transientPartitionColumnsProperty));
        }


        for (DataDictionary.Table t : dataDictionary.getTables()) {
            HiveTable transientTable = new HiveTable();
            transientTable.setSourceTableName(t.getTableName());
            transientTable.setDatabaseName(PropertyHandler.getInstance().getValue("transientDatabaseNamePrefix") + t.getCommunityName() + "_" + t.getAppCode() + "_" + t.getModuleCode() + "_" + t.getDatabaseName());
            transientTable.setSourceDatabaseName(PropertyHandler.getInstance().getValue("stagingDatabaseNamePrefix") + t.getCommunityName() + "_" + t.getAppCode() + "_" + t.getModuleCode() + "_" + t.getDatabaseName());
            transientTable.setTableName(t.getTableName());
            transientTable.setHdfsLocation(PropertyHandler.getInstance().getValue("transientHdfsBasePath") + "/" + t.getCommunityName() + "/public/trans/" + t.getAppCode() + "/" + t.getModuleCode() + "/" + t.getDatabaseName() + "/" + t.getTableName());
            transientTable.setStorageFormat(PropertyHandler.getInstance().getValue("transientStoredAs"));
            transientTable.setFieldTerminator(PropertyHandler.getInstance().getValue("transientFieldsTerminator"));
            for (DataDictionary.Table.Column c : t.getColumns()) {
                HiveTable.HiveColumn hiveColumn = new HiveTable.HiveColumn(c.getColumnName(), c.getDataType());
                hiveColumn.setSourceColumnName(c.getColumnName());
                hiveColumn.setSourceTableName(t.getTableName());
                transientTable.addColumn(hiveColumn);

            }
            transientPartitionColumns.forEach(parameter -> {
                transientTable.addPartitionColumn(new HiveTable.HiveColumn(parameter, "STRING"));
            });
            transientTable.addColumn(new HiveTable.HiveColumn("edl_end_time", "STRING"));
            transientTable.addColumn(new HiveTable.HiveColumn("edl_record_source", "STRING"));
            transientTables.add(transientTable);
        }
    }


    public List<HiveTable> getTables() {
        return transientTables;
    }
}
