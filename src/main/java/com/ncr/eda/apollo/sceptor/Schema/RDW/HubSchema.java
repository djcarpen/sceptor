package com.ncr.eda.apollo.sceptor.Schema.RDW;

import com.ncr.eda.apollo.sceptor.DataDictionary;
import com.ncr.eda.apollo.sceptor.HiveTable;
import com.ncr.eda.apollo.sceptor.PropertyHandler;
import com.ncr.eda.apollo.sceptor.Schema.Schema;

import java.util.ArrayList;
import java.util.List;

public class HubSchema implements Schema {


    private List<HiveTable> hubTables = new ArrayList<>();


    public List<HiveTable> getTables() {

        return hubTables;
    }

    public void generateTables(DataDictionary dataDictionary) {
        for (DataDictionary.Table t : dataDictionary.getTables()) {
            HiveTable hubTable = new HiveTable();
            hubTable.setSourceTableName(t.getTableName());
            hubTable.setDatabaseName(PropertyHandler.getInstance().getValue("rdwDatabaseNamePrefix") + t.getDatabaseName());
            hubTable.setTableName(PropertyHandler.getInstance().getValue("rdwHubTablePrefix") + t.getTableName());

            HiveTable.HiveColumn hubKey = new HiveTable.HiveColumn(PropertyHandler.getInstance().getValue("rdwHubKeyPrefix") + t.getTableName(), "STRING");
            hubKey.setHubKey(true);
            hubTable.addColumn(hubKey);

            List<HiveTable.HiveColumn> businessKeys;
            businessKeys = new ArrayList<>();

            for (DataDictionary.Table.Column c : t.getColumns()) {
                if (c.getIsBusinessKey()) {
                    HiveTable.HiveColumn businessKey = new HiveTable.HiveColumn();
                    if (c.getColumnName().equals("id")) {
                        businessKey.setColumnName(t.getTableName() + "_" + c.getColumnName());
                    } else {
                        businessKey.setColumnName(c.getColumnName());
                    }
                    businessKey.setSourceColumnName(c.getColumnName());
                    businessKey.setSourceTableName(t.getTableName());
                    businessKey.setBusinessKey(true);
                    businessKey.setDataType(c.getDataType());

                    businessKeys.add(businessKey);
                }

            }
            businessKeys.sort(new HiveTable.OrderByHiveColumnName());
            businessKeys.forEach(b -> hubTable.addColumn(b));

            HiveTable.HiveColumn loadDate = new HiveTable.HiveColumn(PropertyHandler.getInstance().getValue("rdwLoadDateColumn"), "STRING");
            loadDate.setLoadDate(true);
            hubTable.addColumn(loadDate);
            hubTable.addPartitionColumn(loadDate);

            hubTable.setStorageFormat(PropertyHandler.getInstance().getValue("rdwStoredAs"));
            hubTable.setSourceTable(t);

            hubTable.setStorageFormat(PropertyHandler.getInstance().getValue("rdwStoredAs"));
            hubTable.setHdfsLocation(PropertyHandler.getInstance().getValue("rdwHdfsBasePath") + "/rdw/" + t.getCommunityName() + "/public/" + t.getDatabaseName() + "/" + PropertyHandler.getInstance().getValue("rdwHubTablePrefix") + t.getTableName());
            hubTable.setSourceDatabaseName(PropertyHandler.getInstance().getValue("stagingDatabaseNamePrefix") + t.getCommunityName() + "_" + t.getAppCode() + "_" + t.getModuleCode() + "_" + t.getDatabaseName());
            hubTable.setHubKeyDelimiter(PropertyHandler.getInstance().getValue("rdwHubKeyDelimiter"));
            hubTables.add(hubTable);
        }
    }


}



