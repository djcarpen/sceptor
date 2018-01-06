package com.github.djcarpen.sceptor.Schema.DataVault;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.Schema;

import java.util.ArrayList;
import java.util.List;

public class RawDataWarehouseSchema implements Schema {

    private List<HiveTable> dataVaultTables;


    private DataVaultSchema hub = new Hub();
    private DataVaultSchema satellite = new Satellite();
    private DataVaultSchema link = new Link();


    public void generateTables(DataDictionary dataDictionary) {
        hub.generateTables(dataDictionary);
        satellite.generateTables(dataDictionary);
        link.generateTables(dataDictionary);
    }

    public List<HiveTable> getTables() {
        dataVaultTables = new ArrayList<>();
        dataVaultTables.addAll(hub.getTables());
        dataVaultTables.addAll(satellite.getTables());
        dataVaultTables.addAll(link.getTables());
        return dataVaultTables;
    }

}




