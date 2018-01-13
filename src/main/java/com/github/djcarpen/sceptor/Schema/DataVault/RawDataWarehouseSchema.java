package com.github.djcarpen.sceptor.Schema.DataVault;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.Schema;

import java.util.ArrayList;
import java.util.List;

public class RawDataWarehouseSchema implements Schema {


    private final DataVaultSchema satellite = new Satellite();
    private final DataVaultSchema link = new Link();
    private DataVaultSchema hub = new Hub();

    public List<HiveTable> getHubs() {
        return hub.getTables();
    }

    public List<HiveTable> getLinks() {
        return link.getTables();
    }

    public List<HiveTable> getSatellites() {
        return satellite.getTables();
    }
    public void generateTables(DataDictionary dataDictionary) {
        hub.generateTables(dataDictionary);
        satellite.generateTables(dataDictionary);
        link.generateTables(dataDictionary);
    }


    public List<HiveTable> getTables() {
        List<HiveTable> dataVaultTables = new ArrayList<>();
        dataVaultTables.addAll(hub.getTables());
        dataVaultTables.addAll(satellite.getTables());
        dataVaultTables.addAll(link.getTables());
        return dataVaultTables;
    }


}




