package com.github.djcarpen.sceptor.Schema.DataVault;

import java.util.*;

import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.Schema;
import com.github.djcarpen.sceptor.Schema.SourceSchema;

public class RawDataWarehouseSchema implements Schema {

    List<HiveTable> dataVaultTables;
    Hub hub = new Hub();
    Satellite satellite = new Satellite();
    Link link = new Link();

    public void generateTables (SourceSchema sourceSchema) {
        hub.generateHubs(sourceSchema);
        satellite.generateSatellites(sourceSchema);
        link.generateLinks(sourceSchema);
    }

    public List<HiveTable> getTables(){
        dataVaultTables = new ArrayList<>();
        dataVaultTables.addAll(hub.getHubTables());
        dataVaultTables.addAll(satellite.getSatellites());
        dataVaultTables.addAll(link.getLinks());
        return dataVaultTables;
    }

 }


