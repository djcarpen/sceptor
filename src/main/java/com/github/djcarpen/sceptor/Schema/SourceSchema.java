package com.github.djcarpen.sceptor.Schema;

import java.util.*;

public class SourceSchema {
    public List<HiveTable> tables = new ArrayList<>();

    public void addTable(HiveTable hiveTable) {
        tables.add(hiveTable);

    }
    public List<HiveTable> getTables() {
//        for (HiveTable t: tables) {
//            System.out.println("SourceSchema Tablename: " + t.getTableName());
//        }
        return tables;

    }


}
