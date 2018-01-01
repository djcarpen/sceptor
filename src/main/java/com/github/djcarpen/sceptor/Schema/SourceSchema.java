package com.github.djcarpen.sceptor.Schema;

import java.util.ArrayList;
import java.util.List;

public class SourceSchema {
    private List<HiveTable> tables = new ArrayList<>();

    public void addTable(HiveTable hiveTable) {
        tables.add(hiveTable);

    }
    public List<HiveTable> getTables() {
        return tables;

    }


}
