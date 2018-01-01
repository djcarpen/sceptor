package com.github.djcarpen.sceptor.Schema;

import com.github.djcarpen.sceptor.Schema.Schema;

import java.util.ArrayList;
import java.util.List;

public class RawSchema implements Schema {

    public void generateTables(SourceSchema sourceSchema){

    }

    public List<HiveTable> getTables() {
        List<HiveTable> x = new ArrayList<>();
        return x;
    }
}
