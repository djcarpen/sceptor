package com.github.djcarpen.sceptor.Schema;

import java.util.List;

public interface Schema {

    void generateTables(DataDictionary dataDictionary);

    List<HiveTable> getTables();

}
