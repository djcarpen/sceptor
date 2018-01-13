package com.github.djcarpen.sceptor.Schema.DataVault;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.Schema;

import java.util.List;

public interface DataVaultSchema extends Schema {
    void generateTables(DataDictionary dataDictionary);

    void generateColumns(DataDictionary.Table sourceTable);



    List<HiveTable.HiveColumn> sortColumns();
}
