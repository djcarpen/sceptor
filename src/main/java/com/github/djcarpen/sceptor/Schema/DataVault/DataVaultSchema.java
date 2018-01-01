package com.github.djcarpen.sceptor.Schema.DataVault;

import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.Schema;
import com.github.djcarpen.sceptor.Schema.SourceSchema;

import java.util.List;

public interface DataVaultSchema extends Schema {
    void generateTables(SourceSchema sourceSchema);

    void generateColumns(HiveTable sourceTable);

    List<HiveTable.HiveColumn> sortColumns();
}
