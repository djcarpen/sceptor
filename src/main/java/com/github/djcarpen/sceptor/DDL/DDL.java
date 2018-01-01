package com.github.djcarpen.sceptor.DDL;

import com.github.djcarpen.sceptor.Schema.Schema;
import com.github.djcarpen.sceptor.Zone;

public interface DDL {

    void writeDDL(Zone zone, Schema schema, String filePath);
}
