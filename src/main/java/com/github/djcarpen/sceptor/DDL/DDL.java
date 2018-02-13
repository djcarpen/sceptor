package com.github.djcarpen.sceptor.DDL;

import com.github.djcarpen.sceptor.Schema.Schema;

import java.util.List;
import java.util.Map;

public interface DDL {
    Map getDDLs(List<Schema> schemas);
}
