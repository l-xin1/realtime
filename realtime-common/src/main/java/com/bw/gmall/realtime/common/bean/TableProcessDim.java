package com.bw.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDim {
      String sourceTable;
     String sinkTable;
     String sinkFamily;
     String sinkColumns;
     String sinkRowKey;
     String op;
}
