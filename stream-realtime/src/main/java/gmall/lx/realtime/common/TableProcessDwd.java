package gmall.lx.realtime.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @className: TableProcessDwd
 * @author: lx
 * @date: 2024/12/26 10:00
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableProcessDwd implements Serializable {
     String sourceTable;
     String sourceType;
     String sinkTable;
     String sinkColumns;

}
