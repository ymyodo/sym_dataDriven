package com.sym.canal.message;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 封装 canal 解析出的表信息
 *
 * @author shenyanming
 * @date 2020/7/26 12:50.
 */
@Data
public class CanalMessage implements Serializable {
    private static final long serialVersionUID = -6996185371544370458L;

    /**
     * 事件类型
     */
    private CanalEventType eventType;

    /**
     * 表名称
     */
    private String tableName;

    /**
     * 修改前的表字段数据
     */
    private List<Map<String, Object>> rowDataBeforeList = new ArrayList<>();

    /**
     * 修改后的表字段数据
     */
    private List<Map<String, Object>> rowDataAfterList = new ArrayList<>();


    public void parseCanalEventType(CanalEntry.EventType entryType) {
        if (Objects.nonNull(entryType)) {
            switch (entryType) {
                case INSERT:
                    this.eventType = CanalEventType.INSERT;
                    break;
                case UPDATE:
                    this.eventType = CanalEventType.UPDATE;
                    break;
                case DELETE:
                    this.eventType = CanalEventType.DELETE;
                    break;
                default:
                    // do nothing
                    break;
            }
        }
    }
}

