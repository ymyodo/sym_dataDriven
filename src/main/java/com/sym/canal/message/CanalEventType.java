package com.sym.canal.message;

import java.io.Serializable;

/**
 * 只关心表的增删改
 *
 * @author shenyanming
 * @date 2020/7/26 12:52.
 */
public enum CanalEventType implements Serializable {
    INSERT, UPDATE, DELETE;
}
