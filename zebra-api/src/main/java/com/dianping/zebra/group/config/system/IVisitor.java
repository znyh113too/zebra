package com.dianping.zebra.group.config.system;

import com.dianping.zebra.group.config.system.entity.DataCenter;
import com.dianping.zebra.group.config.system.entity.SqlFlowControl;
import com.dianping.zebra.group.config.system.entity.SystemConfig;

/**
 * 抽象访问者
 */
public interface IVisitor {

    /**
     * 访问数据中心
     */
    void visitDataCenter(DataCenter dataCenter);

    /**
     * 访问sql flow control
     */
    void visitSqlFlowControl(SqlFlowControl sqlFlowControl);

    /**
     * 访问system config
     */
    void visitSystemConfig(SystemConfig systemConfig);
}
