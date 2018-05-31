package com.dianping.zebra.group.config.system;

/**
 * 抽象元素类
 * 
 * @param <T>
 */
public interface IEntity<T> {

    /**
     * 子类自己判断访问什么
     */
    void accept(IVisitor visitor);

   public void mergeAttributes(T other);

}
