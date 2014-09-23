package org.conan.myhadoop.mr.common;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * Created with IntelliJ IDEA.
 * User: liliangyang
 * Date: 14-6-24
 * Time: 上午8:54
 * GameETLv2
 */
public class ETLAopHandler implements InvocationHandler {
    private Object proxyObj;

    public Object bind(Object obj) {
        this.proxyObj = obj;
        return Proxy.newProxyInstance(obj.getClass().getClassLoader(), obj.getClass().getInterfaces(), this);
    }


    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Object result = null;
        try {
//请在这里插入代码，在方法前调用
            //System.out.printf("ETL:" + method.getName());
            result = method.invoke(proxyObj, args); //原方法
//请在这里插入代码，方法后调用
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
