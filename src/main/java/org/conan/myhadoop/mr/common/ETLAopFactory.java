package org.conan.myhadoop.mr.common;

/**
 * Created with IntelliJ IDEA.
 * User: liliangyang
 * Date: 14-6-24
 * Time: 上午8:53
 * GameETLv2
 */
public class ETLAopFactory {

    private static Object getClassInstance(String clzName) {
        Object obj = null;
        try {
            Class cls = Class.forName(clzName);
            obj = (Object) cls.newInstance();
        } catch (ClassNotFoundException cnfe) {
            System.out.println("ClassNotFoundException:" + cnfe.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return obj;
    }

    public static Object getAOPProxyedObject(String clzName) {
        Object proxy = null;
        ETLAopHandler handler = new ETLAopHandler();
        Object obj = getClassInstance(clzName);
        if (obj != null) {
            proxy = handler.bind(obj);
        } else {
            System.out.println("Can't get the proxyobj");
//throw
        }
        return proxy;
    }
}
