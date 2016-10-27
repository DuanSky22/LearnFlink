package com.duansky.learn.flink.util;

import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Created by DuanSky on 2016/10/26.
 */
public class Environments {

    private static int defaultEnv = 2;

    private static ExecutionEnvironment remoteEnvironment =
            ExecutionEnvironment.createRemoteEnvironment(Constract.host,Constract.port);

    private static ExecutionEnvironment localEnvironment =
            ExecutionEnvironment.getExecutionEnvironment();

    public static ExecutionEnvironment getRemoteExecutionEnvironment(){
        return remoteEnvironment;
    }

    public static ExecutionEnvironment getLocalExecutionEnvironment(){
        return localEnvironment;
    }

    public static ExecutionEnvironment getEnvironment(boolean local){
        if(local) return localEnvironment;
        else return remoteEnvironment;
    }


}
