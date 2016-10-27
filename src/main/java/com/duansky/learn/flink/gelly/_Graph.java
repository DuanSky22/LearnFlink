package com.duansky.learn.flink.gelly;

import com.duansky.learn.flink.gelly.util.Graphs;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;

/**
 * Created by DuanSky on 2016/10/26.
 */
public class _Graph {
    public static void main(String args[]){
        testGraph();
    }

    public static void testGraph(){
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        Graph graph = Graphs.createSimpleDirectedGraph(env);
        Graphs.printGraph(graph);
    }
}
