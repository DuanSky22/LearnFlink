package com.duansky.learn.flink.gelly.lib;

import com.duansky.learn.flink.gelly.util.Graphs;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;

/**
 * Created by DuanSky on 2016/10/27.
 */
public class _TriangleListing {

    public static void main(String args[]){
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        ExecutionEnvironment env = com.duansky.learn.flink.util.Environments.getEnvironment(false);
        testDirectedTriangleListing(env);
        testUndirectedTriangleListing(env);
    }

    //TODO this code cannot run as jar file. WHT?
    public static void testDirectedTriangleListing(ExecutionEnvironment env){
        System.out.println("======directed triangle listing======");
        env.getConfig().disableSysoutLogging();
        Graph graph = Graphs.createSimpleDirectedGraph(env);
        DataSet<org.apache.flink.graph.library.clustering.directed.TriangleListing.Result<LongValue>> res;
        try {
            res = (DataSet<org.apache.flink.graph.library.clustering.directed.TriangleListing.Result<LongValue>>) graph.run(new org.apache.flink.graph.library.clustering.directed.TriangleListing<LongValue,LongValue,DoubleValue>());
            res.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testUndirectedTriangleListing(ExecutionEnvironment env){
        System.out.println("======undirected triangle listing======");
        env.getConfig().disableSysoutLogging();
        Graph graph = Graphs.createSimpleUndirectedGraph(env);
        DataSet<LongValue> res;
        try {
            res = (DataSet<LongValue>) graph.run(new org.apache.flink.graph.library.clustering.undirected.TriangleListing<LongValue,LongValue,DoubleValue>());
            res.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
