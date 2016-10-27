package com.duansky.learn.flink.gelly.lib;

import com.duansky.learn.flink.gelly.util.Graphs;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;

/**
 * Created by DuanSky on 2016/10/24.
 */
public class _TriangleCount {

    public static void main(String args[]) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        testDirectedTriangleCount(env);
        testUndirectedTriangleCount(env);
    }

    /**
     * test the undirected triangle count.
     */
    public static void testUndirectedTriangleCount(ExecutionEnvironment env){
        org.apache.flink.graph.library.clustering.undirected.TriangleCount triangleCount
                = new org.apache.flink.graph.library.clustering.undirected.TriangleCount();

        Graph<LongValue,LongValue,DoubleValue> graph = Graphs.createSimpleUndirectedGraph(env);
        try {
            triangleCount = (org.apache.flink.graph.library.clustering.undirected.TriangleCount) graph.run(triangleCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(triangleCount.getResult());
    }

    /**
     * test the directed triangle count.
     */
    public static void testDirectedTriangleCount(ExecutionEnvironment env){
        org.apache.flink.graph.library.clustering.directed.TriangleCount triangleCount
                = new org.apache.flink.graph.library.clustering.directed.TriangleCount();

        Graph<LongValue,LongValue,DoubleValue> graph = Graphs.createSimpleDirectedGraph(env);
        try {
            triangleCount = (org.apache.flink.graph.library.clustering.directed.TriangleCount) graph.run(triangleCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(triangleCount.getResult());
    }
}
