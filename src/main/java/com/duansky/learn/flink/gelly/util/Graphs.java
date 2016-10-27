package com.duansky.learn.flink.gelly.util;

import com.duansky.learn.flink.gelly.data.SimpleGraphData;
import com.duansky.learn.flink.gelly.data.SingleSourceShortestPathsData;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

/**
 * Created by DuanSky on 2016/10/24.
 */
public class Graphs {

    /**
     * create a directed graph use {@link SimpleGraphData}.
     * @return a directed graph
     */
    public static Graph<LongValue,LongValue,DoubleValue> createSimpleDirectedGraph(ExecutionEnvironment env){
        return SimpleGraphData.getGraph(env);
    }

    /**
     * create an undirected graph use {@link SimpleGraphData}.
     * @return an undirected graph
     */
    public static Graph<LongValue,LongValue,DoubleValue> createSimpleUndirectedGraph(ExecutionEnvironment env){
        return createSimpleDirectedGraph(env).getUndirected();
    }

    public static void printGraph(Graph graph){
        try {
            System.out.println("=================graph======================");
            System.out.println("  number of edges:"+graph.numberOfEdges());
            System.out.println("number of verties:"+graph.numberOfVertices());
            System.out.println("       verties id:");
            graph.getVertexIds().print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
