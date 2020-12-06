package com.hug.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCountScala {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    
        val infos: Array[String] = Array[String]("--host", "hadoop102", "--port", "7777")
        val parameterTool: ParameterTool = ParameterTool.fromArgs(infos)
        val host: String = parameterTool.get("host")
        val port: Int = parameterTool.getInt("port")
        
        val dataStream: DataStream[String] = env.socketTextStream(host,port)
        val resultDataStream: DataStream[(String, Int)] = dataStream
            .flatMap(_.split(" "))
            .map((_, 1))
            .keyBy(0)
            .sum(1)
        resultDataStream.print()
        
        env.execute()
    }
}
