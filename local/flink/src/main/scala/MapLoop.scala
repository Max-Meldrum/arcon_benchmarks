import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import java.lang.System.currentTimeMillis
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

object MapLoop {
  def main(args: Array[String]) {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // JVM Warmup
    1 to 5 foreach { _ => run(env) }
  }

  def run(env: StreamExecutionEnvironment) = {
    val stream = env.addSource(new SourceFunction[Int]() {
       override def run(ctx: SourceContext[Int]) = {
         var counter: Long = 0
         val limit: Long = 10000000
         while (counter < limit) {
           ctx.collect(10)
           counter += 1
         }
       }
       override def cancel(): Unit =  {}
    })
      .map(num => {
        var c = 0
        for (i <- 0 to 49) {
          c += 1
        }
        c
      }).setParallelism(1)
      .addSink(new ThroughputSink[Int](100000)).setParallelism(1)

    println(env.getExecutionPlan)
    val res = env.execute()
  }
}
