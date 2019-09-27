import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import java.lang.System.currentTimeMillis
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

object ChainingMuch {
  def main(args: Array[String]) {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // JVM Warmup
    1 to 5 foreach { _ => run(env) }
  }

  def run(env: StreamExecutionEnvironment) = {
    val stream = env.addSource(new SourceFunction[Int]() {
       override def run(ctx: SourceContext[Int]) = {
         var counter: Long = 0
         val r = new scala.util.Random
         val limit: Long = 10000000
         while (counter < limit) {
           ctx.collect(10)
           counter += 1
         }
       }
       override def cancel(): Unit =  {}
    })
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .map(num => num + 1).setParallelism(1)
      .addSink(new ThroughputSink[Int](100000)).setParallelism(1)

    println(env.getExecutionPlan)
    val res = env.execute()
  }
}
