import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import java.lang.System.currentTimeMillis
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

object NoChaining {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.createLocalEnvironment()
    1 to 20 foreach { _ => run(env) }
    run(env)
  }

  def run(env: StreamExecutionEnvironment) = {
    val stream = env.addSource(new SourceFunction[Int]() {
       override def run(ctx: SourceContext[Int]) = {
         var counter: Long = 0
         while (counter < 10000000) {
           ctx.collect(10)
           counter += 1
         }
       }
       override def cancel(): Unit =  {}
    })
      .map(num => num + 50).setParallelism(1)
      .addSink(new ThroughputSink[Int](100000)).setParallelism(1)

    println(env.getExecutionPlan)
    val res = env.execute()
  }
}
