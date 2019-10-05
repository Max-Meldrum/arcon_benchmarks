import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import java.lang.System.currentTimeMillis
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

object InvocationFusion {
  def main(args: Array[String]) {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // JVM Warmup
    1 to 5 foreach { _ => run(env) }
  }

  def run(env: StreamExecutionEnvironment) = {
    val stream = env.addSource(Sources.fusionSource())
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
    println("The job took " + res.getNetRuntime() + " to execute");
  }
}
