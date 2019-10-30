import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import java.lang.System.currentTimeMillis
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

case class Item(id: Int, number: Long, scalingFactor: Double)
case class EnrichedItem(id: Int, total: Long)

object Threading {
  def main(args: Array[String]) {
    val parallelism = args(0).toInt
    val file = args(1)
    val scalingFactor = args(2).toDouble
    println(s"Running with parallelism ${parallelism}, file ${file}, scalingFactor ${scalingFactor}")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setMaxParallelism(parallelism) // Sets key-groups
    env.setParallelism(parallelism)
    val par = env.getParallelism
    val max = env.getMaxParallelism
    println("JOB PAR: " + par)
    println("MAX PAR: " + max)
    // JVM Warmup..
    1 to 5 foreach { _ => run(env, file, scalingFactor, parallelism) }
  }

  def run(env: StreamExecutionEnvironment, path: String, scalingFactor: Double, parallelism: Int) = {
    val stream = env.addSource(Sources.itemSource(path, scalingFactor))
      .keyBy(_.id)
      .map(item => {
        def fibonacci(n: Long): Long  = {
          if (n == 0) {
            println("Fib not supposed to take 0");
            System.exit(1)
          } else if (n == 1) {
            return 1
          }

          var sum = 0
          var last = 0
          var curr = 1
          for( _ <- 1 until n.toInt) {
            sum = last + curr
            last = curr
            curr = sum
          }
          sum
        }

        val fib = math.ceil((item.number.toDouble / 100.0)) * item.scalingFactor
        val fibSum = fibonacci(fib.toLong)
        new EnrichedItem(item.id, fibSum)
      }).setParallelism(parallelism)
      .addSink(new ThroughputSink[EnrichedItem](100000)).setParallelism(1)

    println(env.getExecutionPlan)
    val res = env.execute()
    println("The job took " + res.getNetRuntime() + " to execute");
  }
}
