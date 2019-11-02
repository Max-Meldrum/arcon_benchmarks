package threading

import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import java.lang.System.currentTimeMillis
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

case class Item(id: Int, number: Long, scalingFactor: Int)
case class EnrichedItem(id: Int, root: Double)

object Threading {
  def main(args: Array[String]) {
    val parallelism = args(0).toInt
    val file = args(1)
    val scalingFactor = args(2).toInt
    val logThroughput = {
      if (args.length > 3) {
        true 
      } else {
        false
      }
    }

    println(s"Running with parallelism ${parallelism}, file ${file}, scalingFactor ${scalingFactor}, logThroughput ${logThroughput}")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setMaxParallelism(parallelism) // Sets key-groups
    env.setParallelism(parallelism)
    val par = env.getParallelism
    val max = env.getMaxParallelism
    println("JOB PAR: " + par)
    println("MAX PAR: " + max)
    // JVM Warmup..
    1 to 5 foreach { _ => run(env, file, scalingFactor, parallelism, logThroughput) }
  }

  def run(env: StreamExecutionEnvironment, path: String, scalingFactor: Int, parallelism: Int, logThroughput: Boolean) = {
    val stream = env.addSource(Sources.itemSource(path, scalingFactor))
      .keyBy(_.id)
      .map(item => {
        val root = newtonSqrt(item.number, item.scalingFactor)
        new EnrichedItem(item.id, root)
      }).setParallelism(parallelism)
      .addSink(new ThroughputSink[EnrichedItem](100000, logThroughput)).setParallelism(1)


    println(env.getExecutionPlan)
    val res = env.execute()
    println("The job took " + res.getNetRuntime() + " to execute");
  }

  def newtonSqrt(square: Long, iters: Int): Double = {
    val target = square.toDouble
    var currentGuess = target

    for( _ <- 0 until iters) {
      val numerator = currentGuess * currentGuess - target
      val denom = currentGuess * 2.0
      currentGuess = currentGuess - (numerator/denom)
    }
    currentGuess
  }
}
