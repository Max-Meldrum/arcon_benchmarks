import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import java.lang.System.currentTimeMillis
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

case class Item(id: Int, sales: Seq[Long])
case class EnrichedItem(id: Int, totalPrice: Long)

object Threading {
  def main(args: Array[String]) {
    val parallelism = args(0).toInt
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setMaxParallelism(parallelism) // Sets key-groups
    env.setParallelism(parallelism)
    val par = env.getParallelism
    val max = env.getMaxParallelism
    println("JOB PAR: " + par)
    println("MAX PAR: " + max)
    val items = read_data(args(1))
    // JVM Warmup..
    1 to 5 foreach { _ => run(env, items, parallelism) }
  }

  def run(env: StreamExecutionEnvironment, items: List[Item], parallelism: Int) = {
    val stream: DataStream[Item] = env.fromCollection(items)

    stream.keyBy(_.id)
      .map(item => {
        val totalPrice = item.sales.reduce(_ + _)
        new EnrichedItem(item.id, totalPrice)
      }).setParallelism(parallelism)
      .addSink(new ThroughputSink[EnrichedItem](100000)).setParallelism(1)

    println(env.getExecutionPlan)
    val res = env.execute()
    println("The job took " + res.getNetRuntime() + " to execute");
  }

   def read_data(path: String): List[Item] = {
    val source = scala.io.Source.fromFile(path)
    val lines = source.getLines
    lines.map(line => {
      val splitted = line.split(" ")
      val id = splitted(0).toInt
      val sales = splitted(1).split(",").map(_.toLong)
      Item(id, sales)
    }).toList
  }
}
