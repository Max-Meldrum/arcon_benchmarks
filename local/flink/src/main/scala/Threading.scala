import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import java.lang.System.currentTimeMillis
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

case class Item(id: Int, price: Long)

object Threading {
  def main(args: Array[String]) {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setMaxParallelism(2) // Set Key-groups to 2
    val par = env.getParallelism
    val max = env.getMaxParallelism
    println("JOB PAR: " + par)
    println("MAX PAR: " + max)
    val items = read_data(args(0))
    // JVM Warmup..
    1 to 5 foreach { _ => run(env, items) }
  }

  def run(env: StreamExecutionEnvironment, items: List[Item]) = {
    val stream: DataStream[Item] = env.fromCollection(items)

    stream.keyBy(_.id)
      .map(item => new Item(item.id, item.price + 5)).setParallelism(2)
      .addSink(new ThroughputSink[Item](100000)).setParallelism(1)

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
      val price = splitted(1).toLong
      Item(id, price)
    }).toList
  }
}
