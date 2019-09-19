import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import java.lang.System.currentTimeMillis
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

case class Item(id: Long, price: Long)

object Threading {
  def main(args: Array[String]) {
    if (args.length == 1) {
      val parallelism  = args(0).toInt
      val env = StreamExecutionEnvironment.createLocalEnvironment()
      env.disableOperatorChaining()
      run(env, parallelism)
    } else {
      println("Expected 1 arg: parallelism of filters/mappers")
    }
  }

  def run(env: StreamExecutionEnvironment, parallelism: Int) = {
    val stream = env.addSource(new SourceFunction[Item]() {
       override def run(ctx: SourceContext[Item]) = {
         while (true) {
           val r = new scala.util.Random
           val id = 1 + r.nextInt(( 10 - 1) + 1)
           val price = 1 + r.nextInt(( 100 - 1) + 1)
           ctx.collect(Item(id, price))
         }
       }
       override def cancel(): Unit =  {}
    }).rebalance
      .filter(obj => obj.id > 2).setParallelism(parallelism)
      .shuffle
      .map(obj => new Item(obj.id, obj.price + 5)).setParallelism(parallelism)
      .addSink(new ThroughputSink[Item](100000)).setParallelism(1)

    println(env.getExecutionPlan)
    val res = env.execute()
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
