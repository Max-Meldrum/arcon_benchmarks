import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import java.lang.System.currentTimeMillis
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

case class SensorData(id: Int, vec: Seq[Int])
case class EnrichedSensor(id: Int, total: Int)

object Fusion {
  def main(args: Array[String]) {
    if (args.length == 1) {
      val version = args(0).toInt
      if (version == 1) {
        val env = StreamExecutionEnvironment.createLocalEnvironment()
        run(env)
      } else if (version == 2) {
        val env = StreamExecutionEnvironment.createLocalEnvironment()
        runTwo(env)
      } else {
        println("Expected number 1 or 2.")
      }
    } else {
      println("Expected number 1 or 2.")
    }
  }

  def run(env: StreamExecutionEnvironment) = {
    val stream = env.addSource(new SourceFunction[SensorData]() {
       override def run(ctx: SourceContext[SensorData]) = {
         while (true) {
           val r = new scala.util.Random
           val id = 1 + r.nextInt(( 10 - 1) + 1)
           val vec = (1 to 20).map(_ => 1 + r.nextInt(100)).toVector
           ctx.collect(new SensorData(id, vec))
         }
       }
       override def cancel(): Unit =  {}
    }).map(sensor => {
        val total = sensor.vec.map(_ + 5).filter(_ > 50).sum
        new EnrichedSensor(sensor.id, total)
    }).setParallelism(1)
      .addSink(new ThroughputSink[EnrichedSensor](100000)).setParallelism(1)

    println(env.getExecutionPlan)
    val res = env.execute()
  }

  def runTwo(env: StreamExecutionEnvironment) = {
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
    }).filter(item => item.id < 5 ).setParallelism(1)
      .map(item => Item(item.id, item.price + 10)).setParallelism(1)
      .map(item => Item(item.id, item.price * 2)).setParallelism(1)
      .addSink(new ThroughputSink[Item](100000)).setParallelism(1)

    println(env.getExecutionPlan)
    val res = env.execute()
  }
}
