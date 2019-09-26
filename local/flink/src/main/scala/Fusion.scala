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
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    run(env)
  }

  def run(env: StreamExecutionEnvironment) = {
    val stream = env.addSource(new SourceFunction[SensorData]() {
       override def run(ctx: SourceContext[SensorData]) = {
         var counter: Long = 0
         val r = new scala.util.Random
         val limit: Long = 10000000
         while (counter < limit) {
           val id = 1 + r.nextInt(( 10 - 1) + 1)
           val vec = (1 to 20).map(_ => 1 + r.nextInt(100)).toVector
           ctx.collect(new SensorData(id, vec))
           counter += 1
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
}
