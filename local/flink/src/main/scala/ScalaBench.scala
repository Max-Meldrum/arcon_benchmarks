import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import java.lang.System.currentTimeMillis
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

object ScalaBench {
  def main(args: Array[String]) {
    val r = new scala.util.Random
    val vec = (1 to 100000).map(_ => 1 + r.nextInt(100)).toVector
    val sensorData = SensorData(1, vec)

    1 to 40 foreach { _ => time(run(sensorData)) }
  }

  def run(sensorData: SensorData) {
    val enriched = new EnrichedSensor(sensorData.id, sensorData.vec.map(_ + 5).filter(_ > 50).sum)
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

}
