import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import java.lang.System.currentTimeMillis
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

case class SensorData(id: Int, vec: Seq[Int], ts: Int)
case class EnrichedSensor(id: Int, total: Int, ts: Int)

object Sensor {
  def main(args: Array[String]) {
    if (args.length == 1) {
      val totalSensors = args(0).toInt
      val env = StreamExecutionEnvironment.createLocalEnvironment()
      //env.disableOperatorChaining()
      val data: List[SensorData] = read_data("sensor_data")
      // Warming up the JVM
      1 to 20 foreach { _ => run(data, env, totalSensors) }
    } else {
      println("Expected 1 args: (1) total sensor tasks")
    }
  }

  def run(data: List[SensorData], env: StreamExecutionEnvironment, sensorTasks: Int) = {
    val stream: DataStream[SensorData] = env.fromCollection(data)

    val pipeline = stream
      .rebalance
      //.filter(sensor => sensor.id < 5)
      .map(sensor => {
        val total = sensor.vec.map(_ + 5).filter(_ > 50).sum
        new EnrichedSensor(sensor.id, total, 1)
      }).setParallelism(sensorTasks)
      .addSink(new SensorSink(5000)).setParallelism(1)

    println(env.getExecutionPlan)
    val res = env.execute()
  }
  def read_data(path: String): List[SensorData] = {
    val source = scala.io.Source.fromFile(path)
    val lines = source.getLines
    lines.map(line => {
      val splitted = line.split(" ")
      val id = splitted(0).toInt
      val numbers = splitted(1).split(",").map(_.toInt)
      SensorData(id, numbers, 1)
    }).toList
  }
}

// Throughput logic taken from: https://github.com/lsds/StreamBench/blob/master/yahoo-streaming-benchmark/src/main/scala/uk/ac/ic/imperial/benchmark/flink/YahooBenchmark.scala
class SensorSink(logFreq: Long) extends RichSinkFunction[EnrichedSensor] {
  private var lastTotalReceived: Long = 0L
  private var lastTime: Long = 0L
  private var totalReceived: Long = 0L
  private var averageThroughput = 0d
  private var throughputCounter = 0
  private var throughputSum = 0d
  private var count: Long = 0

  var totalSensors: Long = 0
  var instant: Long = 0
  
  override def invoke(sensor: EnrichedSensor) {
    if (totalReceived == 0) {
          println(s"ThroughputLogging:${System.currentTimeMillis()},${totalReceived}")
    }
    totalReceived += 1
    if (totalReceived % logFreq == 0) {
      val currentTime = System.currentTimeMillis()
      val throughput = (totalReceived - lastTotalReceived) / (currentTime - lastTime) * 1000.0d
      if (throughput !=0) {
        throughputCounter +=1
        throughputSum += throughput
        averageThroughput = throughputSum / throughputCounter
      }
      println(s"Throughput:${throughput}, Average:${averageThroughput}")
      lastTime = currentTime
      lastTotalReceived = totalReceived
      println(s"ThroughputLogging:${System.currentTimeMillis()},${totalReceived}")
    }
  }
/*
    totalSensors += sensor.total
    counter += 1
    if (counter > 1) {
      if (counter >= totalMessages) {
        val after = currentTimeMillis
        val time = after - instant
        println("Total time in ms " + time)
        println("Total sensors " + totalSensors)
      }
    } else {
      instant = currentTimeMillis
    }
  }
*/
}

class SensorSource(data: List[SensorData]) extends SourceFunction[SensorData] {

  var isRunning: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[SensorData]): Unit = {
    // For each sensor item, add currentTimeMilli to field
    var cnt: Long = -1
    while (isRunning && cnt < Long.MaxValue) {
      // increment cnt
      cnt += 1
      ctx.collect(SensorData(1, Seq(1,2,3), 1))
    }

  }

  override def cancel(): Unit = isRunning = false
}
