import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

// Throughput logic taken from: https://github.com/lsds/StreamBench/blob/master/yahoo-streaming-benchmark/src/main/scala/uk/ac/ic/imperial/benchmark/flink/YahooBenchmark.scala
class ThroughputSink[A](logFreq: Long, logOn: Boolean) extends RichSinkFunction[A] {
  private var lastTotalReceived: Long = 0L
  private var lastTime: Long = 0L
  private var totalReceived: Long = 0L
  private var averageThroughput = 0d
  private var throughputCounter = 0
  private var throughputSum = 0d
  
  override def invoke(input: A) {
    if (totalReceived == 0) {
      if (logOn) {
        println(s"ThroughputLogging:${System.currentTimeMillis()},${totalReceived}")
      }
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

      if (logOn) {
        println(s"Throughput:${throughput}, Average:${averageThroughput}")
        println(s"ThroughputLogging:${System.currentTimeMillis()},${totalReceived}")
      }
      lastTime = currentTime
      lastTotalReceived = totalReceived
    }
  }
}
