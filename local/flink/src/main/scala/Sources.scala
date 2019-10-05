import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

object Sources {
  def fusionSource(): SourceFunction[Int] = {
    new SourceFunction[Int]() {
       override def run(ctx: SourceContext[Int]) = {
         var counter: Long = 0
         val r = new scala.util.Random
         val limit: Long = 10000000
         while (counter < limit) {
           ctx.collect(10)
           counter += 1
         }
       }
       override def cancel(): Unit =  {}
    }
  }
}
