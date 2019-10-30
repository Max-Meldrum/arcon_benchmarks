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

  def itemSource(path: String, scalingFactor: Double): SourceFunction[Item] = {
    new SourceFunction[Item]() {
       override def run(ctx: SourceContext[Item]) = {
          val source = scala.io.Source.fromFile(path)
          val lines = source.getLines
          lines.foreach { line =>
            val splitted = line.split(" ")
            val id = splitted(0).toInt
            val number = splitted(1).toLong
            val item = Item(id, number, scalingFactor)
            ctx.collect(item)
          }
       }
       override def cancel(): Unit =  {}
    }
  }
}
