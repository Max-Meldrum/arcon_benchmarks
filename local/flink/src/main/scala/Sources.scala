package threading

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

object Sources {
  def itemSource(path: String, scalingFactor: Int): SourceFunction[Item] = {
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
