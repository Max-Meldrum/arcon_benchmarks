import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import java.lang.System.currentTimeMillis
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext


case class Item(id: Long, price: Long, timestamp: Long)

object MapReduce {
  def main(args: Array[String]) {
    if (args.length == 2) {
      val totalMappers = args(0).toInt
      val totalReducers = args(1).toInt
      val env = StreamExecutionEnvironment.createLocalEnvironment()
      //env.disableOperatorChaining()
      val data: List[Item] = read_data("data")
      // Warming up the JVM
      1 to 20 foreach { _ => run(data, env, totalMappers, totalReducers) }
    } else {
      println("Expected 2 args: (1) total mappers; (2) total reducers")
    }
  }

  def run(data: List[Item], env: StreamExecutionEnvironment, mappers: Int, reducers: Int) = {
    val stream: DataStream[Item] = env.fromCollection(data)
    /*
    val stream = env.addSource(new SourceFunction[Item]() {
       override def run(ctx: SourceContext[Item]) = {
         while (true) {
           ctx.collect(Item(1, 3, 10))
           println("putting to sleep")
           Thread.sleep(1000)
         }
       }
       override def cancel(): Unit =  {}
    })
    */


    val pipeline = stream
      .rebalance
      .map(obj => new Item(obj.id, obj.price + 5, obj.timestamp)).setParallelism(mappers)
      .keyBy(0)
      .reduce { (ob1, ob2) => new Item(ob2.id, ob1.price + ob2.price, ob2.timestamp) }.setParallelism(reducers)
      .addSink(new CountingSink(data.length)).setParallelism(1)

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
      Item(id, price, 1)
    }).toList
  }
}

class CountingSink(totalMessages: Long) extends RichSinkFunction[Item]{
  var totalPrice: Long = 0
  var counter: Long = 0
  var instant: Long = 0
  
  override def invoke(item: Item) {
    totalPrice += item.price
    counter += 1
    if (counter > 1) {
      if (counter >= totalMessages) {
        val after = currentTimeMillis
        val time = after - instant
        println("Total time in ms " + time)
        println("Total price of stream " + totalPrice)
      }
    } else {
      instant = currentTimeMillis
    }
  }
}

