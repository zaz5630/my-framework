import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Event Time语义下我们使用Watermark来判断数据是否迟到。一个迟到元素是指元素到达窗口算子时，该元素本该被分配到某个窗口，但由于延迟，窗口已经触发计算。目前Flink有三种处理迟到数据的方式：
 * （1）直接将迟到数据丢弃：会造成数据丢失
 * （2）将迟到数据发送到另一个流：输出到侧输出流，保证数据的完整性，可更新计算结果
 * （3）重新执行一次计算，将迟到数据考虑进来，更新计算结果：数据准确率高，保证数据完整性
 * 需要注意的是，使用了allowedLateness可能会导致两个窗口合并成一个窗口
 *
窗口5s,迟到3s  [1614171330000,1614171335000)  [1614171335000,1614171340000)  允许乱序2s
aaa,1614171330110  watermark:1614171330110 ->  1614171328110
ccc,1614171335110  watermark:1614171335110 ,触发aaa,输出  normal> (aaa,1) -> 1614171333110
ddd,1614171331610     --迟到数据 > 结束时间1614171330000 ，< 1614171330000+5000，也会纳入到 [1614171330000,1614171335000) 窗口,
						即：迟到元素归属窗口的结束时间 + lateness > watermark 时间，该元素仍然会被加入到该窗口中
            输出：normal> (ddd,1)  watermark:1614171335110  -> 1614171333110
hhh,1614171339998     watermark:1614171339998  -> 1614171337998, 触发第一个窗口 normal> (aaa,1)  normal> (ddd,1)
没设置乱序时间：aaa,1614171330110   1614171339998 - 1614171330110 > 9s  lateData> (aaa,1)
没设置乱序时间：kkk,1614171334110   1614171339998 - 1614171334110 > 5s  lateData> (aaa,1)
没设置乱序时间：ggg,1614171335110   1614171339998 - 1614171335110 > 4s  lateData> (aaa,1) 这个数据属于第二个窗口，不会触发
lll,1614171341998     watermark:1614171341998  -> 1614171339998
fff,1614171334998     1614171339998 - 1614171332998 = 7000
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 从外部命令中获取参数
    //val params: ParameterTool =  ParameterTool.fromArgs(args)
    //val host: String = params.get("host")
    val host: String = "106.52.231.38"
    //val port: Int = params.getInt("port")
    val port: Int = 9999

    // 创建流处理环境
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 接收socket文本流
    val textDstream: DataStream[String] = env.socketTextStream(host, port)

    // flatMap和Map需要引用的隐式转换
    env.setParallelism(1)

    import org.apache.flink.api.scala._

    val lateData = new OutputTag[(String, Int)]("lateData")
    val dataStream: DataStream[(String, Int)] = textDstream
      //.flatMap(_.split(","))
      //.filter(_.nonEmpty)
      .map(x=>(x.split(",")(0),x.split(",")(1).toLong))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(0L)) {
        override def extractTimestamp(element: (String, Long)): Long = element._2
      })

      .map(t => (t._1,1))
      .keyBy(t => t._1)
      .timeWindow(Time.seconds(5))
      //设置迟到的数据超过了2秒的情况下，交给AllowedLateness处理
    //也分两种情况，第一种：允许数据迟到3秒（迟到2-3秒），再次迟到触发窗口函数，触发的条件是 watermark < end-of-window + AllowedLateness
      // 第二种：迟到的数据在3秒以上，输出到侧流中
      .allowedLateness(Time.seconds(3))
      .sideOutputLateData(lateData)  //收集延迟太多的数据
        .process(new ProcessWindowFunction[(String,Int),(String,Int),String,TimeWindow] {
          override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
            println("start:" + context.window.getStart)
            println("end:" + context.window.getEnd)
            for (elem <- elements) {
              out.collect(elem)
            }
          }
        })

    dataStream.print("normal")

    // 迟到时间超过5秒的数据，根据业务做处理，如果正常数据存储到mysql中，迟到的数据需要进行update
    dataStream.getSideOutput(lateData).print("lateData")


    // 启动executor，执行任务
    env.execute("Socket stream word count")
  }
}
