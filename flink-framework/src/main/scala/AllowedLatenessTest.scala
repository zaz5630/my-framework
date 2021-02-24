import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 窗口大小5s，允许乱序5s，允许迟到2s，最大允许迟到7s
 watermark = 当前事件时间 - 5s
[1461756860000,1461756865000)
[1461756865000,66000)
000001,1461756862000   watermark 0-5000 -> 1461756857000
000001,1461756866000   watermark 1461756861000
000001,1461756868000   watermark 1461756863000
000001,1461756869000   watermark 1461756864000
000001,1461756870000   - watermark 1461756865000 触发第一个窗口：[1461756860000,1461756865000)   ok> (000001,1)
000001,1461756862000   --迟到数据,依然属于第一个窗口 62000 > 60000 , 62000 < 60000 + 7000    ok> (000001,2)
000001,1461756871000    watermark 1461756866000
000001,1461756872000    watermark 1461756867000
000001,1461756862000     late> (000001,1461756862000)   1461756867000 - 1461756857000 = 10s
000001,1461756863000     late> (000001,1461756863000)   1461756867000 - 1461756858000  = 9s
000001,1461756864000     late> (000001,1461756864000)   1461756867000 - 1461756859000  = 8s
000001,1461756865000     late> (000001,1461756864000)   1461756867000 - 1461756860000  = 7s  属于第二个窗口，属于ok数据
 */
//todo: 允许延迟一段时间，并且对延迟太久的数据单独进行收集
object AllowedLatenessTest {

  def main(args: Array[String]): Unit = {
      //todo:1.构建流式处理环境
      val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
      import org.apache.flink.api.scala._
      environment.setParallelism(1)


     //todo:2.设置时间类型
   environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //todo:3.获取数据源
    val sourceStream: DataStream[String] = environment.socketTextStream("106.52.231.38",8888)

    //todo:4. 数据处理
    val mapStream: DataStream[(String, Long)] = sourceStream.map(x=>(x.split(",")(0),x.split(",")(1).toLong))

    //todo: 定义一个侧输出流的标签，用于收集迟到太多的数据
      val lateTag=new OutputTag[(String, Long)]("late")

    //todo:5.  数据计算--添加水位线
    val result: DataStream[(String, Long)] = mapStream.assignTimestampsAndWatermarks(
            new AssignerWithPeriodicWatermarks[(String, Long)] {
              //最大的乱序时间
              val maxOutOfOrderness = 5000L
              //记录最大事件发生时间
              var currentMaxTimestamp: Long = _

              //周期性的生成水位线watermark
              override def getCurrentWatermark: Watermark = {
              val watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
                watermark
              }

              //抽取事件发生时间
            override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
                //获取事件发生时间
                val currentElementEventTime: Long = element._2

                //对比当前事件时间和历史最大事件时间, 将较大值重新赋值给currentMaxTimestamp
              currentMaxTimestamp = Math.max(currentMaxTimestamp, currentElementEventTime)

                println("接受到的事件：" + element + " |事件时间： " + currentElementEventTime )

                currentElementEventTime
              }
      })
              .keyBy(0)
              .timeWindow(Time.seconds(5))
              .allowedLateness(Time.seconds(2)) //允许数据延迟2s
            .sideOutputLateData(lateTag)     //收集延迟大多的数据
              .process(new ProcessWindowFunction[(String, Long), (String, Long), Tuple, TimeWindow] {
                          override def process(key: Tuple, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, Long)]): Unit = {
                              //获取分组的字段
                              val value: String = key.getField[String](0)

                              //窗口的开始时间
                            val startTime: Long = context.window.getStart
                              //窗口的结束时间
                              val startEnd: Long = context.window.getEnd

                              //获取当前的 watermark
                              val watermark: Long = context.currentWatermark

                              var sum: Long = 0
                              val toList: List[(String, Long)] = elements.toList

                              for (eachElement <- toList) {
                            sum += 1
                          }


                          println("窗口的数据条数:" + sum +
                          " |窗口的第一条数据：" + toList.head +
                            " |窗口的最后一条数据：" + toList.last +
                          " |窗口的开始时间： " + startTime +
                            " |窗口的结束时间： " + startEnd +
                            " |当前的watermark:" + watermark)

                          out.collect((value, sum))

            }
            })

    //todo: 打印延迟太多的数据 侧输出流：主要的作用用于保存延迟太久的数据
    result.getSideOutput(lateTag).print("late")

    //todo:打印正常的数据
    result.print("ok")

    //todo：启动任务
    environment.execute()

  }

}
