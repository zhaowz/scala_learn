// 导入包
import spark.implicits._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._

// 获取情感词列表
def get_sentiment_list( line:String ) : String = {
    var sentiment_list = new ListBuffer[String]()
    var words = line.split(" ",0)
    val word_num = words.length
    for ( k <- 0 to word_num-1 ) {
         var word = words(k)
         var before_word=""
         var senti_word=word
         if(sentiment_map.contains(word)){
             var score = sentiment_map.get(word).get.toDouble
             println(k,word,score )
             if(score!=0){
                 if(k!=0){
                    before_word = words(k-1)
                    if(deny_map.contains(before_word) || degree_map.contains(before_word)){
                        senti_word = before_word + word
                    }
                 }
                 sentiment_list+=senti_word
             }
         }
      }
      return sentiment_list(0)
   }
// get_sentiment_list("这件 衣服 非常 难看")

//获取情感分值
def compute_sentiment(line:String):Double={
    val words = line.split(" ",0)
    var sum_score:Double= 0
    val word_num=words.length
    for(k<-0 to word_num-1){
        var word=words(k)
        if(sentiment_map.contains(word)){
            var score = sentiment_map.get(word).get.toDouble
            println(word,score)
            if(k!=0){
                var before_word = words(k-1)
                if(deny_map.contains(before_word)){
                    score=(-1) * score
                }
                else if(degree_map.contains(before_word)){
                    score = degree_map.get(before_word).get.toDouble * score
                }
            }
            sum_score+=score
        }
    }
    return sum_score
}
// compute_sentiment("这件 衣服 非常 难看 鞋子 俊美")

//获取情感类别
def get_senti_label(line:String):String={
    val score:Double = compute_sentiment(line)
    if(score>0){
        return "正面"
    }else if(score<0){
        return "负面"
    }else{
        return "中立"
    }
}
// get_senti_label("这件 衣服 非常 难看 鞋子 俊美")

// 平台获取聊天数据，情感分值等
var chatDF = spark.read.format("csv").option("header","false").option("delimiter","\t").load("/user/zeppelin/yunying_dm/bbs_data")
chatDF = chatDF.toDF(Seq("hash_id","game","source","time","user_name","content","title","star","forum_id","type1","type2","type3","keyword","data"):_*)
chatDF.show()
var df = chatDF.select("time","user_name","content")
df.show(3)

// 获取情感词典等数据
def csv_to_map(filename:String, header:Seq[String]):Map[String,String]= {
     // 读取csv为dataframe，转为scala map类型
  var df=spark.read.format("csv").option("header","false").option("delimiter","\t").load("/user/zeppelin/yunying_dm/"+filename)
  df = df.toDF(Seq("word","sentiment"):_*)
  val array = df.collect.map(r => Map(df.columns.zip(r.toSeq):_*))
  val dict = Map(array.map(row => (row.getOrElse("word", null), row.getOrElse("sentiment", null))):_*)
  return dict.asInstanceOf[Map[String, String]]
}
val sentiment_map = csv_to_map("sentiment.csv",Seq("word","sentiment"))
val deny_map = csv_to_map("deny.csv",Seq("word","deny"))
val degree_map = csv_to_map("degree.csv",Seq("word","degree"))

// 声明udf
val get_sentiment_list_udf = udf(get_sentiment_list _)
val compute_sentiment_udf = udf(compute_sentiment _)
val get_senti_label_udf = udf(get_senti_label _)

// 暂时无法分词，使用测试数据
val df_test = Seq(
  (1, "这件 衣服 非常 难看"),
  (2, "这件 衣服 真 漂亮")).toDF("id", "content")
df_test.show()
val df_sentiment_list=df_test.withColumn("sentiment", get_sentiment_list_udf($"content"))  //获取情感词列表
df_sentiment_list.show()

