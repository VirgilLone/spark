from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer

'''
创建机器学习流水线，预测dataframe数据某列是否包含spark字符串
'''
spark = SparkSession.builder.master("local").appName("wordCount").getOrCreate()

training = spark.createDataFrame([
    (0, "a b c d spark", 1.0),
    (1, "b d", 0.0),
    (2, "spark f g h", 1.0),
    (3, "hadoop i j", 0.0)
], ["id", "text", "label"])

# 定义流水线各个阶段PipelineStage：
# 定义转换器 分词器
tokenizer = Tokenizer(inputCol="text", outputCol="words")
# 定义转换器 单词转换为特征向量
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
# 定义逻辑斯蒂回归评估器（算法）
lr = LogisticRegression(maxIter=10, regParam=0.001)

# 按照处理逻辑有序组织PipelineStage
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

# training数据集对pipeline进行训练，得到模型PipelineModel
model = pipeline.fit(training)

# 构建测试数据
test_data = spark.createDataFrame([
    (4, "spark i j k"),
    (5, "l m"),
    (6, "spark fink hadoop"),
    (7, "apache hadoop")
], ["id", "text"])

prediction = model.transform(test_data)
selected = prediction.select("id", "text", "probability", "prediction")
for row in selected.collect():
    rid, text, prob, prediction = row
    print("(%d,%s)-->prob=%s,prediction=%f" % (rid, text, str(prob), prediction))
