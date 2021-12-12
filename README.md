# 金融大数据-实验4

施宇 191250119

## 任务一

 将整个任务分为两个部分统计特征`industry`每个取值的数量，并按照数量进行排序。

### 统计数量

与WordCount任务类似，在Map阶段，对于读入的每一行数据，先按照逗号分开，得到每个单独的数据，然后取出industry的取值，这里有一点需要注意，Map第一次读入的数据为列名，需要去除。Map的输出为`(industry特征的值， 1)`。

```java
public static class ICMap extends Mapper<LongWritable, Text, Text, LongWritable>{
    private boolean first = true;
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context){
        if(first){
            first = false;
            return;
        }
        String train_data = value.toString();
        String[] features = train_data.split(",");
        String industry = features[10];
        //            System.out.println(industry);
        try {
            context.write(new Text(industry), new LongWritable(1));
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
```

在Reduce阶段，合并每个industry取值的计数，输出最终的计数结果：

```java
public static class ICReducer
    extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values,
                          Context context
                         ){
        long count = 0;
        for(LongWritable value: values){
            count += value.get();
        }
        try {
            context.write(key, new LongWritable(count));
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
```

计数结果：

<img src="D:\Hadoop\实验四\image\13.png" alt="13" style="zoom:80%;" />

### 排序

由于是按照值进行排序的，所以通过构造一个新的数据类，将industry和值都作为该类的变量，该类之间进行排序比较的时候，先按照值的大小比较，再按照industry的大小比较。

在Map中，读入上一步计数的结果后，输出`((industry, count), Null)`：

```java
public static class ISMapper extends Mapper<LongWritable, Text , TextIntWritable, NullWritable>{

    @Override
    public void map(LongWritable key, Text value, Context context){
        try{
            TextIntWritable k = new TextIntWritable();
            String []string = value.toString().split("\t");
            k.set(new Text(string[0]), new IntWritable(Integer.valueOf(string[1])));
            context.write(k,  NullWritable.get());
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

在Reduce中，由于读入的键值对已经是有序的，所以直接输出即可：

```java
public static class ISReducer extends Reducer<TextIntWritable, NullWritable, TextIntWritable, NullWritable>{
    public void reduce(TextIntWritable key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException{
        for(NullWritable v : value)
            context.write(key, v);
    }
}
```

排序结果：

<img src="D:\Hadoop\实验四\image\14.png" alt="14" style="zoom:80%;" />

### 通过Spark复现结果

先读入数据：

```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

session = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
df = session.read.csv("train_data.csv", encoding="utf-8", header=True)
```

选出industry列后按照取值分组计数，然后按照计数结果降序排列：

```python
df.select("industry").groupBy("industry").count().orderBy(f.desc("count")).show()
```

结果：

<img src="D:\Hadoop\实验四\image\15.png" alt="15" style="zoom:80%;" />

## 任务二

使用Spark的Python API，PySpark完成后续的spark程序的编写。

由于需要按区间对total_loan进行计数，所以新建一列`total_loan_class`，该列的值为该行`total_loan`的值所在的区间。再新建一列total_loan_sort，为区间的左端，用于后续的排序。生成该列的函数如下，将该函数注册为udf函数：

```python
def total_loan_classify(str_value):
    value = float(str_value)
    c = int(value // 1000)
    res = "(" + str(c*1000) + "," + str((c+1)*1000) + ")"
    return res
def total_loan_sort(str_value):
    value = int(str_value.split(",")[0][1:])
    return value
udf_total_loan_classify = f.udf(total_loan_classify, StringType())
udf_total_loan_sort = f.udf(total_loan_sort, StringType())
```

选出total_loan和total_loan_class列后，按照total_loan_class列分组计数，然后按照total_loan_sort进行升序排列：

```python
df = df.withColumn("total_loan_class", udf_total_loan_classify("total_loan"))
res = df.select(["total_loan", "total_loan_class"]).groupBy("total_loan_class").count()
res = res.withColumn("total_loan_sort", udf_total_loan_sort("total_loan_class"))
res = res.withColumn("total_loan_sort", res["total_loan_sort"].cast(IntegerType()))
res = res.orderBy("total_loan_sort")
res = res.select(["total_loan_class", "count"])
```

结果如下：

<img src="D:\Hadoop\实验四\image\25.png" alt="25" style="zoom:80%;" />

根据统计结果绘制频度分布直方图：

```python
respd = res.toPandas()
plt.figure(figsize=(15, 10))
sns.barplot(respd['count'], respd['total_loan_class'], orient='h')
```

![26](D:\Hadoop\实验四\image\26.png)

可以看出贷款金额分布大致呈左偏分布。

## 任务三

### 第一问

先计算数据总数：

<img src="D:\Hadoop\实验四\image\17.png" alt="17" style="zoom:80%;" />

所以每个数据的占比为1/300000，将该值存储为列`employer_type_percent`：

```python
df = df.withColumn("employer_type_percent", f.lit(1/total_employer_type))
```

从数据中选出employer_type和employer_type_percent，然后按照employer_type分组对employer_type_percent求和，再按照求和的大小正序排列并保留4位小数，导出成csv格式：

```python
res = df.select(["employer_type", "employer_type_percent"]).groupBy("employer_type").agg(f.sum("employer_type_percent")).orderBy(f.sum("employer_type_percent"))
res = res.withColumnRenamed('sum(employer_type_percent)','employer_type_percent')
res = res.select(['employer_type', f.bround("employer_type_percent", scale=4).alias('employer_type_percent')])
res.repartition(1).write.csv("./work3-1", encoding="gbk", header=True)
```

结果：

<img src="D:\Hadoop\实验四\image\18.png" alt="18" style="zoom:80%;" />

### 第二问

由于数据读入的时候，所有的数据都默认设置为了String类型，因此先将需要参与计算的三个变量`year_of_loan`、`monthly_payment`、`total_loan`转为合适的类型：

```python
df = df.withColumn("year_of_loan", df["year_of_loan"].cast(IntegerType()))
df = df.withColumn("monthly_payment", df["monthly_payment"].cast(FloatType()))
df = df.withColumn("total_loan", df["total_loan"].cast(FloatType()))
```

构建新列`total_money`，按照题目提供的公式计算，然后导出csv格式：

```python
df = df.withColumn("total_money", df["year_of_loan"]*df["monthly_payment"]*12-df["total_loan"])
res = df.select(["user_id", "total_money"])
res.repartition(1).write.csv("./work3-2", encoding="gbk", header=True)
```

结果（部分）：

<img src="D:\Hadoop\实验四\image\19.png" alt="19" style="zoom:80%;" />

### 第三问

由于work_year特征的类别为String，且总体可分为三种类型：< 1 year，x years，10+ years，为了筛选出所有大于5 years的值，需要分情况进行处理。新增列work_year_num，对于work_year取值为：Null，设置其值为-1；< 1 year，设置其值为0；x years，设置其值为x；10+ years，设置其值为11。再筛选出所有work_year_num大于5的行即可：

```python
def work_year_process(str_value):
    if str_value is None:
        return -1
    elif '10+' in str_value:
        return 11
    elif "<" in str_value:
        return 0
    else:
        str_num = str_value.split(' ')[0]
        return int(str_num)
udf_work_year_process = f.udf(work_year_process, StringType())
df = df.withColumn("work_year_num", udf_work_year_process('work_year'))
res = df.select(['user_id', 'censor_status', 'work_year', 'work_year_num']).filter(df['work_year_num']>5)
res = res.select(['user_id', 'censor_status', 'work_year'])
res.repartition(1).write.csv("./work3-3", encoding="gbk", header=True)
```

结果（部分）：

<img src="D:\Hadoop\实验四\image\20.png" alt="20" style="zoom:80%;" />



## 任务四

先根据数据中每个特征的含义和内容，确定其合理的数据类型，然后按照该类型读取数据：

```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.ml.feature import VectorAssembler
import pyspark.sql.types as typ
import pandas as pd
import  pyspark.ml.feature as ft
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import RandomForestClassifier
import matplotlib.pyplot as plt
from pyspark.ml.evaluation import BinaryClassificationEvaluator 
from pyspark.ml.evaluation import MulticlassClassificationEvaluator 

session = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
labels = [('loan_id', typ.IntegerType()),
          ('user_id', typ.IntegerType()),
          ('total_loan', typ.DoubleType()),
          ('year_of_loan', typ.IntegerType()),
          ('interest', typ.DoubleType()),
          ('monthly_payment', typ.DoubleType()),
          ('class', typ.StringType()),
          ('sub_class', typ.StringType()),
          ('work_type', typ.StringType()),
          ('employment_type', typ.StringType()),
          ('industry', typ.StringType()),
          ('work_year', typ.StringType()),
          ('house_exist', typ.IntegerType()),
          ('house_loan_status', typ.IntegerType()),
          ('censor_status', typ.IntegerType()),
          ('marriage', typ.IntegerType()),
          ('offsprings', typ.IntegerType()),
          ('issue_date', typ.StringType()),
          ('use', typ.IntegerType()),
          ('post_code', typ.DoubleType()),
          ('region', typ.IntegerType()),
          ('debt_loan_ratio', typ.DoubleType()),
          ('del_in_18month', typ.DoubleType()),
          ('scoring_low', typ.DoubleType()),
          ('scoring_high', typ.DoubleType()),
          ('pub_dero_bankrup', typ.DoubleType()),
          ('early_return', typ.IntegerType()),
          ('early_return_amount', typ.IntegerType()),
          ('early_return_amount_3mon', typ.DoubleType()),
          ('recircle_b', typ.DoubleType()),
          ('recircle_u', typ.DoubleType()),
          ('initial_list_status', typ.IntegerType()),
          ('earlies_credit_mon', typ.StringType()),
          ('title', typ.DoubleType()),
          ('policy_code', typ.DoubleType()),
          ('f0', typ.DoubleType()),
          ('f1', typ.DoubleType()),
          ('f2', typ.DoubleType()),
          ('f3', typ.DoubleType()),
          ('f4', typ.DoubleType()),
          ('f5', typ.DoubleType()),
          ('is_default', typ.IntegerType()),
          ]
schema = typ.StructType([
    typ.StructField(e[0], e[1], True) for e in labels
])
df = session.read.csv("train_data.csv", encoding="utf-8", header=True, schema=schema)
```

填充缺失值，对于数值变量，缺失值填充为-1，对于字符串变量，缺失值填充为'-1'：

```python
df = df.na.fill(-1)
df = df.na.fill('-1')
```

对所有String类型的特征进行编码，转为数值类型特征：

```python
strings = ['class', 'sub_class', 'work_type', 'employment_type', 'industry', 'work_year', 'issue_date', 'earlies_credit_mon']
indexes = [StringIndexer(inputCol =s, outputCol=s+"_ind") for s in strings]
pipeline = Pipeline(stages=indexes)
tmodel = pipeline.fit(df)
dfres = tmodel.transform(df)
dfres = dfres.drop(*strings)
```

考虑到日期特征取值较多，进行类型编码后类型数量过多，因此对两个日期特征进行分桶，设置桶的数量为20：

```python
discretizers = [QuantileDiscretizer(numBuckets=20, inputCol=s, outputCol=s+"b") for s in ["issue_date_ind", "earlies_credit_mon_ind"]]
pipeline = Pipeline(stages=discretizers)
bmodel = pipeline.fit(dfres)
dfres = bmodel.transform(dfres)
dfres = dfres.drop("issue_date_ind", "earlies_credit_mon_ind")
```

<img src="D:\Hadoop\实验四\image\27.png" alt="27" style="zoom:80%;" />

进行特征集成，将所有特征合并到一个数组feature中：

```python
df_assembler = VectorAssembler(inputCols=data.columns, outputCol="features")
data = df_assembler.transform(dfres)
```

集成结果：

<img src="D:\Hadoop\实验四\image\21.png" alt="21" style="zoom:80%;" />

按照8：2的比例划分训练集和测试集：

```python
train_data, test_data = data.randomSplit([0.8,0.2]) 
```

### Logistic回归

使用Logistic回归进行分类：

```python
logistic = cl.LogisticRegression(maxIter=10, regParam=0.01, labelCol='is_default').fit(train_data)
# 预测
predictions = logistic.transform(test_data)
```

计算预测准确性：

```python
auc = BinaryClassificationEvaluator(labelCol='is_default').evaluate(predictions)
print('BinaryClassificationEvaluator 准确性： {0:.0%}'.format(auc))
```

<img src="D:\Hadoop\实验四\image\28.png" alt="28" style="zoom:80%;" />

### 随机森林

使用随机森林模型进行分类：

```python
cla = RandomForestClassifier(labelCol='is_default', maxDepth=7, maxBins=700, numTrees=30).fit(train_data)
```

训练完成后，对测试集中的数据进行预测：

```python
predictions = cla.transform(test_data)
```

绘制特征重要性柱状图：

```python
fig = plt.figure(figsize=(4,10))
plt.barh(feas, cla.featureImportances)
```

![22](D:\Hadoop\实验四\image\22.png)

可以发现，employment_type_ind、work_type_ind、offsprings、marriage和interest的重要性较高。

查看测试集的预测情况：

<img src="D:\Hadoop\实验四\image\23.png" alt="23" style="zoom:80%;" />

可以看到大部分预测是准确的，有小部分的预测是错误的。计算模型在测试集上的准确率：

<img src="D:\Hadoop\实验四\image\24.png" alt="24" style="zoom:80%;" />

准确率为85%，较Logistic模型79%的准确率有一定程度的提高。

## 附录-安装Spark(Python版本)

到[官网](http://spark.apache.org/downloads.html)下载Spark，这里选择2.4.5版本，需要去历史release里下载：

<img src="D:\Hadoop\实验四\image\1.png" alt="1" style="zoom:80%;" />

由于目前已经安装的Hadoop版本为2.9.1，所以这里选择对应Hadoop版本为2.7的spark：

<img src="D:\Hadoop\实验四\image\2.png" alt="2" style="zoom:80%;" />

下载后解压。

配置环境变量：新建变量SPARK_HOME

<img src="D:\Hadoop\实验四\image\3.png" alt="3" style="zoom:80%;" />

在Path变量中添加：

<img src="D:\Hadoop\实验四\image\4.png" alt="4" style="zoom:80%;" />

为了防止Spark每次运行时给出许多信息，影响结果的直观性，因此需要修改一下INFO设置。找到spark中的conf文件夹并打开，找到log4j.properties.template文件，复制一份修改文件名为log4j.properties，并写字板打开修改INFO为ERROR（或WARN）

<img src="D:\Hadoop\实验四\image\5.png" alt="5" style="zoom:80%;" />

打开Anaconda命令行，输入pyspark验证是否安装成功：

<img src="D:\Hadoop\实验四\image\6.png" alt="6" style="zoom:80%;" />

启动Jupter Notebook，尝试导入spark，发现失败：

<img src="D:\Hadoop\实验四\image\7.png" alt="7" style="zoom:80%;" />

这是因为当前的Anaconda的环境中还没有pyspark包，因此这里手动安装，把Spark路径下的pyspark文件夹复制粘贴到安装的Anaconda3下envs中对应的环境的Lib下的site-packages下面：

<img src="D:\Hadoop\实验四\image\8.png" alt="8" style="zoom:80%;" />

<img src="D:\Hadoop\实验四\image\9.png" alt="9" style="zoom:80%;" />

再次测试，发现提示没有py4j包：

<img src="D:\Hadoop\实验四\image\10.png" alt="10" style="zoom:80%;" />

使用Anaconda在当前环境中安装该包：

<img src="D:\Hadoop\实验四\image\11.png" alt="11" style="zoom:80%;" />

再次测试，发现导入成功：

<img src="D:\Hadoop\实验四\image\12.png" alt="12" style="zoom:80%;" />
