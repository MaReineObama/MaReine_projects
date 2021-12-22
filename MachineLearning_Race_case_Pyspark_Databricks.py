# Databricks notebook source
# DBTITLE 1,READING DATA 
#Gathering all the path together to use later

circuitsPath = "/FileStore/tables/Data_IndividualProject_BDT2_2021_RETAKE/circuits.csv"
CRPath = "/FileStore/tables/Data_IndividualProject_BDT2_2021_RETAKE/constructorResults.csv"
constructorPath = "/FileStore/tables/Data_IndividualProject_BDT2_2021_RETAKE/constructors.csv"
driverPath = "/FileStore/tables/Data_IndividualProject_BDT2_2021_RETAKE/drivers.csv"
lapTimePath = "/FileStore/tables/Data_IndividualProject_BDT2_2021_RETAKE/lapTimes.csv"
pitStopPath = "/FileStore/tables/Data_IndividualProject_BDT2_2021_RETAKE/pitStops.csv"
racePath = "/FileStore/tables/Data_IndividualProject_BDT2_2021_RETAKE/races.csv"
resultPath = "/FileStore/tables/Data_IndividualProject_BDT2_2021_RETAKE/results.csv"
statusPath = "/FileStore/tables/Data_IndividualProject_BDT2_2021_RETAKE/status.csv"

# COMMAND ----------

#CIRCUITS TABLE

circuits = spark\
.read\
.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.load(circuitsPath)

circuitsSchema = circuits.schema
display(circuits)

# COMMAND ----------

#CONSTRUCTORS TABLE

constructors = spark\
.read\
.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.load(constructorPath)

constructorsSchema = constructors.schema
display(constructors)

# COMMAND ----------

#CONSTRUCTOR RESULTS TABLE

constructorResult = spark\
.read\
.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.load(CRPath)

constructorResultSchema = constructorResult.schema
display(constructors)

# COMMAND ----------

#DRIVERS TABLE

drivers = spark\
.read\
.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.load(driverPath)

driversSchema = drivers.schema
display(drivers)

# COMMAND ----------

#LAPTIMES TABLES 

lapTimes = spark\
.read\
.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.load(lapTimePath)

lapTimesSchema = lapTimes.schema
display(lapTimes)

# COMMAND ----------

#PITSTOPS TABLE
pitStops = spark\
.read\
.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.load(pitStopPath)

pitStopsSchema = pitStops.schema
display(pitStops)

# COMMAND ----------

#RACES TABLE

races = spark\
.read\
.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.load(racePath)

racessSchema = races.schema
display(races)

# COMMAND ----------

#RESULTS TABLE

results = spark\
.read\
.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.load(resultPath)

resultssSchema = results.schema
display(results)

# COMMAND ----------

#STATUS TABLE

status = spark\
.read\
.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.load(statusPath)

statussSchema = status.schema
display(status)

# COMMAND ----------

# DBTITLE 1,PROCESSING DATA
#Importing the libraries

from pyspark.sql.functions import col, count, countDistinct, max, min, round, datediff, months_between, current_date, to_date, coalesce, floor, when, lit, isnan, sumDistinct, expr

from pyspark.ml import Pipeline

from pyspark.ml.feature import StringIndexer

from pyspark.sql.types import IntegerType

import pyspark.sql.functions as F

from pyspark.sql import Window

# COMMAND ----------

#We start with the drivers table

#We decide to drop all the other identification variables except the driverId
columns_to_drop = ["driverRef", "number", "code", "forename", "surname", "url", "dob"]

drivers = drivers.drop(*columns_to_drop)

# COMMAND ----------

#we rename the column to make the difference with other nationality variables

drivers = drivers.withColumnRenamed("nationality", "driverNationality")

# COMMAND ----------

#Replace the nationality by its frequency
drivers = drivers.withColumn('driverNationality', F.count('driverNationality').over(Window.partitionBy('driverNationality')))

# COMMAND ----------

#RESULTS
results = results.withColumnRenamed("laps", "NbrOfLaps")
results = results.withColumnRenamed("points", "driverPoints")
results = results.withColumnRenamed("milliseconds", "finalMilliseconds")
results = results.drop("time", "fastestLap", "positionText", "resultId")
display(results)

# COMMAND ----------

#Aggregating new variables
result1 = results.groupBy("driverId").agg(F.sum("driverPoints").alias("TotalDriverPoints"), F.mean("NbrOfLaps").alias("avgLaps"), F.mean("finalMilliseconds").alias("avgFinalTime"), F.mean("fastestLapSpeed").alias("avgFastestLapSpeed"))

result1 = result1.withColumn("avgFinalTime", F.round(result1["avgFinalTime"], 2))\
                .withColumn("avgFastestLapSpeed", F.round(result1["avgLaps"], 2))\
                .withColumn("avgLaps", F.round(result1["avgLaps"], 2))

#Creating dummies for NA of variabbles avgFinalTime and avgFastestLapSpeed
result1 = result1.withColumn('avgFinalTime_NA', when(result1.avgFinalTime.isNull(), 1).otherwise(0))

result1 = result1.withColumn('avgFastestLapSpeed_NA', when(result1.avgFastestLapSpeed.isNull(), 1).otherwise(0))

result1 = result1.fillna(0)

display(result1)

# COMMAND ----------

#Checking data types

for col in result1.dtypes:
    print(col[0]+" , "+col[1])

# COMMAND ----------

# results.createOrReplaceTempView('results')
# result2 = spark.sql(
#   """
#   select driverId, count("raceId") as finishedRace from results where statusId = 1 group by 1
  
#   """)

#count finished race per driver
result2 = results.where(col('statusId') == 1).groupBy('driverId').agg(F.count('raceId').alias('finishedRace'))

display(result2)

# COMMAND ----------

#Count number of races each driver took place
raceCount = results.groupBy('driverId').agg(F.count('raceId').alias('NbrOfRace'))

display(raceCount)

# COMMAND ----------

# Count missing fastest lap time
result3 = results.where(col('fastestLapTime').isNull()).groupBy('driverId').agg(F.count('driverId').alias('fastestLapTime_NA'))

display(result3)


# COMMAND ----------

#Counting missing final milliseconds
result4 = results.where(col('finalMilliseconds') == 'NA').groupBy('driverId').agg(F.count('driverId').alias('finalMilliseconds_NA'))

display(result4)

# COMMAND ----------

#Number of times the drivers were in rank 5 for their fastest lap
results = results.withColumn("FastestLapTop_5", when(results.rank <= 5, 1).otherwise(0))
result5 = results.where(col("fastestLapTop_5") == 1).groupBy("driverId").agg(count("driverId").alias("fastestLapRank_5"))
display(result5)

# COMMAND ----------

#Merging all the tables
result1.createOrReplaceTempView('result1')
result2.createOrReplaceTempView('result2')
result3.createOrReplaceTempView('result3')
result4.createOrReplaceTempView('result4')
result5.createOrReplaceTempView('result5')
raceCount.createOrReplaceTempView('raceCount')

resultFinal = spark.sql(
  """
  SELECT r1.driverId, r1.TotalDriverPoints, r1.avgLaps, r1.avgFinalTime, r1.avgFastestLapSpeed, r1.avgFinalTime_NA, r1.avgFastestLapSpeed_NA, r2.finishedRace, r3.fastestLapTime_NA, r4.finalMilliseconds_NA, r5.fastestLapRank_5, r6.NbrOfRace
  FROM result1 as r1 
   LEFT JOIN result2 AS r2 ON r1.driverId = r2.driverId 
   LEFT JOIN result3 AS r3 ON r1.driverId = r3.driverId 
   LEFT JOIN result4 AS r4 ON r1.driverId = r4.driverId 
   LEFT JOIN result5 AS r5 ON r1.driverId = r5.driverId
   LEFT JOIN raceCount AS r6 on r1.driverId = r6.driverId
  """
)

display(resultFinal)

# COMMAND ----------

resultFinal = resultFinal.fillna(0)

# COMMAND ----------

#Pivot table to get for each constructor the number of drivers they work with
pivotDf =  results.groupBy(['driverId', 'constructorId']).agg(F.count('driverId').alias('values'))
pivotDf = pivotDf.groupBy("driverId").pivot("constructorId").sum("values")
pivotDf = pivotDf.fillna(0)

display(pivotDf)

# COMMAND ----------

#add constructor to the column names
for val in pivotDf.columns:
  pivotDf = pivotDf.withColumnRenamed(val, "constructor_" + val)

# COMMAND ----------

display(pivotDf)

# COMMAND ----------

#merge with the new table
resultEnd = resultFinal.alias('a').join(pivotDf.alias('b'), resultFinal.driverId == pivotDf.constructor_driverId, 'left')

resultEnd = resultEnd.drop('constructor_driverId')

display(resultEnd)

# COMMAND ----------

#races and laptimes tables
new1 = races.alias('a').join(lapTimes.alias('b'), races.raceId == lapTimes.raceId, 'left').drop('b.raceId')

display(new1)

# COMMAND ----------

#Aggregate year difference
new2 = new1.withColumn('raceYearDiff', F.lit(2018) - F.col('year'))

raceLap = new2.groupBy('driverId').agg(F.max('raceYearDiff').alias('raceYearDiff'), F.mean('milliseconds').alias('avgLapTime'))

display(raceLap)

# COMMAND ----------

#Aggregate average pit stop duration
pitStops = pitStops.drop("duration", "time", "lap")


pitStops = pitStops.groupBy('driverId').agg(F.mean("milliseconds").alias('avgPitStopDuration'), F.count('stop').alias('NbrOfStops'))

pitStops = pitStops.withColumn('avgPitStopDuration', F.round(pitStops['avgPitStopDuration'],2))

display(pitStops)

# COMMAND ----------

#merge and create dummies for NAs
pitStops = pitStops.withColumnRenamed('driverId', 'driver_id')
driverPit = drivers.alias('c').join(pitStops.alias('d'), drivers.driverId ==  pitStops.driver_id, 'leftouter')

driverPit = driverPit.drop('driver_id')

driverPit = driverPit.withColumn('avgPitStopDuration_NA', when(driverPit.avgPitStopDuration.isNull(), 1).otherwise(0))\
                    .withColumn('NbrOfStops_NA', when(driverPit.NbrOfStops.isNull(),1).otherwise(0))

driverPit = driverPit.fillna(0)

display(driverPit)

# COMMAND ----------

#Merge and create dummies for NAs
raceLap = raceLap.withColumnRenamed('driverId', 'driver_id')

basetable1 = driverPit.join(raceLap, driverPit.driverId == raceLap.driver_id, 'leftouter')

basetable1 = basetable1.drop('driver_id')

basetable1 = basetable1.withColumn('raceYearDiff_NA', when(basetable1.raceYearDiff.isNull(), 1).otherwise(0))\
                  .withColumn('avgLapTime_NA', when(basetable1.avgLapTime.isNull(), 1).otherwise(0))

basetable1 = basetable1.fillna(0)

display(basetable1)

# COMMAND ----------

#Construct the basetable
resultEnd = resultEnd.withColumnRenamed('driverId', 'driver_id')

basetable = basetable1.alias('e').join(resultEnd.alias('f'), basetable1.driverId == resultEnd.driver_id, 'left')

basetable = basetable.drop('driver_id')

display(basetable)

# COMMAND ----------

#ratio of number of stops out of number of race per driver. The variable will help build the categories for the second variable
basetable = basetable.withColumn('ratioStops', F.col('NbrOfStops')/F.col('NbrOfRace'))

display(basetable)

# COMMAND ----------

#target variable. 1 if the driver finished at least 1 race
basetable = basetable.withColumn('target', when(basetable.finishedRace >= 1, 1).otherwise(0))

basetable = basetable.drop('finishedRace')

display(basetable)

# COMMAND ----------

print(basetable.select(min('ratioStops')).show())
print(basetable.select(max('ratioStops')).show())

# COMMAND ----------

#3 categories for the second model
basetable.createOrReplaceTempView('basetable')

basetable = spark.sql(
"""
select *, case when ratioStops >0 and ratioStops <1 then 0
              when ratioStops >=1 and ratioStops <2 then 1
              when ratioStops >=2 then 2 end as stopCat
from basetable
""")

basetable = basetable.fillna(0)

display(basetable)

# COMMAND ----------

#Notice the strong correlation with the target so drop to improve the performance
basetable = basetable.drop('avgFinalTime', 'avgFinalTime_NA')

# COMMAND ----------

basetable.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/df/Sample.csv")

# COMMAND ----------

# DBTITLE 1,Machine Learning Model
#SPLIT THE DATA INTO TRAIN(70%) AND TEST (30%)

#Create a train and test set with a 70% train, 30% test split

basetable_train, basetable_test = basetable.randomSplit([0.7, 0.3], seed=123)

print(basetable_train.count())

print(basetable_test.count())

# COMMAND ----------

# CREATE RFormula TRANSFORMATION FOR TRAIN, TEST and BASETABLE

#Transform the tables in a table of label, features format

from pyspark.ml.feature import RFormula

trainBig = RFormula(formula="target ~ . - driverId").fit(basetable).transform(basetable)

train = RFormula(formula="target ~ . - driverId").fit(basetable_train).transform(basetable_train)

test = RFormula(formula="target ~ . - driverId").fit(basetable_test).transform(basetable_test)

print("trainBig nobs: " + str(trainBig.count()))

print("train nobs: " + str(train.count()))

print("test nobs: " + str(test.count()))

# COMMAND ----------

train.columns

# COMMAND ----------

# REMOVE ALL COLUMNS FROM THE DATA. ONLY COLUMNS IN THE DATA ARE 'FEATURES' AND 'LABEL'

trainBig = trainBig.drop('driverNationality','avgPitStopDuration','NbrOfStops','avgPitStopDuration_NA','NbrOfStops_NA','raceYearDiff','avgLapTime','raceYearDiff_NA','avgLapTime_NA','TotalDriverPoints','avgLaps','avgFastestLapSpeed','avgFastestLapSpeed_NA','fastestLapTime_NA','finalMilliseconds_NA','fastestLapRank_5','NbrOfRace','constructor_1','constructor_2','constructor_3','constructor_4','constructor_5','constructor_6','constructor_7','constructor_8','constructor_9','constructor_10','constructor_11','constructor_12','constructor_13','constructor_14','constructor_15','constructor_16','constructor_17','constructor_18','constructor_19','constructor_20','constructor_21','constructor_22','constructor_23','constructor_24','constructor_25','constructor_26','constructor_27','constructor_28','constructor_29','constructor_30','constructor_31','constructor_32','constructor_33','constructor_34','constructor_35','constructor_36','constructor_37','constructor_38','constructor_39','constructor_40','constructor_41','constructor_42','constructor_44','constructor_45','constructor_46','constructor_47','constructor_48','constructor_49','constructor_50','constructor_51','constructor_52','constructor_53','constructor_54','constructor_55','constructor_56','constructor_58','constructor_59','constructor_60','constructor_61','constructor_62','constructor_63','constructor_64','constructor_65','constructor_66','constructor_67','constructor_68','constructor_69','constructor_70','constructor_71','constructor_72','constructor_73','constructor_74','constructor_75','constructor_76','constructor_77','constructor_78','constructor_79','constructor_80','constructor_81','constructor_82','constructor_83','constructor_84','constructor_85','constructor_86','constructor_87','constructor_89','constructor_90','constructor_91','constructor_92','constructor_93','constructor_94','constructor_95','constructor_96','constructor_97','constructor_98','constructor_99','constructor_100','constructor_101','constructor_102','constructor_103','constructor_104','constructor_105','constructor_106','constructor_107','constructor_108','constructor_109','constructor_110','constructor_111','constructor_112','constructor_113','constructor_114','constructor_115','constructor_116','constructor_117','constructor_118','constructor_119','constructor_120','constructor_121','constructor_122','constructor_123','constructor_124','constructor_125','constructor_126','constructor_127','constructor_128','constructor_129','constructor_130','constructor_131','constructor_132','constructor_133','constructor_134','constructor_135','constructor_136','constructor_137','constructor_138','constructor_139','constructor_140','constructor_141','constructor_142','constructor_143','constructor_144','constructor_145','constructor_146','constructor_147','constructor_148','constructor_149','constructor_150','constructor_151','constructor_152','constructor_153','constructor_154','constructor_155','constructor_156','constructor_157','constructor_158','constructor_159','constructor_160','constructor_161','constructor_162','constructor_163','constructor_164','constructor_166','constructor_167','constructor_168','constructor_169','constructor_170','constructor_171','constructor_172','constructor_173','constructor_174','constructor_175','constructor_176','constructor_177','constructor_178','constructor_179','constructor_180','constructor_181','constructor_182','constructor_183','constructor_184','constructor_185','constructor_186','constructor_187','constructor_188','constructor_189','constructor_190','constructor_191','constructor_192','constructor_193','constructor_194','constructor_195','constructor_196','constructor_197','constructor_198','constructor_199','constructor_200','constructor_201','constructor_202','constructor_203','constructor_204','constructor_205','constructor_206','constructor_207','constructor_208','constructor_209','constructor_210','ratioStops','stopCat','driverId','target')

train = train.drop('driverNationality','avgPitStopDuration','NbrOfStops','avgPitStopDuration_NA','NbrOfStops_NA','raceYearDiff','avgLapTime','raceYearDiff_NA','avgLapTime_NA','TotalDriverPoints','avgLaps','avgFastestLapSpeed','avgFastestLapSpeed_NA','fastestLapTime_NA','finalMilliseconds_NA','fastestLapRank_5','NbrOfRace','constructor_1','constructor_2','constructor_3','constructor_4','constructor_5','constructor_6','constructor_7','constructor_8','constructor_9','constructor_10','constructor_11','constructor_12','constructor_13','constructor_14','constructor_15','constructor_16','constructor_17','constructor_18','constructor_19','constructor_20','constructor_21','constructor_22','constructor_23','constructor_24','constructor_25','constructor_26','constructor_27','constructor_28','constructor_29','constructor_30','constructor_31','constructor_32','constructor_33','constructor_34','constructor_35','constructor_36','constructor_37','constructor_38','constructor_39','constructor_40','constructor_41','constructor_42','constructor_44','constructor_45','constructor_46','constructor_47','constructor_48','constructor_49','constructor_50','constructor_51','constructor_52','constructor_53','constructor_54','constructor_55','constructor_56','constructor_58','constructor_59','constructor_60','constructor_61','constructor_62','constructor_63','constructor_64','constructor_65','constructor_66','constructor_67','constructor_68','constructor_69','constructor_70','constructor_71','constructor_72','constructor_73','constructor_74','constructor_75','constructor_76','constructor_77','constructor_78','constructor_79','constructor_80','constructor_81','constructor_82','constructor_83','constructor_84','constructor_85','constructor_86','constructor_87','constructor_89','constructor_90','constructor_91','constructor_92','constructor_93','constructor_94','constructor_95','constructor_96','constructor_97','constructor_98','constructor_99','constructor_100','constructor_101','constructor_102','constructor_103','constructor_104','constructor_105','constructor_106','constructor_107','constructor_108','constructor_109','constructor_110','constructor_111','constructor_112','constructor_113','constructor_114','constructor_115','constructor_116','constructor_117','constructor_118','constructor_119','constructor_120','constructor_121','constructor_122','constructor_123','constructor_124','constructor_125','constructor_126','constructor_127','constructor_128','constructor_129','constructor_130','constructor_131','constructor_132','constructor_133','constructor_134','constructor_135','constructor_136','constructor_137','constructor_138','constructor_139','constructor_140','constructor_141','constructor_142','constructor_143','constructor_144','constructor_145','constructor_146','constructor_147','constructor_148','constructor_149','constructor_150','constructor_151','constructor_152','constructor_153','constructor_154','constructor_155','constructor_156','constructor_157','constructor_158','constructor_159','constructor_160','constructor_161','constructor_162','constructor_163','constructor_164','constructor_166','constructor_167','constructor_168','constructor_169','constructor_170','constructor_171','constructor_172','constructor_173','constructor_174','constructor_175','constructor_176','constructor_177','constructor_178','constructor_179','constructor_180','constructor_181','constructor_182','constructor_183','constructor_184','constructor_185','constructor_186','constructor_187','constructor_188','constructor_189','constructor_190','constructor_191','constructor_192','constructor_193','constructor_194','constructor_195','constructor_196','constructor_197','constructor_198','constructor_199','constructor_200','constructor_201','constructor_202','constructor_203','constructor_204','constructor_205','constructor_206','constructor_207','constructor_208','constructor_209','constructor_210','ratioStops','stopCat','driverId','target')

test = test.drop('driverNationality','avgPitStopDuration','NbrOfStops','avgPitStopDuration_NA','NbrOfStops_NA','raceYearDiff','avgLapTime','raceYearDiff_NA','avgLapTime_NA','TotalDriverPoints','avgLaps','avgFastestLapSpeed','avgFastestLapSpeed_NA','fastestLapTime_NA','finalMilliseconds_NA','fastestLapRank_5','NbrOfRace','constructor_1','constructor_2','constructor_3','constructor_4','constructor_5','constructor_6','constructor_7','constructor_8','constructor_9','constructor_10','constructor_11','constructor_12','constructor_13','constructor_14','constructor_15','constructor_16','constructor_17','constructor_18','constructor_19','constructor_20','constructor_21','constructor_22','constructor_23','constructor_24','constructor_25','constructor_26','constructor_27','constructor_28','constructor_29','constructor_30','constructor_31','constructor_32','constructor_33','constructor_34','constructor_35','constructor_36','constructor_37','constructor_38','constructor_39','constructor_40','constructor_41','constructor_42','constructor_44','constructor_45','constructor_46','constructor_47','constructor_48','constructor_49','constructor_50','constructor_51','constructor_52','constructor_53','constructor_54','constructor_55','constructor_56','constructor_58','constructor_59','constructor_60','constructor_61','constructor_62','constructor_63','constructor_64','constructor_65','constructor_66','constructor_67','constructor_68','constructor_69','constructor_70','constructor_71','constructor_72','constructor_73','constructor_74','constructor_75','constructor_76','constructor_77','constructor_78','constructor_79','constructor_80','constructor_81','constructor_82','constructor_83','constructor_84','constructor_85','constructor_86','constructor_87','constructor_89','constructor_90','constructor_91','constructor_92','constructor_93','constructor_94','constructor_95','constructor_96','constructor_97','constructor_98','constructor_99','constructor_100','constructor_101','constructor_102','constructor_103','constructor_104','constructor_105','constructor_106','constructor_107','constructor_108','constructor_109','constructor_110','constructor_111','constructor_112','constructor_113','constructor_114','constructor_115','constructor_116','constructor_117','constructor_118','constructor_119','constructor_120','constructor_121','constructor_122','constructor_123','constructor_124','constructor_125','constructor_126','constructor_127','constructor_128','constructor_129','constructor_130','constructor_131','constructor_132','constructor_133','constructor_134','constructor_135','constructor_136','constructor_137','constructor_138','constructor_139','constructor_140','constructor_141','constructor_142','constructor_143','constructor_144','constructor_145','constructor_146','constructor_147','constructor_148','constructor_149','constructor_150','constructor_151','constructor_152','constructor_153','constructor_154','constructor_155','constructor_156','constructor_157','constructor_158','constructor_159','constructor_160','constructor_161','constructor_162','constructor_163','constructor_164','constructor_166','constructor_167','constructor_168','constructor_169','constructor_170','constructor_171','constructor_172','constructor_173','constructor_174','constructor_175','constructor_176','constructor_177','constructor_178','constructor_179','constructor_180','constructor_181','constructor_182','constructor_183','constructor_184','constructor_185','constructor_186','constructor_187','constructor_188','constructor_189','constructor_190','constructor_191','constructor_192','constructor_193','constructor_194','constructor_195','constructor_196','constructor_197','constructor_198','constructor_199','constructor_200','constructor_201','constructor_202','constructor_203','constructor_204','constructor_205','constructor_206','constructor_207','constructor_208','constructor_209','constructor_210','ratioStops','stopCat','driverId','target')

# COMMAND ----------

# DBTITLE 1,Logistic Regression
# FIT LOGISTIC REGRESSION

#Train a Logistic Regression model
from pyspark.ml.classification import LogisticRegression

#Define the algorithm class
lr = LogisticRegression()

#Fit the model
lrModel = lr.fit(train)

# COMMAND ----------

predictions = lrModel.transform(test)

from pyspark.ml.evaluation import BinaryClassificationEvaluator

# COMMAND ----------

# Predict labels of test set using built model
preds = predictions.select("prediction", "label")
preds.show(10)

#Get model performance on test set
from pyspark.mllib.evaluation import BinaryClassificationMetrics

out = preds.rdd.map(lambda x: (float(x[0]), float(x[1])))
metrics = BinaryClassificationMetrics(out)

print(metrics.areaUnderPR)  #area under precision/recall curve
print(metrics.areaUnderROC) #area under Receiver Operating Characteristic curve

#AUC 0.849

# COMMAND ----------

# ROC CURVE ON TRAIN DATA

display(lrModel, train, "ROC")

# COMMAND ----------

# ROC CURVE ON TEST DATA

display(lrModel, test, "ROC")

# COMMAND ----------

# HYPERPARAMETER TUNING FOR LOGISTIC REGRESSION

from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

#Define pipeline
lr = LogisticRegression()
pipeline = Pipeline().setStages([lr])

#Set param grid
params = ParamGridBuilder()\
  .addGrid(lr.regParam, [0.1, 0.01])\
  .addGrid(lr.maxIter, [50, 100,150])\
  .build()

#Evaluator: uses max(AUC) by default to get the final model
evaluator = BinaryClassificationEvaluator()
#Check params through: evaluator.explainParams()

#Cross-validation of entire pipeline
cv = CrossValidator()\
  .setEstimator(pipeline)\
  .setEstimatorParamMaps(params)\
  .setEvaluator(evaluator)\
  .setNumFolds(2) # Here: 10-fold cross validation

# COMMAND ----------

#Run cross-validation, and choose the best set of parameters.
#Spark automatically saves the best model in cvModel.
#cvModel = cv.fit(train)

#preds2 = cvModel.transform(test)\
#  .select("prediction", "label")
#preds2.show(10)

#Get model performance on test set

#out = preds2.rdd.map(lambda x: (float(x[0]), float(x[1])))
#metrics = BinaryClassificationMetrics(out)

#print(metrics.areaUnderPR)  #area under precision/recall curve
#print(metrics.areaUnderROC) #area under Receiver Operating Characteristic curve

# COMMAND ----------

#display(cvModel, test, "ROC")

# COMMAND ----------

# DBTITLE 1,Gradient Boosting
# FIT GRADIENT BOOSTING

from pyspark.ml.regression import GBTRegressor
gbt = GBTRegressor()
gbtModel = gbt.fit(train)

# COMMAND ----------

# CREATE PREDICTIONS

preds = gbtModel.transform(test).select("prediction", "label")
preds.show(10)

# COMMAND ----------

# EVALUATE PERFORMANCE ON AUC

#Get model performance on test set
from pyspark.mllib.evaluation import BinaryClassificationMetrics

out = preds.rdd.map(lambda x: (float(x[0]), float(x[1])))
metrics = BinaryClassificationMetrics(out)

print(metrics.areaUnderPR)  #area under precision/recall curve
print(metrics.areaUnderROC) #area under Receiver Operating Characteristic curve

# AUC : 0.96

# COMMAND ----------

gbtModel.featureImportances

# COMMAND ----------

def ExtractFeatureImp(featureImp, dataset, featuresCol):
    list_extract = []
    for i in dataset.schema[featuresCol].metadata["ml_attr"]["attrs"]:
        list_extract = list_extract + dataset.schema[featuresCol].metadata["ml_attr"]["attrs"][i]
    varlist = pd.DataFrame(list_extract)
    varlist['score'] = varlist['idx'].apply(lambda x: featureImp[x])
    return(varlist.sort_values('score', ascending = False))

# COMMAND ----------

preds_all = gbtModel.transform(test)

# COMMAND ----------

preds_all_pd = preds_all.toPandas()
basetable_test = basetable_test.toPandas()

preds_all_pd.columns = ['Id','features', 'probability', 'preditions']
display(preds_all_pd)

# COMMAND ----------

#top 20 in feature importance
import pandas as pd
varlist = ExtractFeatureImp(gbtModel.featureImportances, preds_all, "features")
varlist

# COMMAND ----------

print((preds_all.count(), len(preds_all.columns)))
print((basetable_test.count(), len(basetable_test.columns)))

#merge predictions with basetable
merged = pd.concat([basetable_test, preds_all_pd], axis=1)
#merge predictions with original drivers table in order to get original categories

# DRIVERS

drivers = spark\
.read\
.format("json")\
.option("header","true")\
.option("inferSchema","true")\
.load(driverPath)

driversSchema = drivers.schema
display(drivers.limit(5))

original_plus_predictions = pd.merge(drivers.toPandas(), merged, how="inner", on="driverId")
#merge predictions with original base table in order to have the features into separate columns
basetable_plus_predictions = pd.merge(basetable.toPandas(), merged, how="inner", on="driverId")

# COMMAND ----------

#calculate total drivers that will and will not finish the race 
total0 = len(basetable_plus_predictions[basetable_plus_predictions['target'] == 0])
total1 = len(basetable_plus_predictions[basetable_plus_predictions['target'] == 1])

# COMMAND ----------

# DBTITLE 1,10 Fold Cross-Validation using Logistic Regression
# CROSS-VALIDATION ON 10 FOLDS

#Spark offers two options for performing hyperparameter tuning automatically:

#1. TrainValidationSplit: randomly split data in 2 groups
from pyspark.ml.tuning import TrainValidationSplit

#2. CrossValidator: k-fold cross-validation by splitting the data into k non-overlapping, randomly partitioned folds
from pyspark.ml.tuning import CrossValidator

#First method is good for quick model evaluations, 2nd method is recommended for a more rigorous model evaluation.

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


lr = LinearRegression(maxIter=5, solver="l-bfgs")


modelEvaluator=RegressionEvaluator()
pipeline = Pipeline(stages=[lr])
paramGrid = ParamGridBuilder().addGrid(lr.regParam, [0.01]).addGrid(lr.elasticNetParam, [0, 1]).build()

crossval = CrossValidator(estimator=lr,
                          estimatorParamMaps=paramGrid,
                          evaluator=modelEvaluator,
                          numFolds=10)

# COMMAND ----------

# CREATE LABEL COLUMN FOR TARGET VARIABLE

# Convert label into label indices using the StringIndexer
from pyspark.ml.feature import StringIndexer, VectorAssembler

stages = []

label_stringIdx = StringIndexer(inputCol="target", outputCol="label")
stages += [label_stringIdx]

# COMMAND ----------

# CREATING ALL COLUMNS AS FEATURES IN A VECTOR 

from pyspark.ml.feature import StringIndexer, VectorAssembler


numericCols = ['driverNationality','avgPitStopDuration','NbrOfStops','avgPitStopDuration_NA','NbrOfStops_NA','raceYearDiff','avgLapTime','raceYearDiff_NA','avgLapTime_NA','TotalDriverPoints','avgLaps','avgFastestLapSpeed','avgFastestLapSpeed_NA','fastestLapTime_NA','finalMilliseconds_NA','fastestLapRank_5','NbrOfRace','constructor_1','constructor_2','constructor_3','constructor_4','constructor_5','constructor_6','constructor_7','constructor_8','constructor_9','constructor_10','constructor_11','constructor_12','constructor_13','constructor_14','constructor_15','constructor_16','constructor_17','constructor_18','constructor_19','constructor_20','constructor_21','constructor_22','constructor_23','constructor_24','constructor_25','constructor_26','constructor_27','constructor_28','constructor_29','constructor_30','constructor_31','constructor_32','constructor_33','constructor_34','constructor_35','constructor_36','constructor_37','constructor_38','constructor_39','constructor_40','constructor_41','constructor_42','constructor_44','constructor_45','constructor_46','constructor_47','constructor_48','constructor_49','constructor_50','constructor_51','constructor_52','constructor_53','constructor_54','constructor_55','constructor_56','constructor_58','constructor_59','constructor_60','constructor_61','constructor_62','constructor_63','constructor_64','constructor_65','constructor_66','constructor_67','constructor_68','constructor_69','constructor_70','constructor_71','constructor_72','constructor_73','constructor_74','constructor_75','constructor_76','constructor_77','constructor_78','constructor_79','constructor_80','constructor_81','constructor_82','constructor_83','constructor_84','constructor_85','constructor_86','constructor_87','constructor_89','constructor_90','constructor_91','constructor_92','constructor_93','constructor_94','constructor_95','constructor_96','constructor_97','constructor_98','constructor_99','constructor_100','constructor_101','constructor_102','constructor_103','constructor_104','constructor_105','constructor_106','constructor_107','constructor_108','constructor_109','constructor_110','constructor_111','constructor_112','constructor_113','constructor_114','constructor_115','constructor_116','constructor_117','constructor_118','constructor_119','constructor_120','constructor_121','constructor_122','constructor_123','constructor_124','constructor_125','constructor_126','constructor_127','constructor_128','constructor_129','constructor_130','constructor_131','constructor_132','constructor_133','constructor_134','constructor_135','constructor_136','constructor_137','constructor_138','constructor_139','constructor_140','constructor_141','constructor_142','constructor_143','constructor_144','constructor_145','constructor_146','constructor_147','constructor_148','constructor_149','constructor_150','constructor_151','constructor_152','constructor_153','constructor_154','constructor_155','constructor_156','constructor_157','constructor_158','constructor_159','constructor_160','constructor_161','constructor_162','constructor_163','constructor_164','constructor_166','constructor_167','constructor_168','constructor_169','constructor_170','constructor_171','constructor_172','constructor_173','constructor_174','constructor_175','constructor_176','constructor_177','constructor_178','constructor_179','constructor_180','constructor_181','constructor_182','constructor_183','constructor_184','constructor_185','constructor_186','constructor_187','constructor_188','constructor_189','constructor_190','constructor_191','constructor_192','constructor_193','constructor_194','constructor_195','constructor_196','constructor_197','constructor_198','constructor_199','constructor_200','constructor_201','constructor_202','constructor_203','constructor_204','constructor_205','constructor_206','constructor_207','constructor_208','constructor_209','constructor_210','ratioStops','stopCat']

assemblerInputs = numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]

# COMMAND ----------

# CREATE PIPELINE
# Training the model
  
partialPipeline = Pipeline().setStages(stages)
pipelineModel = partialPipeline.fit(basetable)
preppedDataDF = pipelineModel.transform(basetable)

#preppedDataDF = preppedDataDF.drop(numericCols)

# COMMAND ----------

preppedDataDF = preppedDataDF.drop('driverNationality','avgPitStopDuration','NbrOfStops','avgPitStopDuration_NA','NbrOfStops_NA','raceYearDiff','avgLapTime','raceYearDiff_NA','avgLapTime_NA','TotalDriverPoints','avgLaps','avgFastestLapSpeed','avgFastestLapSpeed_NA','fastestLapTime_NA','finalMilliseconds_NA','fastestLapRank_5','NbrOfRace','constructor_1','constructor_2','constructor_3','constructor_4','constructor_5','constructor_6','constructor_7','constructor_8','constructor_9','constructor_10','constructor_11','constructor_12','constructor_13','constructor_14','constructor_15','constructor_16','constructor_17','constructor_18','constructor_19','constructor_20','constructor_21','constructor_22','constructor_23','constructor_24','constructor_25','constructor_26','constructor_27','constructor_28','constructor_29','constructor_30','constructor_31','constructor_32','constructor_33','constructor_34','constructor_35','constructor_36','constructor_37','constructor_38','constructor_39','constructor_40','constructor_41','constructor_42','constructor_44','constructor_45','constructor_46','constructor_47','constructor_48','constructor_49','constructor_50','constructor_51','constructor_52','constructor_53','constructor_54','constructor_55','constructor_56','constructor_58','constructor_59','constructor_60','constructor_61','constructor_62','constructor_63','constructor_64','constructor_65','constructor_66','constructor_67','constructor_68','constructor_69','constructor_70','constructor_71','constructor_72','constructor_73','constructor_74','constructor_75','constructor_76','constructor_77','constructor_78','constructor_79','constructor_80','constructor_81','constructor_82','constructor_83','constructor_84','constructor_85','constructor_86','constructor_87','constructor_89','constructor_90','constructor_91','constructor_92','constructor_93','constructor_94','constructor_95','constructor_96','constructor_97','constructor_98','constructor_99','constructor_100','constructor_101','constructor_102','constructor_103','constructor_104','constructor_105','constructor_106','constructor_107','constructor_108','constructor_109','constructor_110','constructor_111','constructor_112','constructor_113','constructor_114','constructor_115','constructor_116','constructor_117','constructor_118','constructor_119','constructor_120','constructor_121','constructor_122','constructor_123','constructor_124','constructor_125','constructor_126','constructor_127','constructor_128','constructor_129','constructor_130','constructor_131','constructor_132','constructor_133','constructor_134','constructor_135','constructor_136','constructor_137','constructor_138','constructor_139','constructor_140','constructor_141','constructor_142','constructor_143','constructor_144','constructor_145','constructor_146','constructor_147','constructor_148','constructor_149','constructor_150','constructor_151','constructor_152','constructor_153','constructor_154','constructor_155','constructor_156','constructor_157','constructor_158','constructor_159','constructor_160','constructor_161','constructor_162','constructor_163','constructor_164','constructor_166','constructor_167','constructor_168','constructor_169','constructor_170','constructor_171','constructor_172','constructor_173','constructor_174','constructor_175','constructor_176','constructor_177','constructor_178','constructor_179','constructor_180','constructor_181','constructor_182','constructor_183','constructor_184','constructor_185','constructor_186','constructor_187','constructor_188','constructor_189','constructor_190','constructor_191','constructor_192','constructor_193','constructor_194','constructor_195','constructor_196','constructor_197','constructor_198','constructor_199','constructor_200','constructor_201','constructor_202','constructor_203','constructor_204','constructor_205','constructor_206','constructor_207','constructor_208','constructor_209','constructor_210','ratioStops','stopCat')

# COMMAND ----------

preppedDataDF = preppedDataDF.drop('target')
preppedDataDF = preppedDataDF.drop('driverId')

# COMMAND ----------

# DBTITLE 1,Random Forest
# RANDOM FOREST ALGORTIHM

from pyspark.ml.classification import RandomForestClassifier
rf = RandomForestClassifier(numTrees=20, maxDepth=30, labelCol="label", seed=42)
model = rf.fit(train)

# COMMAND ----------

predictions = model.transform(test)

# COMMAND ----------

# Predict labels of test set using built model

from pyspark.mllib.evaluation import BinaryClassificationMetrics

preds = model.transform(test).select("prediction", "label")
preds.show(10)

#Get model performance on test set
out = preds.rdd.map(lambda x: (float(x[0]), float(x[1])))
metrics = BinaryClassificationMetrics(out)

print(metrics.areaUnderPR) #area under precision/recall curve
print(metrics.areaUnderROC)#area under Receiver Operating Characteristic curve

# AUC : 0.90

# COMMAND ----------

# CROSS-VALIDATION AND HYPER-PARAMETER TUNING FOR RANDOM FOREST

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

rfc = RandomForestClassifier()
rfPipe = Pipeline().setStages([rfc])

rfParams = ParamGridBuilder()\
  .addGrid(rfc.numTrees, [150, 300, 500])\
  .build()

rfCv = CrossValidator()\
  .setEstimator(rfPipe)\
  .setEstimatorParamMaps(rfParams)\
  .setEvaluator(BinaryClassificationEvaluator())\
  .setNumFolds(2) # Here: 5-fold cross validation

#Run cross-validation, and choose the best set of parameters.
#rfcModel = rfCv.fit(train)

# COMMAND ----------

#CREATING A LIST OF MOST IMPORTANT FEATURES

def ExtractFeatureImp(featureImp, dataset, featuresCol):
    list_extract = []
    for i in dataset.schema[featuresCol].metadata["ml_attr"]["attrs"]:
        list_extract = list_extract + dataset.schema[featuresCol].metadata["ml_attr"]["attrs"][i]
    varlist = pd.DataFrame(list_extract)
    varlist['score'] = varlist['idx'].apply(lambda x: featureImp[x])
    return(varlist.sort_values('score', ascending = False))
  
import pandas as pd
varlist = ExtractFeatureImp(model.featureImportances, predictions, rf.getFeaturesCol())

# COMMAND ----------

# LIST OF MOST IMPORTANT FEATURES

varlist.head(15)

# COMMAND ----------

# DBTITLE 1,Decision Trees
from pyspark.ml.classification import DecisionTreeClassifier
dt = DecisionTreeClassifier()
dtModel = dt.fit(train)

# COMMAND ----------

# Predict labels of test set using built model
preds = dtModel.transform(test).select("prediction", "label")
preds.show(10)

#Get model performance on test set
out = preds.rdd.map(lambda x: (float(x[0]), float(x[1])))
metrics = BinaryClassificationMetrics(out)

print(metrics.areaUnderPR)  #area under precision/recall curve
print(metrics.areaUnderROC) #area under Receiver Operating Characteristic curve

#AUC 0.91

# COMMAND ----------

# HYPER-PARAMETER TUNING AND CROSS-VALIDATION FOR DECISION TREES

dtparamGrid = (ParamGridBuilder()
             .addGrid(dt.maxDepth, [2, 5, 10, 20, 30])
             .addGrid(dt.maxBins, [10, 20, 40, 80, 100])
             .build())


dtcv = CrossValidator(estimator = dt,
                      estimatorParamMaps = dtparamGrid,
                      evaluator = BinaryClassificationEvaluator(),
                      numFolds = 3)

# COMMAND ----------

# dtcvModel = dtcv.fit(train)

# COMMAND ----------

# # Predict labels of test set using built model
# dtpredictions2 = dtcvModel.transform(test).select("prediction", "label")
# dtpredictions2.show(10)


# #Get model performance on test set
# out = dtpredictions2.rdd.map(lambda x: (float(x[0]), float(x[1])))
# metrics = BinaryClassificationMetrics(out)

# print(metrics.areaUnderPR) #area under precision/recall curve
# print(metrics.areaUnderROC)#area under Receiver Operating Characteristic curve

# COMMAND ----------

# DBTITLE 1,Deep Learning
#Transform to numpy arrays
import numpy as np
x = np.array(basetable.drop("stopCat","driverId").collect())
y = np.array(basetable.select("stopCat").collect())

# COMMAND ----------

#Convert y to an appropriate format
y = np.asarray(y).astype('float32')

# COMMAND ----------

#Split in a train, val, and test set
from sklearn.model_selection import train_test_split
x_trainBig, x_test, y_trainBig, y_test = train_test_split(x, y, test_size=0.25, random_state=123)
x_train, x_val, y_train, y_val = train_test_split(x_trainBig, y_trainBig, test_size=0.50, random_state=123)

# COMMAND ----------

#Standardize the data
from sklearn.preprocessing import StandardScaler
x_train = StandardScaler().fit(x_train).transform(x_train)
x_val = StandardScaler().fit(x_val).transform(x_val)
x_test = StandardScaler().fit(x_test).transform(x_test)

# COMMAND ----------

#Estimate a DL model with 2 layers of 16 units and 1 output layer.
from tensorflow.keras import models
from tensorflow.keras import layers

#Define the model
model = models.Sequential()

#Define the layers
model.add(layers.Dense(16, activation='relu', input_shape=(x_train.shape[1],)))
model.add(layers.Dense(16, activation='relu'))
model.add(layers.Dense(3, activation='softmax'))

# COMMAND ----------

#Define an optimizer, loss function, and metric for success
model.compile(optimizer='rmsprop',
  loss='sparse_categorical_crossentropy',
  metrics=['acc'])

#Fit the model
history = model.fit(x_train,
  y_train,
  epochs=40,
  batch_size=64,
  validation_data=(x_val, y_val))

# COMMAND ----------

#Estimate a DL model with 5 layers of 16 units and 1 output layer
model = models.Sequential()
model.add(layers.Dense(16, activation='relu', input_shape=(x_train.shape[1],)))
model.add(layers.Dense(16, activation='relu'))
model.add(layers.Dense(16, activation='relu'))
model.add(layers.Dense(16, activation='relu'))
model.add(layers.Dense(16, activation='relu'))
model.add(layers.Dense(3, activation='softmax'))

model.compile(optimizer='rmsprop',
  loss='sparse_categorical_crossentropy',
  metrics=['acc'])

model.fit(x_train,
  y_train,
  epochs=40,
  batch_size=64,
  validation_data=(x_val, y_val))

# results_exc2 = model.evaluate(x_test, y_test)
