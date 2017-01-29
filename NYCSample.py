from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import *
from dateutil.parser import parse
import pandas as pd 
import pyspark_csv as pycsv
from pyspark.sql.functions import lit, year, dayofmonth, month
import calendar
from time import strftime
from datetime import datetime
from pyspark.sql.functions import udf, count, monotonicallyIncreasingId
from pyspark.sql import SQLContext
from bokeh.charts import defaults, vplot, hplot, Histogram,Bar, output_file, show
sc = SparkContext()
from collections import OrderedDict
from math import log, sqrt

import numpy as np
import pandas as pd
from six.moves import cStringIO as StringIO

from bokeh.plotting import figure, show, output_file

#for plotting burtin graph
from collections import OrderedDict
from math import log, sqrt

import numpy as np


from bokeh.plotting import figure, show, output_file

from bokeh.charts import Bar, output_file, show
from bokeh.charts.attributes import cat, color, CatAttr
from bokeh.charts.operations import blend
from bokeh.charts.utils import df_from_json


sqlContext = SQLContext(sc)


#####For loading the trip data
df1 = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('/home/jay/Documents/NYCTaxi/trip/*')
df1 = sqlContext.read.load('/home/jay/Documents/NYCTaxi/trip/*',                          format='com.databricks.spark.csv', 
                          header='true', 
                          inferSchema='true')

   
month_converter = udf(lambda month_number:datetime(2012,int(month_number), 1).strftime("%b"))





taxi_temp = df1.map(lambda p: (p[0], p[5])).sortByKey(True)

#By year sorting the medallion for total trips

'''
Taxi_year = sqlContext.createDataFrame(taxi_temp, ["medallion","time"])
Taxi_year[0].dataType = StringType()
Taxi_year[1].dataType = TimestampType()
year_temp = Taxi_year.select("medallion",year("time")).rdd
trip_count_year = sqlContext.createDataFrame(year_temp, ["medallion","year"])
trip_count_year.registerTempTable("YearTrip")
print "######################number of trips by medallion for an year"
groupedyear = sqlContext.sql("SELECT year, COUNT(medallion) AS YearCount from YearTrip GROUP BY year ORDER BY year").toPandas()

groupedyear_G = Bar(groupedyear, 'year', values='YearCount',
        title="Total Trips Grouped by Year", color="green", bar_width=0.4)


'''


#By month  sorting the medallion for total trips

Taxi_month = sqlContext.createDataFrame(taxi_temp, ["medallion","time"])
Taxi_month[0].dataType = StringType()
Taxi_month[1].dataType = TimestampType()
month_temp = Taxi_month.select("medallion",month("time")).rdd
trip_count_month = sqlContext.createDataFrame(month_temp, ["medallion","monthID"])

temp = trip_count_month.na.drop('any')


temp_month = temp.withColumn("month_name", month_converter(temp.monthID))

temp_month.registerTempTable("MonthTrip")

trip_count = sqlContext.sql("SELECT medallion, COUNT(medallion) AS COUNT FROM MonthTrip GROUP BY medallion ORDER BY COUNT DESC LIMIT 5").toPandas()

groupedmonth = sqlContext.sql("SELECT month_name AS Month, COUNT(medallion) AS MONTHCOUNT, first_value(monthID) AS monthID from MonthTrip GROUP BY month_name ORDER BY monthID ASC LIMIT 5").toPandas()




trip_count_G= Bar(trip_count,label=CatAttr(columns=['medallion'], sort=False), values='COUNT',
        title="Top 5 Max trip counts", color="black", bar_width=0.4)

groupedmonth_G = Bar(groupedmonth, label=CatAttr(columns=['Month'], sort=False), values='MONTHCOUNT',
        title="Total Trips Grouped by Month", color="red",bar_width=0.4)

output_file("histogram.html")

show(vplot(trip_count_G, groupedmonth_G))




######For loading the fare data 

df2 = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('/home/jay/Documents/NYCTaxi/fare/*')

df2 = sqlContext.read.load('/home/jay/Documents/NYCTaxi/fare/*',                          format='com.databricks.spark.csv', 
                          header='true', 
                          inferSchema='true')




#####consoildated fare, tip, amount for each medallion

df2 = df2.withColumnRenamed(" payment_type", "payment_type").withColumnRenamed(" fare_amount", "fare_amount").withColumnRenamed(" tip_amount", "tip_amount").withColumnRenamed(" total_amount", "total_amount")
Taxi_Fare_Tip=df2.select("medallion",col("fare_amount").alias("Fare"), col("tip_amount").alias("Tip"),col("total_amount").alias("TotalAmount"),"payment_type")
Taxi_Fare_Tip.registerTempTable("TotalFareTip")
print "##################Total tip for each medallion"
total_tip = sqlContext.sql("SELECT medallion, SUM(Fare) AS TotalFare , SUM(Tip) AS TotalTip, SUM(TotalAmount) AS TotalAmount, first_value(payment_type) AS Payment FROM TotalFareTip  GROUP BY medallion ORDER BY TotalAmount DESC LIMIT 6").toPandas()

fare = Bar(total_tip,label=CatAttr(columns=['medallion'], sort=False), values='TotalFare',
        title="Sum of fare for each medallion", color="black", bar_width=0.4)

tip = Bar(total_tip,label=CatAttr(columns=['medallion'], sort=False), values='TotalTip',
        title="Sum of Tips for each medallion", color="#FB8B73",bar_width=0.4)

amount = Bar(total_tip, label=CatAttr(columns=['medallion'], sort=False), values='TotalAmount',
        title="Sum of Amount for each medallion", color="violet",bar_width=0.4)
            
output_file("bar.html")

show(vplot(fare, tip, amount))
















######Trip distance, payment type and total_amount
'''
###Join table for consolidated output
df1.registerTempTable('TripTable')
df2.registerTempTable('FareTable')

#TaxiAll = sqlContext.sql("select TripTable.medallion, TripTable.trip_distance, FareTable.tip_amount, FareTable.total_amount, FareTable.medallion AS med from TripTable, FareTable JOIN FareTable ON TripTable.medallion = FareTable.medallion").drop("med").show()
TaxiAll = df1.join(df2, 'medallion').select("medallion","trip_distance"," tip_amount","total_amount").show(2)
#print TaxiAll.take(5)
#TaxiAll.registerTempTable('TaxiJoined')
#sqlContext.sql('SELECT medallion,trip_distance,tip_amount, total_amount  FROM TaxiJoined').show()
'''

