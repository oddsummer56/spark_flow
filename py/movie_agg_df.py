from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("aggDF").getOrCreate()


df=spark.read.parquet(f"/home/oddsummer/data/movie/hive")
df.createOrReplaceTempView("movie")

#국내/해외영화별 매출액, 관객수 집계
df1= spark.sql(f"""
                SELECT load_dt as load_dt,
                        repNationCd,
                        sum(salesAmt) as salesAmt,
                        sum(audiCnt) as audiCnt,
                        sum(showCnt) as showCnt
                FROM movie
                GROUP BY load_dt, repNationCd
                ORDER BY load_dt
                """
                )

df1.createOrReplaceTempView("nation")

#독립/상업영화별 매출액, 관객수 집계
df2 = spark.sql(f"""
                SELECT load_dt as load_dt,
                        multiMovieYn,
                        sum(salesAmt) as salesAmt,
                        sum(audiCnt) as audiCnt,
                        sum(showCnt) as showCnt
                FROM movie
                GROUP BY load_dt, multiMovieYn
                ORDER BY load_dt
                """
                )

df2.createOrReplaceTempView("multi")

df1.write.mode('overwrite').partitionBy("load_dt", "repNationCd").parquet("/home/oddsummer/data/movie/sum_nation")
df2.write.mode('overwrite').partitionBy("load_dt", "multiMovieYn").parquet("/home/oddsummer/data/movie/sum_multi")

spark.stop()
