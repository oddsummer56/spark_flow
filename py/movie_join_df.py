from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("joinDF").getOrCreate()

load_dt = sys.argv[1]

df=spark.read.parquet(f"/home/oddsummer/data/movie/repartition/load_dt={load_dt}")
df.createOrReplaceTempView("movie")

df_m=spark.sql(f"""
SELECT 
    movieCd, -- 영화의 대표코드
    movieNm,
    salesAmt, -- 매출액
    audiCnt, -- 관객수
    showCnt, --- 사영횟수
    multiMovieYn, -- 다양성 영화/상업영화를 구분지어 조회할 수 있습니다. “Y” : 다양성 영화 “N”
    repNationCd, -- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외국영화
   '{load_dt}'  AS load_DT
FROM movie 
WHERE multiMovieYn IS  NULL""")

df_m.createOrReplaceTempView("multi_null")

df_n=spark.sql(f"""
SELECT
    movieCd, -- 영화의 대표코드
    movieNm,
    salesAmt, -- 매출액
    audiCnt, -- 관객수
    showCnt, --- 사영횟수
    multiMovieYn, -- 다양성 영화/상업영화를 구분지어 조회할 수 있습니다. “Y” : 다양성 영화 “N”
    repNationCd, -- 한국/외국 영화별로 조회할 수 있습니다. “K: : 한국영화 “F” : 외>국영화
   '{load_dt}'  AS load_DT
FROM movie
WHERE repNationCd IS NULL
""")
df_n.createOrReplaceTempView("nation_null")


df_join=spark.sql(f"""
SELECT
    COALESCE(m.movieCd, n.movieCd) AS movieCd,
    COALESCE(m.movieNm, n.movieNm) AS movieNm,
    COALESCE(m.salesAmt, n.salesAmt) AS salesAmt,
    COALESCE(m.audiCnt, n.audiCnt) AS audiCnt,
    COALESCE(m.showCnt, n.showCnt) AS showCnt,
    COALESCE(m.multiMovieYn, n.multiMovieYn) AS multiMovieYn,
    COALESCE(m.repNationCd, n.repNationCd) AS repNationCd,     
    '{load_dt}'  AS load_DT
FROM multi_null m FULL OUTER JOIN nation_null n
ON m.movieCd = n.movieCd""")

df_join.createOrReplaceTempView("join")
df_join.show(30)

df_join.write.mode('append').partitionBy("load_dt", "multiMovieYn", "repNationCd").parquet("/home/oddsummer/data/movie/hive")

spark.stop()
