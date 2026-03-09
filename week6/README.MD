


Task 2: 


Partitions before: 12

Parquet files created: 4
  part-00000-23baf48a-819c-44a7-a251-76486eeffca1-c000.snappy.parquet: 24.39 MB
  part-00001-23baf48a-819c-44a7-a251-76486eeffca1-c000.snappy.parquet: 24.42 MB
  part-00002-23baf48a-819c-44a7-a251-76486eeffca1-c000.snappy.parquet: 24.42 MB
  part-00003-23baf48a-819c-44a7-a251-76486eeffca1-c000.snappy.parquet: 24.41 MB

Average file size: 24.41 MB

Note if I should work with repartitioned files instead: 
The original file benefits from Parquet predicate pushdown — Spark can skip row groups that don't match 2025-11-15 without reading the whole file
The repartitioned files were written after a full shuffle (repartition(4) randomly distributes rows), so date-based filtering has to scan all 4 partitions fully



Task 3
Q3 - Trips on November 15th: 162,604

Task 4

Q4 - Longest trip in hours: 90.6



Task 5

Default Spark UI 4040


Task 6

|Zone                                         |trip_count|
+---------------------------------------------+----------+
|Governor's Island/Ellis Island/Liberty Island|1         |
|Eltingville/Annadale/Prince's Bay            |1         |
|Arden Heights                                |1         |
|Port Richmond                                |3         |
|Rikers Island                                |4   


