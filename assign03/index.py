#!/usr/bin/env python3
"""
Low-Level Spark Transformations and Actions Demo
================================================
This script demonstrates various RDD transformations and actions in PySpark
with practical examples and their results.
"""

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import json
from typing import List, Tuple, Any

def create_spark_session():
    """Initialize Spark session with optimized configuration"""
    conf = SparkConf().setAppName("LowLevelSparkDemo").setMaster("local[*]")
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")  # Reduce verbose logging
    
    return spark, sc

def demo_transformations(sc: SparkContext):
    """Demonstrate various RDD transformations"""
    print("=" * 60)
    print("RDD TRANSFORMATIONS DEMO")
    print("=" * 60)
    
    # Sample data
    numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    words = ["spark", "python", "big", "data", "analytics", "machine", "learning"]
    pairs = [("apple", 5), ("banana", 3), ("apple", 2), ("orange", 4), ("banana", 1)]
    
    # Create RDDs
    num_rdd = sc.parallelize(numbers, 3)  # 3 partitions
    word_rdd = sc.parallelize(words, 2)
    pair_rdd = sc.parallelize(pairs, 2)
    
    print(f"Original numbers: {numbers}")
    print(f"Original words: {words}")
    print(f"Original pairs: {pairs}")
    print()
    
    # 1. MAP TRANSFORMATION
    print("1. MAP - Square each number")
    squared = num_rdd.map(lambda x: x ** 2)
    print(f"   Result: {squared.collect()}")
    
    # 2. FILTER TRANSFORMATION
    print("\n2. FILTER - Even numbers only")
    evens = num_rdd.filter(lambda x: x % 2 == 0)
    print(f"   Result: {evens.collect()}")
    
    # 3. FLATMAP TRANSFORMATION
    print("\n3. FLATMAP - Split words into characters")
    chars = word_rdd.flatMap(lambda word: list(word))
    print(f"   Result: {chars.collect()[:20]}...")  # Show first 20 chars
    
    # 4. DISTINCT TRANSFORMATION
    print("\n4. DISTINCT - Unique characters")
    unique_chars = chars.distinct()
    print(f"   Result: {sorted(unique_chars.collect())}")
    
    # 5. UNION TRANSFORMATION
    print("\n5. UNION - Combine two RDDs")
    more_numbers = sc.parallelize([11, 12, 13])
    combined = num_rdd.union(more_numbers)
    print(f"   Result: {combined.collect()}")
    
    # 6. INTERSECTION TRANSFORMATION
    print("\n6. INTERSECTION - Common elements")
    set1 = sc.parallelize([1, 2, 3, 4, 5])
    set2 = sc.parallelize([4, 5, 6, 7, 8])
    common = set1.intersection(set2)
    print(f"   Result: {common.collect()}")
    
    # 7. CARTESIAN TRANSFORMATION
    print("\n7. CARTESIAN - All pairs")
    small_rdd1 = sc.parallelize([1, 2])
    small_rdd2 = sc.parallelize(['a', 'b'])
    cartesian = small_rdd1.cartesian(small_rdd2)
    print(f"   Result: {cartesian.collect()}")
    
    # 8. SAMPLE TRANSFORMATION
    print("\n8. SAMPLE - Random sample")
    sample = num_rdd.sample(False, 0.5, seed=42)
    print(f"   Result: {sample.collect()}")
    
    # Key-Value Pair Transformations
    print("\n" + "=" * 40)
    print("KEY-VALUE TRANSFORMATIONS")
    print("=" * 40)
    
    # 9. REDUCEBYKEY TRANSFORMATION
    print("\n9. REDUCEBYKEY - Sum values by key")
    summed = pair_rdd.reduceByKey(lambda a, b: a + b)
    print(f"   Result: {summed.collect()}")
    
    # 10. GROUPBYKEY TRANSFORMATION
    print("\n10. GROUPBYKEY - Group values by key")
    grouped = pair_rdd.groupByKey()
    grouped_result = grouped.mapValues(list).collect()
    print(f"    Result: {grouped_result}")
    
    # 11. MAPVALUES TRANSFORMATION
    print("\n11. MAPVALUES - Double all values")
    doubled_values = pair_rdd.mapValues(lambda x: x * 2)
    print(f"    Result: {doubled_values.collect()}")
    
    # 12. KEYS and VALUES TRANSFORMATIONS
    print("\n12. KEYS and VALUES")
    keys_only = pair_rdd.keys()
    values_only = pair_rdd.values()
    print(f"    Keys: {keys_only.collect()}")
    print(f"    Values: {values_only.collect()}")
    
    # 13. JOIN TRANSFORMATION
    print("\n13. JOIN - Inner join two RDDs")
    rdd1 = sc.parallelize([("a", 1), ("b", 2), ("c", 3)])
    rdd2 = sc.parallelize([("a", 4), ("b", 5), ("d", 6)])
    joined = rdd1.join(rdd2)
    print(f"    Result: {joined.collect()}")
    
    # 14. COGROUP TRANSFORMATION
    print("\n14. COGROUP - Group RDDs by key")
    cogrouped = rdd1.cogroup(rdd2)
    cogroup_result = cogrouped.mapValues(lambda x: (list(x[0]), list(x[1]))).collect()
    print(f"    Result: {cogroup_result}")
    
    # 15. SORTBYKEY TRANSFORMATION
    print("\n15. SORTBYKEY - Sort by keys")
    unsorted_pairs = sc.parallelize([("c", 3), ("a", 1), ("b", 2)])
    sorted_pairs = unsorted_pairs.sortByKey()
    print(f"    Result: {sorted_pairs.collect()}")

def demo_actions(sc: SparkContext):
    """Demonstrate various RDD actions"""
    print("\n" + "=" * 60)
    print("RDD ACTIONS DEMO")
    print("=" * 60)
    
    numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    words = ["spark", "python", "big", "data", "analytics"]
    pairs = [("apple", 5), ("banana", 3), ("apple", 2), ("orange", 4)]
    
    num_rdd = sc.parallelize(numbers, 3)
    word_rdd = sc.parallelize(words, 2)
    pair_rdd = sc.parallelize(pairs, 2)
    
    # 1. COLLECT ACTION
    print("1. COLLECT - Retrieve all elements")
    all_numbers = num_rdd.collect()
    print(f"   Result: {all_numbers}")
    
    # 2. COUNT ACTION
    print("\n2. COUNT - Count elements")
    count = num_rdd.count()
    print(f"   Result: {count}")
    
    # 3. FIRST ACTION
    print("\n3. FIRST - Get first element")
    first = num_rdd.first()
    print(f"   Result: {first}")
    
    # 4. TAKE ACTION
    print("\n4. TAKE - Get first N elements")
    first_three = num_rdd.take(3)
    print(f"   Result: {first_three}")
    
    # 5. TOP ACTION
    print("\n5. TOP - Get top N elements")
    top_three = num_rdd.top(3)
    print(f"   Result: {top_three}")
    
    # 6. TAKESAMPLE ACTION
    print("\n6. TAKESAMPLE - Random sample")
    sample = num_rdd.takeSample(False, 5, seed=42)
    print(f"   Result: {sample}")
    
    # 7. REDUCE ACTION
    print("\n7. REDUCE - Sum all numbers")
    total = num_rdd.reduce(lambda a, b: a + b)
    print(f"   Result: {total}")
    
    # 8. FOLD ACTION
    print("\n8. FOLD - Sum with initial value")
    folded = num_rdd.fold(0, lambda a, b: a + b)
    print(f"   Result: {folded}")
    
    # 9. AGGREGATE ACTION
    print("\n9. AGGREGATE - Complex aggregation")
    # Calculate sum and count simultaneously
    agg_result = num_rdd.aggregate(
        (0, 0),  # Initial value (sum, count)
        lambda acc, value: (acc[0] + value, acc[1] + 1),  # Seq function
        lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # Comb function
    )
    avg = agg_result[0] / agg_result[1] if agg_result[1] > 0 else 0
    print(f"   Sum: {agg_result[0]}, Count: {agg_result[1]}, Average: {avg:.2f}")
    
    # 10. FOREACH ACTION
    print("\n10. FOREACH - Print each element (side effect)")
    print("    Elements: ", end="")
    num_rdd.take(5).foreach(lambda x: print(x, end=" "))  # Using take to limit output
    
    # 11. COUNTBYKEY ACTION
    print("\n\n11. COUNTBYKEY - Count occurrences by key")
    count_by_key = pair_rdd.countByKey()
    print(f"    Result: {dict(count_by_key)}")
    
    # 12. COUNTBYVALUE ACTION
    print("\n12. COUNTBYVALUE - Count occurrences by value")
    repeated_numbers = sc.parallelize([1, 2, 2, 3, 3, 3, 4])
    count_by_value = repeated_numbers.countByValue()
    print(f"    Result: {dict(count_by_value)}")
    
    # 13. COLLECTASMAP ACTION (for pair RDDs)
    print("\n13. COLLECTASMAP - Collect as dictionary")
    unique_pairs = sc.parallelize([("a", 1), ("b", 2), ("c", 3)])
    as_map = unique_pairs.collectAsMap()
    print(f"    Result: {dict(as_map)}")
    
    # 14. LOOKUP ACTION
    print("\n14. LOOKUP - Find values for a key")
    values_for_apple = pair_rdd.lookup("apple")
    print(f"    Values for 'apple': {values_for_apple}")
    
    # 15. ISEMPTY ACTION
    print("\n15. ISEMPTY - Check if RDD is empty")
    empty_rdd = sc.parallelize([])
    print(f"    Empty RDD is empty: {empty_rdd.isEmpty()}")
    print(f"    Number RDD is empty: {num_rdd.isEmpty()}")

def demo_advanced_operations(sc: SparkContext):
    """Demonstrate advanced RDD operations"""
    print("\n" + "=" * 60)
    print("ADVANCED OPERATIONS DEMO")
    print("=" * 60)
    
    # 1. PARTITIONING
    print("1. PARTITIONING OPERATIONS")
    data = range(1, 21)  # 1 to 20
    rdd = sc.parallelize(data, 4)
    
    print(f"   Number of partitions: {rdd.getNumPartitions()}")
    print(f"   Partitions content: {rdd.glom().collect()}")
    
    # Repartition
    repartitioned = rdd.repartition(2)
    print(f"   After repartition to 2: {repartitioned.glom().collect()}")
    
    # Coalesce
    coalesced = rdd.coalesce(2)
    print(f"   After coalesce to 2: {coalesced.glom().collect()}")
    
    # 2. CUSTOM PARTITIONER
    print("\n2. CUSTOM PARTITIONING")
    pairs = [("apple", 1), ("banana", 2), ("apple", 3), ("cherry", 4), ("banana", 5)]
    pair_rdd = sc.parallelize(pairs, 3)
    
    # Partition by key hash
    partitioned_pairs = pair_rdd.partitionBy(2)
    print(f"   Partitioned pairs: {partitioned_pairs.glom().collect()}")
    
    # 3. PERSISTENCE/CACHING
    print("\n3. PERSISTENCE OPERATIONS")
    large_rdd = sc.parallelize(range(1, 1001), 4)
    expensive_rdd = large_rdd.map(lambda x: x ** 2).filter(lambda x: x % 2 == 0)
    
    # Cache the RDD
    expensive_rdd.cache()
    
    print(f"   First action - count: {expensive_rdd.count()}")
    print(f"   Second action - sum: {expensive_rdd.sum()}")  # Uses cached data
    
    # Check storage level
    print(f"   Storage level: {expensive_rdd.getStorageLevel()}")
    
    # Unpersist
    expensive_rdd.unpersist()

def demo_real_world_example(sc: SparkContext):
    """Demonstrate a real-world example: Word Count"""
    print("\n" + "=" * 60)
    print("REAL-WORLD EXAMPLE: WORD COUNT")
    print("=" * 60)
    
    # Sample text data
    text_data = [
        "Apache Spark is a unified analytics engine",
        "Spark provides high-level APIs in Java Scala Python and R",
        "Spark runs on Hadoop YARN Kubernetes or standalone",
        "Apache Spark achieves high performance for batch and streaming data"
    ]
    
    # Create RDD
    text_rdd = sc.parallelize(text_data)
    
    # Word count implementation
    word_counts = (text_rdd
                  .flatMap(lambda line: line.lower().split())
                  .map(lambda word: (word, 1))
                  .reduceByKey(lambda a, b: a + b)
                  .sortBy(lambda x: x[1], ascending=False))
    
    print("Word Count Results:")
    for word, count in word_counts.collect():
        print(f"   {word}: {count}")
    
    # Advanced analysis
    print(f"\nStatistics:")
    print(f"   Total unique words: {word_counts.count()}")
    print(f"   Most frequent word: {word_counts.first()}")
    print(f"   Words appearing only once: {word_counts.filter(lambda x: x[1] == 1).count()}")

def performance_comparison(sc: SparkContext):
    """Compare performance of different operations"""
    print("\n" + "=" * 60)
    print("PERFORMANCE COMPARISON")
    print("=" * 60)
    
    import time
    
    # Create large dataset
    large_data = range(1, 100001)  # 100K elements
    large_rdd = sc.parallelize(large_data, 8)
    
    # Compare collect vs take
    start_time = time.time()
    sample_take = large_rdd.take(100)
    take_time = time.time() - start_time
    
    start_time = time.time()
    filtered_collect = large_rdd.filter(lambda x: x <= 100).collect()
    filter_time = time.time() - start_time
    
    print(f"take(100) time: {take_time:.4f} seconds")
    print(f"filter + collect time: {filter_time:.4f} seconds")
    
    # Compare reduceByKey vs groupByKey
    pair_data = [(i % 1000, i) for i in range(10000)]
    pair_rdd = sc.parallelize(pair_data, 4)
    
    start_time = time.time()
    reduce_result = pair_rdd.reduceByKey(lambda a, b: a + b).count()
    reduce_time = time.time() - start_time
    
    start_time = time.time()
    group_result = pair_rdd.groupByKey().mapValues(sum).count()
    group_time = time.time() - start_time
    
    print(f"\nreduceByKey time: {reduce_time:.4f} seconds")
    print(f"groupByKey + sum time: {group_time:.4f} seconds")
    print(f"reduceByKey is {group_time/reduce_time:.2f}x faster")

def main():
    """Main function to run all demonstrations"""
    print("Low-Level Spark Transformations and Actions Demo")
    print("=" * 60)
    
    # Initialize Spark
    spark, sc = create_spark_session()
    
    try:
        # Run all demonstrations
        demo_transformations(sc)
        demo_actions(sc)
        demo_advanced_operations(sc)
        demo_real_world_example(sc)
        performance_comparison(sc)
        
        print("\n" + "=" * 60)
        print("DEMO COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()