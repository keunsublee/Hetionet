from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session with configuration
spark = SparkSession.builder.master("local[4]").appName("MyApp").getOrCreate()
# To run locally without hadoop comment the line above and use the line below:
# spark = SparkSession.builder..appName("MyApp").getOrCreate()
spark.sparkContext.setLogLevel("OFF")

def question_one():
    """
    Q1: For each drug, compute the number of genes
    and the number of diseases associated with the
    drug. Output results with top 5 number of genes in a
    descending order.
    """
    rdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/dx/edges.tsv")
    # change username '/user/dx/' 
    #   rdd = spark.sparkContext.textFile("edges.tsv")
    rdd2 = rdd.map(lambda x: x.split())

    # Filter Drug-Gene and Drug-Disease associations (CdG, CuG, CbG) and (CtD, CpD)
    gene_filter = rdd2.filter(lambda x: x[1] in ['CdG', 'CuG', 'CbG'])
    disease_filter = rdd2.filter(lambda x: x[1] in ['CtD', 'CpD'])

    # Map distinct pairs 
    # (Drug ID, Gene ID) and (Drug ID, Disease ID)
    gene_uniq = gene_filter.map(lambda x: (x[0], x[2])).distinct()
    disease_uniq = disease_filter.map(lambda x: (x[0], x[2])).distinct() 

    # Map (Compound ID, 1) 
    mapped_genes = gene_uniq.map(lambda x: (x[0], 1))
    mapped_diseases = disease_uniq.map(lambda x: (x[0], 1)) 
    
    # (Compound ID, 4) Reduce by key 
    genes = mapped_genes.reduceByKey(lambda a, b: a + b)
    diseases = mapped_diseases.reduceByKey(lambda a, b: a + b)

    # Convert RDD to spark dataFrame
    genes_df = genes.toDF(["Compound", "Genes"]) 
    diseases_df = diseases.toDF(["Compound", "Diseases"])
    
    # Join genes and diseases 
    join_count = genes_df.join(diseases_df, on="Compound", how="outer").fillna(0) 
    # Top 5 drugs in descending order 
    output = join_count.orderBy(col("Genes").desc()).limit(5) 
    # Output
    output.show()

def question_two():
    """
    Compute the number of diseases associated
    with 1, 2, 3, …, n drugs. Output results with the top
    5 number of diseases in a descending order.
    """
    rdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/dx/edges.tsv")
    # change username '/user/dx/' 
    #   rdd = spark.sparkContext.textFile("edges.tsv")
     # filter by compound - disease relationships only
    rdd2 = rdd.map(lambda x: x.split())
    rdd3 = rdd2.filter(lambda x: x[1] in ["CtD", "CpD"])

    # remove middle column
    # (Disease, Compound)
    rdd4 = rdd3.map(lambda x: (x[2], x[0])).distinct()

    # map the count for each compound for a disease
    rdd5 = rdd4.map(lambda x: (x[0], 1))

    # aggregate the counts of diseases for each drug
    # (Disease, drug_count)
    rdd6 = rdd5.reduceByKey(lambda a, b: a + b)

    # map the count for each disease
    rdd7 = rdd6.map(lambda x: (x[1], 1))

    # aggregate the counts of drugs
    # (Drug_count, disease_count)
    rdd8 = rdd7.reduceByKey(lambda a, b: a + b)

    # sort by count of diseases in descending order
    sorted_result = rdd8.sortBy(lambda x: x[1], ascending=False)

    # show top 5 results
    top_5 = sorted_result.take(5)
    for drug_count, disease_count in top_5:
        print(f"{drug_count} drugs -> {disease_count} diseases")

def question_three():
    """
    Q3: Get the name of drugs that have the top 5
    number of genes. Output the results.
    """
    rdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/dx/edges.tsv")
    # change username '/user/dx/' 
    #   rdd = spark.sparkContext.textFile("edges.tsv")
    rdd2 = rdd.map(lambda x: x.split())
    rdd3 = rdd2.filter(lambda x: x[1] in ["CdG", "CuG", "CbG"])

    # ( Compound ID, Gene ID)
    rdd4 = rdd3.map(lambda x: (x[0], x[2])).distinct()
    
    # (Compound ID, 1)
    rdd5 = rdd4.map(lambda x: (x[0], 1))

    # (Compound ID, 4)
    rdd6 = rdd5.reduceByKey(lambda a, b: a + b)

    # get the id - name relationship from nodes.tsv
    nodes_rdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/dx/nodes.tsv")
    # change username '/user/dx/' 
    # nodes_rdd = spark.sparkContext.textFile("nodes.tsv")
    nodes_rdd2 = nodes_rdd.map(lambda x: x.split())

    # Broadcast smaller nodes data 
    id_to_name_map = nodes_rdd2.map(lambda cols: (cols[0], cols[1]))
    id_to_name = spark.sparkContext.broadcast(id_to_name_map.collectAsMap())

    # Map (Compound ID, count) to (Compound Name, count)
    def get_drug_name(compound_id):
        return id_to_name.value.get(compound_id, "")

    # Map to (Drug Name, Gene Count)
    name_count_rdd = rdd6.map(lambda x: (get_drug_name(x[0]), x[1]))

    # Sort by gene count (descending)
    final_rdd = name_count_rdd.sortBy(lambda x: x[1], ascending=False)
    
    top_5 = final_rdd.take(5)
    
    print("\n")
    for drug_name, gene_count in top_5:
        print(f"{drug_name} -> {gene_count}")


question_one()
question_two()
question_three()


spark.stop()
