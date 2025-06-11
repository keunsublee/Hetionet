from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import argparse

# Initialize Spark Session
spark = SparkSession.builder.appName("HetioNet").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def question_one(edges_path):
    """
    Q1: For each drug, compute the number of genes
    and the number of diseases associated with the
    drug. Output results with top 5 number of genes in a
    descending order.
    """
    rdd = spark.sparkContext.textFile(edges_path)
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


def question_two(edges_path):
    """
    Compute the number of diseases associated
    with 1, 2, 3, …, n drugs. Output results with the top
    5 number of diseases in a descending order.
    """
    rdd = spark.sparkContext.textFile(edges_path)

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
    top_5 = rdd8.takeOrdered(5, key=lambda x: -x[1])
    for drug_count, disease_count in top_5:
        print(f"{drug_count} drugs -> {disease_count} diseases")


def question_three(edges_path, nodes_path):
    """
    Q3: Get the name of drugs that have the top 5
    number of genes. Out put the results.
    """
    rdd = spark.sparkContext.textFile(edges_path)
    rdd2 = rdd.map(lambda x: x.split())
    rdd3 = rdd2.filter(lambda x: x[1] in ["CdG", "CuG", "CbG"])

    # ( Compound ID, Gene ID)
    rdd4 = rdd3.map(lambda x: (x[0], x[2])).distinct()

    # (Compound ID, 1)
    rdd5 = rdd4.map(lambda x: (x[0], 1))

    # (Compound ID, 4)
    rdd6 = rdd5.reduceByKey(lambda a, b: a + b)

    # get the id - name relationship from nodes.tsv
    nodes_rdd = spark.sparkContext.textFile(nodes_path)
    nodes_rdd2 = nodes_rdd.map(lambda x: x.split())

    id_to_name_map = nodes_rdd2.map(lambda cols: (cols[0], cols[1]))

    # (Compound Id, (count, Compound Name))
    joined_rdd = rdd6.join(id_to_name_map)

    # (gene_count, drug_name)
    name_count_rdd = joined_rdd.map(lambda x: (x[1][0], x[1][1]))
    sorted_rdd = name_count_rdd.sortByKey(ascending=False)
    

    top_5 = sorted_rdd.take(5)
    for gene_count, drug_name in top_5:
        print(f"{drug_name} -> {gene_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HetioNet Analysis")
    parser.add_argument("--edges", required=True)
    parser.add_argument("--nodes", required=True)

    args = parser.parse_args()

    print('Question 1: ')
    question_one(args.edges)

    print('Question 2: ')
    question_two(args.edges)

    print('Question 3: ')
    question_three(args.edges, args.nodes)
