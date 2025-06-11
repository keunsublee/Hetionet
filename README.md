# Hetionet project
This project analyzes the HetioNet biomedical knowledge graph using PySpark on Hadoop, MongoDB, and Neo4j to explore drug-gene, drug-disease, and disease-anatomy relationships. It processes large-scale biomedical data with the following components:
## PySpark on Hadoop:
Processes edges.tsv and nodes.tsv files to answer three queries:
- Computes the number of genes and diseases per drug, outputting the top 5 drugs by gene count.

- Calculates the number of diseases associated with 1, 2, 3, ..., n drugs, displaying the top 5 disease counts.

- Identifies the names of drugs with the top 5 gene associations by joining edge and node data.

Uses Spark RDDs and DataFrames for distributed processing of large datasets.

## MongoDB:
Queries the Disease collection using find_one({"_id": disease_id}) to retrieve all fields for a given disease ID.

Leverages pre-aggregated data in the database for efficient retrieval of disease-related information.

## Neo4j:
Executes a graph query to identify compounds meeting specific criteria:
```
- Compound upregulates a gene (CuG)
- Anatomy downregulates a gene (AdG)
- Disease localizes in anatomy (DlA)
- compound does not treat disease (CtD)
or
- Compound downregulates a gene (CdG)
- Anatomy upregulates a gene (AuG)
- Disease localizes in anatomy (DlA)
- compound does not treat disease (CtD)
```

Uses Neo4j’s graph database to model and query complex relationships between compounds, genes, anatomies, and diseases.

## My Contribution:
- Project1-neo4j.py
- Project2-hadoop-pyspark.py (questions 1 & 3)

# How to run:
## Project 1
Requires uploading data to mongodb and neo4j localhost

## Project 2
## Without hadoop:

- pip install pyspark (may require python3-venv)
- spark-submit Project2-local.py --nodes nodes.tsv --edges edges.tsv

## Hadoop hdfs locally (requires hadoop):
```
- sudo service ssh start
- start-dfs.sh
- hadoop fs -mkdir -p /user/\<youruser>		
    - make directory for files, change \<youruser> 
- hadoop fs -put ~/\<yourpath>/nodes.tsv 	    
    - put tsv in hdfs, change \<yourpath> to local file path of nodes.tsv
- hadoop fs -put ~/\<yourpath>/edges.tsv 	    
    - put tsv in hdfs, change \<yourpath> to local file path of edges.tsv
    - must also change tsv file paths in code to match "hdfs://localhost:9000/\<your hdfs path>/nodes.tsv"
- change username '/user/dx/' to \<youruser> in Project2-hadoop-pyspark.py
- spark-submit Project2-hadoop-pyspark.py
```

## Expected Output:
```
Question 1: 
+-----------------+-----+--------+
|         Compound|Genes|Diseases|
+-----------------+-----+--------+
|Compound::DB08865|  580|       1|
|Compound::DB01254|  558|       1|
|Compound::DB00997|  528|      17|
|Compound::DB00170|  521|       0|
|Compound::DB00390|  521|       2|
+-----------------+-----+--------+

Question 2: 
1 drugs -> 10 diseases
2 drugs -> 7 diseases
3 drugs -> 6 diseases
11 drugs -> 6 diseases
9 drugs -> 6 diseases

Question 3: 
Crizotinib -> 580
Dasatinib -> 558
Doxorubicin -> 528
Digoxin -> 521
Menadione -> 521
```

# Project 1
## Query 1:

### MongoDB
Reason for using MongoDB: The query wants information for a specific disease. Mongodb allows all the info for a disease to fall under 1 collection. Makes querying fast and simple.

#### MongoDB Schema:
#### Disease Collection:
- _id: ObjectId - diseaseID
- name: String - diseaseName
- CtD: [String] - list of compound names that treats disease
- CpD: [String] - list of compound names that palliates disease
- DaG: [String] - list of gene names that associates/causes disease
- DlA: [String] - list of anatomy names that disease occurs at
	
#### General:
- Updating the names to ids under disease.
- Make the query slower and more complex
- However, it will reduce redundancy and make the schema more flexible. 
- Be able to retrieve other info about the compound/gene/anatomy easier.
- For example, if i want to update the name of a compound, I would've only needed to update the name under the compound document once.
- Adding both name and ids under disease.
- Make query more complex
- It will reduce redundancy and make the schema more flexible. 
- Be able to retrieve other info about the compound/gene/anatomy easier.
- Add the “kind” into the document field in case collection name ever changes

#### Query: 
- self.db['Disease'].find_one({"_id": disease_id})
- Explanation: It looks for the disease id in the Disease collection and returns all the fields. 
- All the data is already aggregated under it from database creation. 

## Query 2:
### Neo4J

Reasoning: Graph database to query complex relationships between different nodes. Has fast data operations- input, traversal and querying. Chose Neo4j bc query 2 involved multiple relationships and Neo4j’s database and the Cypher language is more appropriate for handling interconnected data. 

Potential Improvements:
None. Neo4J already maps the relationships between the nodes and querying neo4j for relationships is straightforward and fast.
Rearranging query so disease is first, although the speed difference was not noticeable

#### Query:
```
MATCH 
    (compound:Compound)-[:upregulates]->(gene:Gene),
    (disease:Disease)-[:localizes]->
    (location:Anatomy)-[:downregulates]->(gene)
WHERE NOT (compound)-[:treats]->(disease)
RETURN DISTINCT compound.name
UNION
MATCH 
    (compound:Compound)-[:downregulates]->(gene:Gene),
    (disease:Disease)-[:localizes]->(location:Anatomy)-
    [:upregulates]->(gene)
WHERE NOT (compound)-[:treats]->(disease)
RETURN DISTINCT compound.name
```

#### Explanation:
```
It matches for the pattern where:
- Compound upregulates a gene (CuG)
- Anatomy downregulates a gene (AdG)
- Disease localizes in anatomy (DlA)
- compound does not treat disease (CtD)
OR
- Compound downregulates a gene (CdG)
- Anatomy upregulates a gene (AuG)
 - Disease localizes in anatomy (DlA)
 - compound does not treat disease (CtD)
```

# Project 2
## Q1:
Get top 5 drugs with the most gene associations and how many diseases each drug is associated with.

#### Pseudocode:
- Read file into RDD, Split each line
- Filter for gene and disease associations 
- Extract distinct pairs  (Compound ID, Gene ID), (Compound ID, Disease ID)
- Map each pair in gene and disease  (Key-Value pair: (Compound ID, 1)
- Reduce by key to count genes and diseases 
    (Key-Value pair: (Compound ID, Gene Count), (Compound ID, Disease Count) )
- Convert the gene & disease RDDs to DataFrames	
    (Key-Value pair: (Compound ID, Gene Count), (Compound ID, Disease Count) )
- Join Gene Dataframe and Disease Dataframe into joined DataFrame
    (Key-Value pair: (Compound, (Gene Count, Disease Count))
- Sort results by number of genes in descending order, limit 5

#### Design Patterns:
- Map-Reduce Pattern: Aggregates gene and disease counts per drug u- sing map to create (Compound ID, 1) pairs and reduceByKey to compute (Compound ID, Gene Count) and (Compound ID, Disease Count), enabling distributed summarization.

- Filter Pattern: Isolates drug-gene (CdG, CuG, CbG) and drug-disease (CtD, CpD) edges to focus on relevant relationships.

- Join Pattern: Merges gene and disease count DataFrames on Compound ID to produce (Compound ID, (Gene Count, Disease Count)), enriching the dataset.

- Top-K Pattern: Ranks drugs by gene count in descending order and selects the top 5, optimizing result presentation for downstream use (e.g., MongoDB storage, React UI display).



## Q2:
Compute the number of diseases associated with 1, 2, 3, …, n drugs. Output results with the top 5 number of diseases in a descending order

#### Pseudocode:
- Filter relations where it includes Compound and Disease only
- Create key-value pair, (Disease, Compound), after removing middle column
- Map to count diseases for each compound, (Disease, 1)
- Reduce to get the counts for disease for each compound, (Disease, drug count)
- Map to get drug count, (drug count, 1), for each disease
- Reduce to get disease counts for drug counts, (drug count, disease count)
- Sort the results by descending
- Take and print top 5 results

#### Design patterns:
- Map-Reduce Pattern: Aggregates drug counts per disease and then diseases per drug count using map to create (Disease, 1) and (Drug Count, 1) pairs, followed by reduceByKey to compute (Disease, Drug Count) and (Drug Count, Disease Count), enabling distributed hierarchical summarization.

- Filter Pattern: Isolates drug-disease relationships (CtD, CpD) to focus on relevant edges.

- Top-K Pattern: Ranks (Drug Count, Disease Count) pairs by disease count in descending order and selects the top 5, optimizing result presentation for downstream use (e.g., MongoDB storage, React UI display).



## Q3:
Get the name of drugs that have the top 5 number of genes. Output the results.

#### Pseudocode:
- Read file into RDD, Split each line
- Filter for gene associations
- Extract distinct pairs (Compound ID, Gene ID)
- Map each pair to (Compound ID, 1)
- Reduce by key to count genes per compound (Key-Value pair:(Compound ID, Gene Count)
- Read node relationships file into RDD
- Split each line
- Create ID to Name mapping (Compound ID, Name)
- Join RDDs to get (Compound ID, (Gene count, Compound Name))
- Map to (Compound Name, Gene count)
- Sort results by Gene count in descending order (Compound Name, Gene count)
- Get top 5 results

#### Design Patterns:
- Map-Reduce Pattern: Aggregates gene counts per drug using map to create (Compound ID, 1) pairs and reduceByKey to compute (Compound ID, Gene Count), enabling distributed summarization.

- Filter Pattern: Isolates drug-gene associations (CdG, CuG, CbG) to focus on relevant edges.

- Join Pattern: Merges (Compound ID, Gene Count) with (Compound ID, Name) from nodes.tsv to produce (Compound ID, (Gene Count, Name)), enriching the dataset with drug names.

- Top-K Pattern: Ranks drugs by gene count in descending order and selects the top 5, optimizing result presentation for downstream use (e.g., MongoDB storage, React UI display).

