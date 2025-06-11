# Hetionet project

## My Contribution:
- Project1-neo4j.py
- Project2-hadoop-pyspark.py (questions 1 & 3)

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
It matches for the pattern where:
- Compound upregulates a gene and (CuG)
- Anatomy downregulates a gene (AdG)
- Disease localizes in anatomy (DlA)
- compound does not treat disease (CtD)
OR
- Compound downregulates a gene and (CdG)
- Anatomy upregulates a gene (AuG)
 - Disease localizes in anatomy (DlA)
 - compound does not treat disease (CtD)


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
- Summarization Pattern:
    - Counting and summing genes for each drug
    - Counting and summing diseases for each drug
- Filtering Pattern: 
    - Filter RDDs for drug-gene and drug-disease  associations
- Pair Pattern:
    - (Compound ID, Compound Name)
    - (Compound ID, Gene Count)
    - (Compound, (Gene Count, Disease Count))
- Join patterns: 
    - Join disease count and gene counts for each drug
- Iteration patterns: 
    -Sorting and limiting the results to get the top 5 drugs.

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
- Summarization pattern:
    - Counting and summing drugs for each disease
    - Counting and summing diseases for unique drug counts
- Filtering pattern:
    - Filters to only disease - drug relationships
- Pairs pattern:
    - The key-value pair (Disease, Compound) are unique so stripes pattern wouldn't be helpful



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
- Summarization Pattern:
    - Counting and summing diseases for each drug
- Filtering Pattern: 
    - Filter RDDs for drug-gene associations
- Pair Pattern:
    - Key-Value pair: (Compound ID, Compound Name)
    - (Compound ID, Gene Count)
    - (Compound Name, Gene Count)
- Join patterns: 
    - Join name and gene counts for each drug
- Iteration patterns: 
    -Sorting and limiting the results to get the top 5 drugs.

# How to run:
## Project 1
Requires uploading data to mongodb and neo4j localhost

## Project 2
## Without hadoop:

- pip install pyspark (in a virtual env)
- spark-submit Project2-local.py --nodes nodes.tsv --edges edges.tsv

## Hadoop hdfs locally (requires hadoop):
```
- sudo service ssh start
- start-dfs.sh
- hadoop fs -mkdir -p /user/\<youruser>		
    - make directory for files, change \<youruser> 
- hadoop fs -put ~/\<yourpath>/nodes.tsv 	    
    - put tsv in hdfs, change \<yourpath> to local file path
- hadoop fs -put ~/\<yourpath>/edges.tsv 	    
    - put tsv in hdfs, change \<yourpath> to local file path
    - must also change tsv file paths in code to match "hdfs://localhost:9000/\<your hdfs path>/nodes.tsv"
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