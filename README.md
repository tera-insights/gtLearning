gtLearning
=========

This project serves as an adaptation of [Ontological Pathfinder](https://bitbucket.org/datasci/ontological-pathfinding) for the [GrokIt](https://github.com/tera-insights/grokit) database engine. The primary purpose is demonstrate the efficiency and scale of GrokIt by comparing the final results for the Freebase dataset to those of Apache Spark and SQL. To that end, the following are key features of the performance:

 - Parallelization: The 388 million facts in the Freebase dataset are stored separately across a striped disk array, then read and processed separately, which results in a near perfect speed-up across 64 cores.
 - Pre-computation: Originally, Ontological Pathfinding considered every rule for each entry in the GroupJoin. However, on average less than 1% of rules were relevant to those entries. Through the use of equijoins and aggregates prior to the GroupJoin, each rule is mapped to only the facts for which that rule will predict new information.
 - Partitioning: Previously, Ontological Pathfinder used a scheme that partitioned the facts into smaller pieces and then stored them alongside the rules relevant to those facts. In the case of a rule being relevant to several different partitions, that rule was simply duplicated. GrokIt avoids this through its state management, which allows for the same states to be visible to every worker simultaneously.

Installation
-------------

Because `gtLearning` is simply an extension for the GrokIt database engine, it is essential that that is installed first. Installation notes can be found [here](https://github.com/tera-insights/grokit/blob/master/README). Additionally, the base R package, `gtBase`, must be installed. It can be found [here](https://github.com/tera-insights/gtBase). Afterwards, simply install this R package and GrokIt library as follows:

1. Navigate to the working directory: `cd /your/repository/here/`
2. Clone this repository: `git clone git@github.com:tera-insights/gtLearning.git`
3. Install the R package: `R CMD INSTALL /your/repository/here/package/`
4. Install the GrokIt library: `grokit makelib /your/repository/here/package/inst/learning`

Queries for each type of rule are included with the repository in the `queries` sub-directory. They can be ran directly in `R` using the `source` function, with the output being captured in the variable `result`, a data frame.

Results
--------

The following are the run-times taken for computing the confidence for rules of a given type:

1. 6.2 minutes
2. 6.2 minutes
3. 10.7 minutes
4. 11.1 minutes
5. 11.5 minutes
6. 16.9 minutes

