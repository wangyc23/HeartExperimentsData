# HeartExperimentData (Under development)
Environment requirements:

> Java 1.8
> Scala 2.11.12
> Spark 2.4.3

****************************************

## Procedures of HEART Experiment 

1. Save intermediate results from the baseline work. 

   ​	To avoid reinventing the wheel, we saved some useful intermediate results from FOCUS. The similar projects will be saved along with the running of FOCUS, but the candidate APIs are not saved. So we simply added some codes in the file of  "~/Focus/src/main/java/org/focus/ContextAwareRecommendation.java"

   ​	The new version of this file can be found in the folder "souceCode/modified ContextAwareRecommendation.java/". By using this file to replace the original one, the intermediate results can saved, which will be used to generate the rating matrices used in our experiment.

   ​	For the details of the baseline work, please refer to https://github.com/crossminer/FOCUS

   and their published paper

   ```tex
   @inproceedings{Nguyen:2019:FRS:3339505.3339636,
    author = {Nguyen, Phuong T. and Di Rocco, Juri and Di Ruscio, Davide and Ochoa, Lina and Degueule, Thomas and Di Penta, Massimiliano},
    title = {{FOCUS: A Recommender System for Mining API Function Calls and Usage Patterns}},
    booktitle = {Proceedings of the 41st International Conference on Software Engineering},
    series = {ICSE '19},
    year = {2019},
    location = {Montreal, Quebec, Canada},
    pages = {1050--1060},
    numpages = {11},
    url = {https://doi.org/10.1109/ICSE.2019.00109},
    doi = {10.1109/ICSE.2019.00109},
    acmid = {3339636},
    publisher = {IEEE Press},
    address = {Piscataway, NJ, USA},
   }
   ```

2. Generate rating matrices

   

​	
