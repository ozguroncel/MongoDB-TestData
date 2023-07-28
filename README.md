# MongoDB-TestData
An algorithm that creates partial datasets based on user given parameters. The raw data size was 300ish GB including over 600 million records. Provided code generates 1 million records in roughly 10 minutes based on parameters. Allowing test data for other duplicate detection algorithms.

READMe

1. Preprocessing steps

1.1 Create MongoDB Collection

The available data is provided in five separate files named Mod0, Mod1, Mod2, Mod3 and Mod4. In the first step, these must first be imported into separate Collections using MongoDB Compass. Then, these five collections are merged into one collection using the Aggregation Pipeline in MongoDB Compass. The provided pipeline can be used for this purpose. It first eliminates all irrelevant data (in our case, everything except people data) in the $unset step and then merges the documents into a new collection called "Filtered5" in the $merge step. This pipeline must be run once in each of the five collections created at the beginning, so that all documents from the five collections end up in "Filtered5", comprising ~13.4 million documents.

1.2 Run Preprocessing.py

Next, we run the preprocessing.py script once, which assigns a random number (int) to each document in the "Filtered5" collection and creates an index on this "random_number" field. To allow the script to access the DB, the connection string in the code may need to be adjusted in advance if the collection is not stored locally on the localhost.                      
  
2. generation of test data
   
2.1 Generation of test data with "TestDataGenerator.py

Before the execution of the script must be adapted also here at the beginning in the code of the data base collection string if necessary. Furthermore, the import of libraries may be necessary, see program notes. At the beginning in the script can be found the #PARAMETER, which can be adjusted as desired. A note: If not all mods are used, e.g. due to memory limitations, then the parameter "max_cluster_number" must be adjusted with the maximum cluster number in the "Filtered5" collection. With this the script is ready for execution. The terminal will display "Process finished!" after successful completion. As well as the Execution Time in seconds will be printed. The records were stored in the "result.csv" file and the used parameter values separately in the "parameters.json" file.

2.2 Generation of test data via GUI parameter input

Alternatively, the GUI script "TestDataGenerator_GUI.py" can be used to generate test data, here the parameter input is done via a graphical input window. In the script only the details to the database connection must be checked before, thus the database collection string and also here applies if not all mods are used, then the parameter "max_cluster_number" must be adapted with the maximum cluster number in the "Filtered5" collection. Furthermore also here the import of libraries can be necessary, there we refer again to the program notes.
The program can be started via the GUI after the parameter input. The output is as described before, in the "result.csv" and "parameters.json" file to find and the execution time is output in the terminal.
