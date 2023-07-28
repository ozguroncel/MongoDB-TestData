from pymongo import MongoClient
import random
from datetime import datetime
import time
import csv
from tkinter import*
import json
import pandas as pd

# Datenbankverbindung
client = MongoClient("mongodb://localhost:27017/")
db = client['Operations']
collection = db['Filtered5']

# Gibt eine Zahl zischen 1 und 100 für den Seed value zurück 
def generateSeed():
    return random.randint(1, 100)

#PARAMETER
desired_size = 100000
overlap_for_snapshots =  0.2
snapshot_number = 2
year_min = 2008
year_max = 2021
min_heterogenity = 0.001
max_heterogenity = 0.5
seed = generateSeed()
batch_size = 1000 # Anzahl der Cluster pro Batch
max_cluster_number = 13406000

# Anzahl an Duplikaten und Unikate und Duplikate-Pairs
number_duplikate = desired_size * overlap_for_snapshots
number_of_unique_to_find = (desired_size - number_duplikate)//2
duplicate_pairs = number_duplikate/2

# Auswahl von 2 Snapshot-Dates die zwischen Min Year und Max Year liegen
all_snapshot_dates = ["2008-11-04","2009-01-01","2010-01-01","2010-11-02","2011-01-01","2012-01-01","2012-05-08","2012-07-17","2012-11-06","2012-12-31","2014-01-01","2014-05-06","2014-07-15","2014-11-04","2015-01-01","2015-09-15","2015-10-06","2015-11-03","2016-01-01","2016-03-15","2016-06-07","2016-11-08","2017-01-01","2017-09-12","2017-10-10","2017-11-07","2018-01-01","2018-05-08","2018-11-06","2019-01-01","2019-04-30","2019-05-14","2019-07-09","2019-09-10","2019-10-08","2019-11-05","2020-01-01","2020-03-03","2020-06-23","2020-11-03","2021-01-01"]
valid_dates = [datetime.strptime(date, "%Y-%m-%d").date() for date in all_snapshot_dates
                  if year_min <= datetime.strptime(date, "%Y-%m-%d").date().year <= year_max]
 
snapshot1, snapshot2 = random.sample(valid_dates, 2)
snapshot1, snapshot2 = snapshot1.strftime("%Y-%m-%d"), snapshot2.strftime("%Y-%m-%d")

# Generiert uns eine Random Liste 0-N - Werte dienen als random_number unserer Cluster
def generate_random_number_list(seed, N):
    random.seed(seed)
    random_number_list = list(range(N))
    random.shuffle(random_number_list)
    return random_number_list

# Gibt die nächsten Nummern aus der random_number_list für den nächsten Batch zurück
def generate_batch_random_number_list(batch_size, random_number_list, batchnumber):
    start_index = batchnumber * batch_size
    end_index = (batchnumber + 1) * batch_size
    print(f"Current Batchnumber: {batchnumber}")
    return random_number_list[start_index:end_index]


# Cluster des Batches abrufen und sortieren 
def get_cluster_of_batch(batch_random_number_list):
    if batch_random_number_list is not None:
        # Erstelle die $or-Abfrage mit der gegebenen Reihenfolge der unique values
        query = {"$or": [{"random_number": value} for value in batch_random_number_list]}
        # Verwende den Index "random_number_index" für die Sortierung
        sort_key = [("random_number", 1)]
        return collection.find(query).sort(sort_key)

# Prüfung der Unikate und Duplikate - MAIN Funktion 
def process_clusters_in_batches():
    all_found_uniques = []
    all_found_duplicates = []
    current_count_snapshot1 = 0 #zählt wie viele Unique1 wir haben 
    current_count_snapshot2 = 0 #zählt wie viele Unique2 wir haben 
    current_count_duplicates = 0 #zählt wie viele Duplikate wir haben 
    numbers_to_skip_in_next_batch = 0 #Trackt den Startpunkt des jeweiligen Batches
    
    random_number_list = generate_random_number_list(seed, max_cluster_number) # Generated random Liste für die abzufragenden Cluster

    while current_count_snapshot1 < number_of_unique_to_find or current_count_snapshot2 < number_of_unique_to_find:
            batch_random_number_list=generate_batch_random_number_list (batch_size, random_number_list, numbers_to_skip_in_next_batch//batch_size)# id-Liste für den jeweiligen Batch
            clusters_in_batch = get_cluster_of_batch(batch_random_number_list)# holt und sortiert den batch 
            
            duplicates_found_in_batch = []
            uniques1_found_in_batch = []
            uniques2_found_in_batch = []
            
            # geht jedes Cluster im Batch nacheinander durch 
            for cluster in clusters_in_batch:
                documents = cluster['duplicates']
                unique1_found_in_cluster = [] #prüft auf Snapshotdatum1
                unique2_found_in_cluster = [] #prüft auf Snapshotdatum2
                document_number = 0

                for i in documents:
                    document_dates = i['occurs-in']
                    for date in document_dates:
                        if snapshot1 == date:
                            unique1_found_in_cluster.append(document_number)# add Objektnummer zu unique1_found_in_cluster
                        if snapshot2 == date: 
                            unique2_found_in_cluster.append(document_number)# add Objektnummer zu unique2_found_in_cluster
                    document_number += 1

                
               #Vergleich der Arrays unique1_found_in_cluster und unique2_found_in_cluster - Einordung in Unique oder Duplicate
                if len(unique1_found_in_cluster) > 0 or len(unique2_found_in_cluster) > 0: # Eins der Arrays oder beide sind nicht null 
                    
                    if len(unique1_found_in_cluster) == 0 or len(unique2_found_in_cluster) == 0:# Prüft ob eins der Arrays leer ist 
                        if len(unique1_found_in_cluster) == 0:
                            if current_count_snapshot2 < number_of_unique_to_find:
                                uniques2_found_in_batch.append(documents[unique2_found_in_cluster[0]]) #Add Dokument wo Snapshotdate2 = true, zu uniques2_found_in_batch
                                current_count_snapshot2 += 1
                          
                        elif len(unique2_found_in_cluster) == 0:
                            if current_count_snapshot1 < number_of_unique_to_find:
                                uniques1_found_in_batch.append(documents[unique1_found_in_cluster[0]]) #Add Dokument wo Snapshotdate1 = true, zu uniques1_found_in_batch
                                current_count_snapshot1 += 1
                          
                    if not set(unique1_found_in_cluster) & set(unique2_found_in_cluster): # Die Arrays haben keine gemeinsamen Zahlen 
                        duplicates_found_in_batch.append(cluster["_id"])
                        current_count_duplicates += 1
        
                    elif len(list(set(unique1_found_in_cluster).difference(set(unique2_found_in_cluster)))) and len(list(set(unique2_found_in_cluster).difference(set(unique1_found_in_cluster)))) >= 1: # Entferne die Überscheidung der Arrays und schaue ob sie trozdem noch >= 1 sind
                        duplicates_found_in_batch.append(cluster["_id"])
                        current_count_duplicates += 1  
                    
                    elif len(list(set(unique1_found_in_cluster).difference(set(unique2_found_in_cluster)))) == 0 and len(list(set(unique2_found_in_cluster).difference(set(unique1_found_in_cluster)))) == 0: # beide Überscheidungen sind leer 
                        if len(uniques1_found_in_batch)>=len(uniques2_found_in_batch): #prüft welche liste länger ist und added das unikat zu der kürzeren 
                            if current_count_snapshot2 < number_of_unique_to_find:
                                uniques2_found_in_batch.append(documents[unique2_found_in_cluster[0]])
                                current_count_snapshot2 += 1   
                        else:
                            if current_count_snapshot1 < number_of_unique_to_find:
                                uniques1_found_in_batch.append(documents[unique1_found_in_cluster[0]])
                                current_count_snapshot1 += 1

                if current_count_snapshot1 >= number_of_unique_to_find and current_count_snapshot2 >= number_of_unique_to_find:
                    break
            
            #gefundnene Paare und Uniques in dem Batch an all_found_uniques übergeben 
            all_found_uniques.extend(uniques1_found_in_batch)
            all_found_uniques.extend(uniques2_found_in_batch)
            all_found_duplicates.extend(duplicates_found_in_batch)
    
            numbers_to_skip_in_next_batch += batch_size #numbers_to_skip_in_next_batch zahl um batchzahl erhöhen = startpunkt für den nächsten Batch

            if current_count_snapshot1 >= number_of_unique_to_find and current_count_snapshot2 >= number_of_unique_to_find:
                    break
            
    return all_found_uniques, all_found_duplicates


# Prüft auf passenden heterogenity 
def calculate_heterogenity(pair):
    heterogenity = 0

    document_1, document_2 = pair
    id_1 = document_1['_id']

    heterogenity = document_2['statistics']['heterogeneity (person)']['A-01'][id_1]
    return heterogenity

def get_duplicates(cluster_ids):
    query = {"_id": {"$in": cluster_ids}} 
    duplicate_clusters = collection.find(query) # holt cluster aus der DB 
    duplicates = []
    current_size = 0
    while current_size < duplicate_pairs:
        for cluster in duplicate_clusters: # Geht jedes Cluster durch 
            documents = cluster['duplicates'] # gehte jedes dokument im cluster durch 
            num_documents = len(documents)
            if num_documents >= 2:
                pairs = [(documents[i], documents[i + 1]) for i in range(num_documents - 1)] #bildet paare aus den dokumenten im Cluster
                for pair in pairs: #prüft jedes Paar auf die ausgewählten Daten und checkt den Heterogrenity score
                    if any(snapshot1 in doc['occurs-in'] for doc in pair) and \
                    any(snapshot2 in doc['occurs-in'] for doc in pair) and \
                    min_heterogenity < calculate_heterogenity(pair) < max_heterogenity:
                        duplicates.append(pair)
                        current_size += 1
                        if current_size >= duplicate_pairs:
                            break
            if current_size >= duplicate_pairs:
                break
    
    return duplicates

def remove_statistics(documents):
    for doc in documents:
        if "statistics" in doc:
            del doc["statistics"]
    return documents

# Schreibt Ergebnisse in eine CSV Datei 
def write_to_csv(filename, uniques, duplicates):
    # Erstelle leere Liste für alle Dokumente (Uniques und Duplikate)
    all_documents = uniques + [document for pair in duplicates for document in pair]

    # Remove "statistics" from uniques and duplicates
    all_documents_removed_statistic = remove_statistics(all_documents)

    # Normalisiere das JSON, um die verschachtelten Attribute in separate Spalten umzuwandeln
    df = pd.json_normalize(all_documents_removed_statistic)

    # Schreibe das DataFrame in die CSV-Datei (ohne Index)
    df.to_csv(filename, index=False)


# Schreibt Parameter values in eine JSON-Datei 
def write_parameters_to_json(json_filename, parameters):
    with open(json_filename, mode='w') as json_file:
        json.dump(parameters, json_file, indent=4)

parameters = {
    'desired_size': desired_size,
    'overlap_for_snapshots': overlap_for_snapshots,
    'snapshot_number': snapshot_number,
    'snapshot1': snapshot1, 
    'snapshot2':snapshot2,
    'year_min': year_min, 
    'year_max': year_max,
    'min_heterogenity': min_heterogenity,
    'max_heterogenity': max_heterogenity,
    'seed': seed,
    'batch_size': batch_size,
    'max_cluster_number': max_cluster_number,
    #'execution_time':execution_time  
}

#------------------------------------------
start_time = time.time()#Zeit messen - START

all_found_uniques, all_found_duplicates = process_clusters_in_batches()
generated_duplicates = get_duplicates(all_found_duplicates)

# Schreiben der gefundenen Daten 
write_to_csv("ergebnis.csv",all_found_uniques, generated_duplicates)
write_parameters_to_json("parameters.json", parameters)

end_time = time.time()#Zeit messen - ENDE
execution_time = end_time - start_time
print(f"Process finished! Execution Time: {execution_time} (Sekunden)")

