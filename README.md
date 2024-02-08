# check hadoop install (with user hadoop)
su hadoop
start-all.sh
hdfs dfs -ls /user/

# check build (with user ubuntu)
cd ~/Downloads/mnmcount/scala
sbt clean package

# run and check output (files should be refreshedi - with user ubuntu)
spark-submit --class main.scala.mnmc.MnMcount target/scala-2.12/main-scala-mnmc_2.12-1.0.jar data/mnm_dataset.csv
hdfs dfs -ls /user/test
ll output

# check spark shell (with user ubuntu)
spark-shell & echo "LAUNCHING spark-shell"
crtl+c to close

# check airflow (user ubuntu) ; in 2 separate windows:
airflow webserver
airflow scheduler
After run is complete --> crtl+c to close each one

# close all hadoop processes (user hadoop)
su hadoop
stop-all.sh

# fffff
Créer un fichier home/workspace/data/off_raw/

sudo mkdir -p /home/workspace/data/off_raw/


données les droit au fichier 
sudo chown -R ubuntu:ubuntu /home/workspace/data/off_raw/

sudo -chmod -R 777 /nom_du_repertoire

créer sur hdfs 
hdfs dfs -mkdir -p /nom du repertoire

données les droit 

 hdfs dfs sudo chown -R ubuntu:ubuntu /home/workspace/data/off_raw/
hdfs dfs sudo -chmod -R 777 /nom_du_repertoire


donnees les droit sur son projet 

chmood -R 777 ,/

#
Methode avec SH LES etapes a suivre..............
creer un fichier Orcherstrateur.sh pour les données avec crumbtable a la racine du project 
creer run.sh dans scala
********************************************************************************************

stockage de API en local et sur HDFS (Orcherstrateur.sh)

données les droits sur mon dossier bigdata : chmod -R 777 ./

***************************************************************
injection.py pour recuperer API  et stockage sur hdfs et en local 
- creer un dossier Datalake sur hadoop 
-données les droit sur le fichier : hdfs dfs -chmod 777 /user/Datalake2/   **2eme commande **  hdfs dfs -chown ubuntu:ubuntu /user/Datalake2/

************************************************
Orcherstrateur.sh donneés les droit sur hdfs en ligne de commande
creation des dossier : hdfs dfs -mkdir -p /chemin du fichier
                       hdfs dfs -ls -R /
                       hdfs dfs -chmod -R 777 /chemin vers le fichier

********************************************************
configurer crontab 
sudo crontab -e
apres selectionné 1 
ajoute sa a la fin         0 * * * * sh /home/ubuntu/mnmcount/Orchestrateur.sh

afficher le contenue crontab -l
**********************************************

