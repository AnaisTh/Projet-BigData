###Projet de BigData - Analyse de données Bresiliens###

# Contenu #
Ce projet contient différents repertoires :
> code : contenant tous les fichiers sources 
> données : repertoire par défaut contenant les données sources
> résutlat : repertoire par défaut contenant les résultats des traitements


# Paramétrage #
Avant d'exécuter les différentes requêtes contenus dans le repertoire "code", il est necessaire de les paramétrer.
Pour cela, il faut remplir les différentes valeurs disponibles dans le fichier "parametres.scala"

Il s'agit de referencer les différents paths necessaire au bon fonctionnement des fonctions mises en places.
Exemple :
> val repertoireStockageProjet = "ProjetBigData_THIRIOT_Anais/"

Après avoir adapté les paramètres, l'ensemble du fichier "parametres.scala" doit être exécuté dans le shell Spark afin de mettre à jour l'environnement.

# Importations des données #

Pour le bon fonctionnement des différentes requêtes, les données doivent être importées :
Il s'agit du fichier 1-préparation.

Suite à cette import de données sources, tous les autres fichiers de traitements peuvent être exécutés. 

# Les fichiers #

> Tous les fichiers numérotés correspondent aux différentes etapes de créations d'indicateurs.

> Le fichier importsFonctions.scala contient l'ensemble des fonctions mise en place pour automatiser la mise en place d'indicateurs.

> Le fichier jointuresIndicateurs.scala permet de faire le lien les différents indicateurs selon leur objectifs


