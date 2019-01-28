
//LE fichier noms entiers est a ajouter dans les données sources !!

/***********************************************************************************************************************************************
************************************ PARAMETRAGE DU PROJET *************************************************************************************
************************************************************************************************************************************************/


//Repertoire dans lequel est stocké le projet
//Path relatif au repertoire spark, ou absolu /!\ SI MODIFIE, LE CORRIGER DANS LA DERNIERE LIGNE
val repertoireStockageProjet = "ProjetBigData_THIRIOT_Anais/"

//Repertoire où se trouvent les données sources, chemins relatif au repertoire spark, ou absolu
val repertoireDonneesSources = repertoireStockageProjet+"donnees/"

//Repertoire où doivent être stockés les résultats, chemins relatif au repertoire spark, ou absolu
val repertoireResultats = repertoireStockageProjet+"résultats/"

//Repertoire où se trouvent les fichiers de code
val repertoireCode = repertoireStockageProjet+"code/"

//Repertoire de d'installation de spark
val repertoireSpark = "C:/spark/"


//CORRIGER LE REPERTOIRE DE STOCKAGE DU PROJET
:load "ProjetBigData_THIRIOT_Anais/code/importsFonctions.scala"

/***********************************************************************************************************************************************
************************************ SAUVEGARDE DES RESULTATS **********************************************************************************
************************************************************************************************************************************************/
//Pour sauvegarder la liste des temps d'éxécutions
//saveTimes(liste)







