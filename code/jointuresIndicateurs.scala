


//initialisation d'une  liste pour stocker les temps d'execution des sauvegardes des résultats
val liste = Array.empty[(String, Long)]

/*
JOINTURE DES DONNEES SUR LES ETATS 
*/

val nbCommandesParEtat = spark.read.format("csv").option("header", "true").load("Projet-BigData/résultats/ETAT-nbCommandesParEtat.csv")
val infosGeneralesCommandesEtats = spark.read.format("csv").option("header", "true").load("Projet-BigData/résultats/ETAT-infosGeneralesCommandesEtats.csv")
val noteMoyenneEtat = spark.read.format("csv").option("header", "true").load("Projet-BigData/résultats/ETAT-noteMoyenneEtat.csv")
val nbVendeursParEtat = spark.read.format("csv").option("header", "true").load("Projet-BigData/résultats/ETAT-nbVendeursParEtat.csv")
val nbVersementMoyenEtatCarte = spark.read.format("csv").option("header", "true").load("Projet-BigData/résultats/ETAT-nbVersementMoyenEtatCarte.csv")
val montantMoyenVersementEtatCarte = spark.read.format("csv").option("header", "true").load("Projet-BigData/résultats/ETAT-montantMoyenVersementEtatCarte.csv")


//Des left outer pour garder les valeurs nulls comme dans le cas des vendeurs
val indicateursEtatsTemp = nbCommandesParEtat.join(infosGeneralesCommandesEtats,
	nbCommandesParEtat.col("Etat")===infosGeneralesCommandesEtats.col("Etat"),"left_outer").drop(infosGeneralesCommandesEtats.col("Etat"))
val indicateursEtatsTemp2 = indicateursEtatsTemp.join(noteMoyenneEtat,
	indicateursEtatsTemp.col("Etat")===noteMoyenneEtat.col("Etat"),"left_outer").drop(noteMoyenneEtat.col("Etat"))
val indicateursEtatsTemp3 = indicateursEtatsTemp2.join(nbVendeursParEtat,
	indicateursEtatsTemp2.col("Etat")===nbVendeursParEtat.col("Etat"),"left_outer").drop(nbVendeursParEtat.col("Etat"))

val indicateursEtatsTemp4 = indicateursEtatsTemp3.join(nbVersementMoyenEtatCarte,
	indicateursEtatsTemp3.col("Etat")===nbVersementMoyenEtatCarte.col("Etat"),"left_outer").drop(nbVersementMoyenEtatCarte.col("Etat"))
val indicateursEtats = indicateursEtatsTemp4.join(montantMoyenVersementEtatCarte,
	indicateursEtatsTemp4.col("Etat")===montantMoyenVersementEtatCarte.col("Etat"),"left_outer").drop(montantMoyenVersementEtatCarte.col("Etat"))

val temp = liste.clone
val liste = saveDfToCsv(indicateursEtats,"GLOBAL_indicateursEtats.csv",temp)

/*
JOINTURE DES DONNEES SUR LES VILLES 
*/

val nbCommandesParVille = spark.read.format("csv").option("header", "true").load("Projet-BigData/résultats/VILLE-nbCommandesParVille.csv")
val infosGeneralesCommandesVilles = spark.read.format("csv").option("header", "true").load("Projet-BigData/résultats/VILLE-infosGeneralesCommandesVilles.csv")
val noteMoyenneVille = spark.read.format("csv").option("header", "true").load("Projet-BigData/résultats/VILLE-noteMoyenneVille.csv")
val nbVendeursParVille = spark.read.format("csv").option("header", "true").load("Projet-BigData/résultats/VILLE-nbVendeursParVille.csv")
val localisationVilles =  spark.read.format("csv").option("header", "true").load("Projet-BigData/résultats/VILLE-localisationVilles.csv")
	


//Des left outer pour garder les valeurs nulls comme dans le cas des vendeurs
val indicateursVillesTemp = nbCommandesParVille.join(infosGeneralesCommandesVilles,
	nbCommandesParVille.col("Ville")===infosGeneralesCommandesVilles.col("Ville"),"left_outer").drop(infosGeneralesCommandesVilles.col("Ville"))
val indicateursVillesTemp2 = indicateursVillesTemp.join(noteMoyenneVille,
	indicateursVillesTemp.col("Ville")===noteMoyenneVille.col("Ville"),"left_outer").drop(noteMoyenneVille.col("Ville"))
val indicateursVillesTemp3 = indicateursVillesTemp2.join(nbVendeursParVille,
	indicateursVillesTemp2.col("Ville")===nbVendeursParVille.col("Ville"),"left_outer").drop(nbVendeursParVille.col("Ville"))
val indicateursVilles = indicateursVillesTemp3.join(localisationVilles,
	indicateursVillesTemp3.col("Ville")===localisationVilles.col("geolocation_city"),"left_outer").drop(localisationVilles.col("geolocation_city"))

val temp = liste.clone
val liste = saveDfToCsv(indicateursVilles,"GLOBAL_indicateursVilles.csv",temp)

/*
JOINTURE DES DONNEES SUR LES CATEGORIES 
*/

val nbProduitsCategories = spark.read.format("csv").option("header", "true").load("Projet-BigData/résultats/CATEGORIES-nbProduitsCategories.csv")
val noteMoyenneCategorieProduit = spark.read.format("csv").option("header", "true").load("Projet-BigData/résultats/CATEGORIES-noteMoyenneCategorieProduit.csv")

//Des left outer pour garder les valeurs nulls comme dans le cas des vendeurs
val indicateursCategorie = nbProduitsCategories.join(noteMoyenneCategorieProduit,
	nbProduitsCategories.col("product_category_name_english")===noteMoyenneCategorieProduit.col("product_category_name_english"),"left_outer").drop(noteMoyenneCategorieProduit.col("product_category_name_english"))

val temp = liste.clone
val liste = saveDfToCsv(indicateursCategorie,"GLOBAL_indicateursCategorie.csv",temp)

/*
JOINTURE DES DONNEES SUR LES TYPES DE PAIEMENTS 
*/
val nbUtilisationModePaiement = spark.read.format("csv").option("header", "true").load("Projet-BigData/résultats/nbUtilisationModePaiement.csv")
val nbVersementsMoyenMode = spark.read.format("csv").option("header", "true").load("Projet-BigData/résultats/nbVersementsMoyenMode.csv")

//Des left outer pour garder les valeurs nulls comme dans le cas des vendeurs
val indicateursTypesPaiements = nbUtilisationModePaiement.join(nbVersementsMoyenMode,
	nbUtilisationModePaiement.col("payment_type")===nbVersementsMoyenMode.col("payment_type"),"left_outer").drop(nbVersementsMoyenMode.col("payment_type"))

val temp = liste.clone
val liste = saveDfToCsv(indicateursTypesPaiements,"GLOBAL_indicateursTypesPaiements.csv",temp)


saveTimes(liste)