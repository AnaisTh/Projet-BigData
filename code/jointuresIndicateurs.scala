/***********************************************************************************************************************************************
************************************  JOINTURES SUR LES ETATS **********************************************************************************
************************************************************************************************************************************************/


val nbCommandesParEtat = spark.read.format("csv").option("header", "true").load(repertoireResultats+"ETAT-nbCommandesParEtat.csv")
val infosGeneralesCommandesEtats = spark.read.format("csv").option("header", "true").load(repertoireResultats+"ETAT-infosGeneralesCommandesEtats.csv")
val noteMoyenneEtat = spark.read.format("csv").option("header", "true").load(repertoireResultats+"ETAT-noteMoyenneEtat.csv")
val nbVendeursParEtat = spark.read.format("csv").option("header", "true").load(repertoireResultats+"ETAT-nbVendeursParEtat.csv")
val nbVersementMoyenEtatCarte = spark.read.format("csv").option("header", "true").load(repertoireResultats+"ETAT-nbVersementMoyenEtatCarte.csv")
val montantMoyenVersementEtatCarte = spark.read.format("csv").option("header", "true").load(repertoireResultats+"ETAT-montantMoyenVersementEtatCarte.csv")
val delaisMoyensEtats = spark.read.format("csv").option("header", "true").load(repertoireResultats+"ETAT-delaisMoyensEtats.csv")


//Des left outer pour garder les valeurs nulls comme dans le cas des vendeurs
val indicateursEtatsTemp = nbCommandesParEtat.join(infosGeneralesCommandesEtats,
	nbCommandesParEtat.col("Etat")===infosGeneralesCommandesEtats.col("Etat"),"left_outer").drop(infosGeneralesCommandesEtats.col("Etat"))
val indicateursEtatsTemp2 = indicateursEtatsTemp.join(noteMoyenneEtat,
	indicateursEtatsTemp.col("Etat")===noteMoyenneEtat.col("Etat"),"left_outer").drop(noteMoyenneEtat.col("Etat"))
val indicateursEtatsTemp3 = indicateursEtatsTemp2.join(nbVendeursParEtat,
	indicateursEtatsTemp2.col("Etat")===nbVendeursParEtat.col("Etat"),"left_outer").drop(nbVendeursParEtat.col("Etat"))

val indicateursEtatsTemp4 = indicateursEtatsTemp3.join(nbVersementMoyenEtatCarte,
	indicateursEtatsTemp3.col("Etat")===nbVersementMoyenEtatCarte.col("Etat"),"left_outer").drop(nbVersementMoyenEtatCarte.col("Etat"))
val indicateursEtatsTemp5 = indicateursEtatsTemp4.join(montantMoyenVersementEtatCarte,
	indicateursEtatsTemp4.col("Etat")===montantMoyenVersementEtatCarte.col("Etat"),"left_outer").drop(montantMoyenVersementEtatCarte.col("Etat"))
val indicateursEtats = indicateursEtatsTemp5.join(delaisMoyensEtats,
	indicateursEtatsTemp5.col("Etat")===delaisMoyensEtats.col("Etat"),"left_outer").drop(delaisMoyensEtats.col("Etat"))

//Ajout de ratios : nombre de vendeurs par commandes par état
val resultat = indicateursEtats.withColumn("nbVendeursParCommande",col("nbVendeurs").divide(col("nbCommandes")))

val temp = liste.clone
val liste = saveDfToCsv(resultat,"GLOBAL_indicateursEtats.csv",temp)

/***********************************************************************************************************************************************
************************************  JOINTURES SUR LES VILLES *********************************************************************************
************************************************************************************************************************************************/

val nbCommandesParVille = spark.read.format("csv").option("header", "true").load(repertoireResultats+"VILLE-nbCommandesParVille.csv")
val infosGeneralesCommandesVilles = spark.read.format("csv").option("header", "true").load(repertoireResultats+"VILLE-infosGeneralesCommandesVilles.csv")
val noteMoyenneVille = spark.read.format("csv").option("header", "true").load(repertoireResultats+"VILLE-noteMoyenneVille.csv")
val nbVendeursParVille = spark.read.format("csv").option("header", "true").load(repertoireResultats+"VILLE-nbVendeursParVille.csv").drop("Etat")
val localisationVilles =  spark.read.format("csv").option("header", "true").load(repertoireResultats+"VILLE-localisationVilles.csv")
	

//Des left outer pour garder les valeurs nulls comme dans le cas des vendeurs
val indicateursVillesTemp = nbCommandesParVille.join(infosGeneralesCommandesVilles,
	nbCommandesParVille.col("Ville")===infosGeneralesCommandesVilles.col("Ville"),"left_outer").drop(infosGeneralesCommandesVilles.col("Ville"))
val indicateursVillesTemp2 = indicateursVillesTemp.join(noteMoyenneVille,
	indicateursVillesTemp.col("Ville")===noteMoyenneVille.col("Ville"),"left_outer").drop(noteMoyenneVille.col("Ville"))
val indicateursVillesTemp3 = indicateursVillesTemp2.join(nbVendeursParVille,
	indicateursVillesTemp2.col("Ville")===nbVendeursParVille.col("Ville"),"left_outer").drop(nbVendeursParVille.col("Ville"))
val indicateursVilles = indicateursVillesTemp3.join(localisationVilles,
	indicateursVillesTemp3.col("Ville")===localisationVilles.col("geolocation_city"),"left_outer").drop(localisationVilles.col("geolocation_city"))

//Ajout de ratios : nombre de vendeurs par commandes par ville
val resultat = indicateursVilles.withColumn("nbVendeursParCommande",col("nbVendeurs").divide(col("nbCommandes")))

val temp = liste.clone
val liste = saveDfToCsv(resultat,"GLOBAL_indicateursVilles.csv",temp)


/***********************************************************************************************************************************************
************************************  JOINTURES SUR LES CATEGORIES *****************************************************************************
************************************************************************************************************************************************/
//Pour récuperer les informations selon un clustering particuleir
val nbCluster = 14

val informationsProduits = spark.read.format("csv").option("header", "true").load(repertoireResultats+"CATEGORIES-informationsProduitsClusters-"+nbCluster+".csv")
val noteMoyenneCategorieProduit = spark.read.format("csv").option("header", "true").load(repertoireResultats+"CATEGORIES-noteMoyenneCategorieProduit.csv")

//Des left outer pour garder les valeurs nulls comme dans le cas des vendeurs
val indicateursCategorie = informationsProduits.join(noteMoyenneCategorieProduit,
	informationsProduits.col("product_category_name_english")===noteMoyenneCategorieProduit.col("product_category_name_english"),"left_outer").
drop(noteMoyenneCategorieProduit.col("product_category_name_english")).withColumnRenamed("product_category_name_english","nomProduit")

//Ajout de ratios : nombre de vendeurs par commandes par categorie de produit
val resultat = indicateursCategorie.withColumn("nbVendeursParCommande",col("nbVendeurs").divide(col("nbCommandes")))

val temp = liste.clone
val liste = saveDfToCsv(resultat,"GLOBAL_indicateursCategorie.csv",temp)


/***********************************************************************************************************************************************
************************************  JOINTURES SUR LES TYPES DE PAIEMENTS  ********************************************************************
************************************************************************************************************************************************/


val nbUtilisationModePaiement = spark.read.format("csv").option("header", "true").load(repertoireResultats+"PAIEMENT-nbUtilisationModePaiement.csv")
val nbVersementsMoyenMode = spark.read.format("csv").option("header", "true").load(repertoireResultats+"PAIEMENT-nbVersementsMoyenMode.csv")

//Des left outer pour garder les valeurs nulls comme dans le cas des vendeurs
val indicateursTypesPaiements = nbUtilisationModePaiement.join(nbVersementsMoyenMode,
	nbUtilisationModePaiement.col("payment_type")===nbVersementsMoyenMode.col("payment_type"),"left_outer").drop(nbVersementsMoyenMode.col("payment_type")).withColumnRenamed("payment_type","typePaiement")

val temp = liste.clone
val liste = saveDfToCsv(indicateursTypesPaiements,"GLOBAL_indicateursTypesPaiements.csv",temp)


saveTimes(liste)