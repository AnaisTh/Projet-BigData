/***********************************************************************************************************************************************

INFORMATIONS SUR LES PRODUITS DES COMMANDES 
Calcul du nombre de commande et du nombre de vendeurs selon les catégories de produits

************************************************************************************************************************************************/


//Nombre de commandes par catégorie de produits, avec le prix moyen et les frais de ports moyens
val nbProduitsCategories = produits_infos_commandes_clients_localisation_vendeurs.groupBy("product_category_name_english").
agg(
	expr("count(*) AS nbCommandes"),
	expr("avg(price) AS prixMoyen"),
	expr("avg(freight_value) AS fraisMoyen")
).sort(desc("nbCommandes")).coalesce(3)


//rdd par défaut : 200 -> réduction à 3
val nbVendeursCategories = (
	produits_infos_commandes_clients_localisation_vendeurs.groupBy("product_category_name_english","seller_id").
	agg(expr("count(*)"))
).groupBy("product_category_name_english").agg((expr("count(*) as nbVendeurs"))).coalesce(3)

val infosCategories = nbProduitsCategories.join(nbVendeursCategories, "product_category_name_english").coalesce(3)



/***********************************************************************************************************************************************

CLUSTERING SUR LES PRODUITS
Regroupements des produits selon différents critères : prix, frais, nombre de vendeurs, nombre de commandes
Le tout sous forme de ratios pour une meilleure clusterisation

************************************************************************************************************************************************/


//On recherche les extremums de chaque colonne
val extremums = infosCategories.agg(
	expr("min(nbCommandes)"), expr("max(nbCommandes)"), 
	expr("min(prixMoyen)"),expr("max(prixMoyen)"), 
	expr("min(fraisMoyen)"),expr("max(fraisMoyen)"),
	expr("min(nbVendeurs)"), expr("max(nbVendeurs)")
).head()


//On récupère ces extremums
val minNbCommandes = extremums.getLong(0); val maxNbCommandes = extremums.getLong(1) ;val nominateurNbCommandes = maxNbCommandes - minNbCommandes
val minPrixMoyen = extremums.getDouble(2); val maxPrixMoyen = extremums.getDouble(3); val nominateurPrixMoyen = maxPrixMoyen - minPrixMoyen
val minFraisMoyen = extremums.getDouble(4); val maxFraisMoyen = extremums.getDouble(5); val nominateurFraisMoyen = maxFraisMoyen - minFraisMoyen
val minNbVendeurs= extremums.getDouble(4); val maxNbVendeurs = extremums.getDouble(5); val nominateurNbVendeurs = maxNbVendeurs - minNbVendeurs


//On normalise en faisant colonne / (max - min) pour ne pas qu'un attribut prenne le dessus sur les autres
val temp1 = infosCategories.withColumn("nbCommandes-normalise",col("nbCommandes").divide(nominateurNbCommandes))
val temp2 = temp1.withColumn("prixMoyen-normalise",col("prixMoyen").divide(nominateurPrixMoyen))
val temp3 = temp2.withColumn("fraisMoyen-normalise",col("fraisMoyen").divide(nominateurFraisMoyen))
val donneesNormalises = temp3.withColumn("nbVendeurs-normalise",col("nbVendeurs").divide(nominateurNbVendeurs))

//Création nouvelle colonne feature pour l'étude avec les valeurs à étudier
val assembler = new VectorAssembler()
assembler.setInputCols(Array("nbCommandes-normalise","prixMoyen-normalise","fraisMoyen-normalise","nbVendeurs-normalise")) // Colonne que l'on veut étudier
assembler.setOutputCol("features")
val donneesEtudeKmeans = assembler.transform(donneesNormalises)

val nbCluster = 14
val kmeans = new KMeans().setK(nbCluster).setSeed(1L)
//Apprentissage
val model = kmeans.fit(donneesEtudeKmeans)
// Predictions
val predictions = model.transform(donneesEtudeKmeans)

// Stockage des centres des clusters
case class Result(index: Integer, nbCommande: Double, prixMoyen: Double, fraisMoyen: Double, nbVendeurs : Double)
val recuperationCentres = model.clusterCenters.zipWithIndex.map{
	case(row,index) => 
	Result(index,row(0)*nominateurNbCommandes,row(1)*nominateurPrixMoyen,row(2)*nominateurFraisMoyen, row(3)*nominateurNbVendeurs)
}
val centresClusters = sc.parallelize(recuperationCentres)toDF("numCluster","centreNbCommande","centrePrixMoyen","centreFraisMoyen","centreNbVendeurs")

//Classement des individus
val resultatsPrediction = model.summary.predictions
val clustersProduits = resultatsPrediction.withColumnRenamed("prediction", "numCluster").drop("features","nbCommandes-normalise","prixMoyen-normalise","fraisMoyen-normalise","nbVendeurs-normalise")

//On ajoute le nombre de produits du clusters à ses informations
val nbProduitsCluster = clustersProduits.groupBy("numCluster").agg(expr("count(*) as nombreProduitsCluster"))

val centresClustersJointure = centresClusters.join(nbProduitsCluster,"numCluster").withColumn("nbVendeursParCommande",col("centreNbVendeurs").divide(col("centreNbCommande")))

//Enregistrement des résultats
val temp = liste.clone
val liste = saveDfToCsv(clustersProduits,"CATEGORIES-informationsProduitsClusters-"+nbCluster+".csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(centresClustersJointure,"CLUSTER-centresclustersProduits-"+nbCluster+".csv",temp)


