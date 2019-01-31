/***********************************************************************************************************************************************
************************************  PRIX --> FRAIS (par commande) ****************************************************************************
************************************************************************************************************************************************/

val infosCommandes = infos_commandes_clients_localisations.groupBy("order_id").agg(
	expr("sum(price) AS prixProduits"), 
	expr("sum(freight_value) AS fraisTotal")
).coalesce(3)

val prixFraisCommandes = infosCommandes.select(infosCommandes("fraisTotal").as("label"),infosCommandes("prixProduits"))

val echantillonEntrainement = prixFraisCommandes.orderBy(rand()).limit(10000) 
val echantillonsTest = prixFraisCommandes.orderBy(rand()).limit(10000)

//Création des features des deux echantillons
val assembler = new VectorAssembler()
assembler.setInputCols(Array("prixProduits")) // Colonne que l'on veut étudier
assembler.setOutputCol("features")
val entrainement = assembler.transform(echantillonEntrainement)
val test = assembler.transform(echantillonsTest)


// Création de la regression à réaliser
val lr = new LinearRegression().
setMaxIter(30).
setRegParam(0.3).
setElasticNetParam(0.8)

//Entrainement pour determiner le modèle
val model = lr.fit(entrainement)
val r2 = model.summary.r2
//Qualité d'ajustement r2 = 0.19, entre 0 et 1 -> proche de 0 -> l'équation determine seulement 20% des données


//Applications sur les données de test
val predictions = model.transform(test)
predictions.show

//Evaluation de la regression
val regEval = new RegressionEvaluator().setMetricName("rmse")
val eval = regEval.evaluate(predictions) // Erreur quadratique
// Erreur 18.85
//rmse : plus c'est petit, plus la variance de l'erreur de prévision est faible


//On stocke les résultats
val regressionPrixFrais = predictions.select(predictions("label").as("Frais"),predictions("prixProduits"),predictions("prediction").as("FraisPredits"))
//On ajoute un id
val regressionPrixFrais_Resultat = regressionPrixFrais.select("*").withColumn("id", monotonically_increasing_id())
//On recupère les indicateurs
val indicateurs = sc.parallelize(Seq((r2,eval))).toDF("r2","erreurQuadratique")

val temp = liste.clone
val liste = saveDfToCsv(regressionPrixFrais_Resultat,"REGRESSION-regressionPrixFrais-resultat.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(indicateurs,"REGRESSION-regressionPrixFrais-indicateurs.csv",temp)
