/***********************************************************************************************************************************************
************************************  PRIX --> NOMBRE DE PAIEMENTS *****************************************************************************
************************************************************************************************************************************************/


val montantCommandes = infos_commandes_clients_localisations.groupBy("order_id").agg(expr("(sum(freight_value) + sum(price)) as montantCommande")).join(paiements,"order_id")

val montantPaiementsCommandes = montantCommandes.groupBy("order_id","montantCommande").agg(expr("count(*) as nbPaiements"))

val donnees = montantPaiementsCommandes.select(
	montantPaiementsCommandes("nbPaiements").as("label"),
	montantPaiementsCommandes("montantCommande")
)

val echantillonEntrainement = donnees.orderBy(rand()).limit(25000) 
val echantillonsTest = donnees.orderBy(rand()).limit(25000)


//Création des features des deux echantillons
val assembler = new VectorAssembler()
assembler.setInputCols(Array("montantCommande")) // Colonne que l'on veut étudier
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
//Qualité d'ajustement r2 =  1.6119189423569047E-5, 


//Applications sur les données de test
val predictions = model.transform(test)
predictions.show

//Evaluation de la regression
val regEval = new RegressionEvaluator().setMetricName("rmse")
val eval = regEval.evaluate(predictions) // Erreur quadratique
// Erreur  0.4134633913660661
//rmse : plus c'est petit, plus la variance de l'erreur de prévision est faible


//On stocke les résultats
val regressionMontantNbPaiement = predictions.select(predictions("label").as("nbPaiements"),predictions("montantCommande"),predictions("prediction").as("nbPaiementPrediction"))
//On ajoute un id
val regressionMontantNbPaiement_resultat = regressionMontantNbPaiement.select("*").withColumn("id", monotonically_increasing_id())
//On recupère les indicateurs
val indicateurs = sc.parallelize(Seq((r2,eval))).toDF("r2","erreurQuadratique")

val temp = liste.clone
val liste = saveDfToCsv(regressionMontantNbPaiement_resultat,"REGRESSION-regressionMontantNbPaiement-resultat.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(indicateurs,"REGRESSION-regressionMontantNbPaiement-indicateurs.csv",temp)

