/***********************************************************************************************************************************************
************************************  PRIX --> NOMBRE DE PAIEMENTS *****************************************************************************
************************************************************************************************************************************************/

val donnees = paiements.filter($"payment_type" === "credit_card").select(
paiements("payment_installments").as("label").cast("double"),
paiements("payment_value").cast("double"))

val echantillonEntrainement = donnees.orderBy(rand()).limit(30000) 
val echantillonsTest = donnees.orderBy(rand()).limit(30000)


//Création des features des deux echantillons
val assembler = new VectorAssembler()
assembler.setInputCols(Array("payment_value")) // Colonne que l'on veut étudier
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
//Qualité d'ajustement r2 = 0.14429614711634886 


//Applications sur les données de test
val predictions = model.transform(test)
predictions.show

//Evaluation de la regression
val regEval = new RegressionEvaluator().setMetricName("rmse")
val eval = regEval.evaluate(predictions)  // Erreur quadratique
// Erreur  2.672310433013765

//On stocke les résultats
val regressionMontantNbPaiement = predictions.select(predictions("label").as("nbPaiements"),predictions("payment_value").as("montant"),predictions("prediction").as("nbPaiementPrediction"))
//On ajoute un id
val regressionMontantNbPaiement_resultat = regressionMontantNbPaiement.select("*").withColumn("id", monotonically_increasing_id())
//On recupère les indicateurs
val indicateurs = sc.parallelize(Seq((r2,eval))).toDF("r2","erreurQuadratique")

val temp = liste.clone
val liste = saveDfToCsv(regressionMontantNbPaiement_resultat,"REGRESSION-regressionMontantNbPaiement-resultat.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(indicateurs,"REGRESSION-regressionMontantNbPaiement-indicateurs.csv",temp)

