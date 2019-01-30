

val data = infosGeneralesParCommandes.join(avisCommandesClients.drop("Etat"), "order_id").drop("Ville").drop("prixTotal").drop("customer_id").drop("customer_unique_id").drop("order_status").drop("review_id").coalesce(3)


val notePrixFraisCommandes = data.selectExpr("cast(review_score as int) review_score", 
                        "order_id", 
                        "prixProduits", 
                        "fraisTotal")

/***********************************************************************************************************************************************
************************************  PRIX --> FRAIS *******************************************************************************************
************************************************************************************************************************************************/

val prixFraisCommandes = notePrixFraisCommandes.select(notePrixFraisCommandes("fraisTotal").as("label"),notePrixFraisCommandes("prixProduits"))

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
//Evaluation de la regression
val regEval = new RegressionEvaluator().setMetricName("rmse")
val eval = regEval.evaluate(predictions) // Erreur quadratique
// Erreur
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


/*


/***********************************************************************************************************************************************
************************************  PRIX --> NOTE  -> Sur la moyenne sur les catégories *******************************************************
************************************************************************************************************************************************/
PAS REPRESENTATIVE !!!!
//On recupère la note de chaque produit -> quand une commande à plusieurs produits, la meme note est donnée à chaque produit

val prixNoteCommandes = notePrixFraisCommandes.select(notePrixFraisCommandes("review_score").as("label"),notePrixFraisCommandes("prixProduits"))

val echantillonEntrainement = prixNoteCommandes.orderBy(rand()).limit(10000) 
val echantillonsTest = prixNoteCommandes.orderBy(rand()).limit(10000)


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
//Qualité d'ajustement r2 = 1.603162047558726E-13 -> Aucune corrélation

//Applications sur les données de test
val predictions = model.transform(test)
//Evaluation de la regression
val regEval = new RegressionEvaluator().setMetricName("rmse")
val eval = regEval.evaluate(predictions)


//On stocke les résultats
val regressionPrixNote = predictions.select(predictions("label").as("Note"),predictions("prixProduits"),predictions("prediction").as("NotePrediction"))
//On ajoute un id
val regressionPrixNote_Resultat = regressionPrixNote.select("*").withColumn("id", monotonically_increasing_id())
//On recupère les indicateurs
val indicateurs = sc.parallelize(Seq((r2,eval))).toDF("r2","erreurQuadratique")

val temp = liste.clone
val liste = saveDfToCsv(regressionPrixNote_Resultat,"REGRESSION-regressionPrixNote-resultat.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(indicateurs,"REGRESSION-regressionPrixNote-indicateurs.csv",temp)


*/