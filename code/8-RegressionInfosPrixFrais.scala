
/***********************************************************************************************************************************************
************************************  PRIX, poids, taille --> FRAIS du produit *****************************************************************
************************************************************************************************************************************************/
//"product_weight_g","product_length_cm","product_height_cm","product_width_cm"


val donnees = produits_infos_commandes_clients_localisation.select(
	produits_infos_commandes_clients_localisation("product_weight_g").cast("double"),
	produits_infos_commandes_clients_localisation("product_length_cm").cast("double"),
	produits_infos_commandes_clients_localisation("product_height_cm").cast("double"),
	produits_infos_commandes_clients_localisation("product_width_cm").cast("double"),
	produits_infos_commandes_clients_localisation("price").cast("double"),
	produits_infos_commandes_clients_localisation("freight_value").cast("double").as("label")
)
//C'est les frais que l'on veux etudier donc label


val echantillonEntrainement = donnees.orderBy(rand()).limit(10000) 
val echantillonsTest = donnees.orderBy(rand()).limit(10000)

//Création des features des deux echantillons
val assembler = new VectorAssembler()
assembler.setInputCols(Array(
	"product_weight_g",
	"product_length_cm",
	"product_height_cm",
	"product_width_cm",
	"price")) 
// Colonne que l'on veut étudier
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
//Qualité d'ajustement r2 = 0.43, -> l'équation determine seulement 43% des données


//Applications sur les données de test
val predictions = model.transform(test)

//Evaluation de la regression
val regEval = new RegressionEvaluator().setMetricName("rmse")
val eval = regEval.evaluate(predictions) // Erreur quadratique
// rmse = 11.85 : plus c'est petit, plus la variance de l'erreur de prévision est faible


//On stocke les résultats
val regressionInfosPrixFrais = predictions.select(
	predictions("label").as("Frais"),
	predictions("product_weight_g"),
	predictions("product_length_cm"),
	predictions("product_height_cm"),
	predictions("product_width_cm"),
	predictions("price").as("prixProduits"),
	predictions("prediction").as("FraisPredits")
)
//On ajoute un id
val regressionInfosPrixFrais_Resultat = regressionInfosPrixFrais.select("*").withColumn("id", monotonically_increasing_id())
//On recupère les indicateurs
val indicateurs = sc.parallelize(Seq((r2,eval))).toDF("r2","erreurQuadratique")

val temp = liste.clone
val liste = saveDfToCsv(regressionInfosPrixFrais_Resultat,"REGRESSION-regressionInfosPrixFrais-resultat.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(indicateurs,"REGRESSION-regressionInfosPrixFrais-indicateurs.csv",temp)



