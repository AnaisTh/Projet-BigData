/***********************************************************************************************************************************************
			REGRESSIONS 
Ce fichier comporte les 3 régressions mise en place ainsi que la fonction générique de réalisation de regression
/!\ Fonction en fin de fichier pour la lisibilité
************************************************************************************************************************************************/


/***********************************************************************************************************************************************/
/*Regression prix --> frais 
Cette regression consiste à recherche un lien de corrélation entre le prix du produit et les frais établis 
***********************************************************************************************************************************************/

val data = infos_commandes_clients_localisations.groupBy("order_id").agg(
	expr("sum(price) AS prixProduits"), 
	expr("sum(freight_value) AS Frais")
).coalesce(3)


val features = Array("prixProduits")
val label = "Frais"
val nbDonneesTest = 10000
val nbDonneesEntrainement = 10000
val nomValeurPrediction = "FraisPredits"
val nomRegression = "regressionPrixFrais"
val temp = liste.clone
val liste = regression(data,features,label,nbDonneesEntrainement,nbDonneesTest,
	nomValeurPrediction,nomRegression,temp)

/***********************************************************************************************************************************************/
/*Regression infos, prix --> frais 
Cette regression consiste à recherche un lien de corrélation entre le prix/infos du produit et les frais établis 
***********************************************************************************************************************************************/

val data = produits_infos_commandes_clients_localisation.select(
	produits_infos_commandes_clients_localisation("product_weight_g").cast("double"),
	produits_infos_commandes_clients_localisation("product_length_cm").cast("double"),
	produits_infos_commandes_clients_localisation("product_height_cm").cast("double"),
	produits_infos_commandes_clients_localisation("product_width_cm").cast("double"),
	produits_infos_commandes_clients_localisation("price").cast("double"),
	produits_infos_commandes_clients_localisation("freight_value").cast("double")
)
val features = Array("product_weight_g","product_length_cm","product_height_cm","product_width_cm","price")
val label = "freight_value"
val nbDonneesTest = 10000
val nbDonneesEntrainement = 10000
val nomValeurPrediction = "FraisPredits"
val nomRegression = "regressionInfosPrixFrais"
val temp = liste.clone
val liste = regression(data,features,label,nbDonneesEntrainement,nbDonneesTest,
	nomValeurPrediction,nomRegression,temp)

/***********************************************************************************************************************************************/
/* REGRESSION prix --> nombre de paiements 
Cette regression consiste à recherche un lien de corrélation entre le prix du montant à payer, et le nombre de paiements 
(uniquement pour les paiements par carte de crédit, les autres n'ont pas plusieurs paiements)	
***********************************************************************************************************************************************/

val data = paiements.filter($"payment_type" === "credit_card").select(
paiements("payment_installments").as("nbPaiements").cast("double"),
paiements("payment_value").as("montant").cast("double"))


val features = Array("montant")
val label = "nbPaiements"
val nbDonneesTest = 25000
val nbDonneesEntrainement = 25000
val nomValeurPrediction = "nbPaiementPrediction"
val nomRegression = "regressionMontantNbPaiement"
val temp = liste.clone
val liste = regression(data,features,label,nbDonneesEntrainement,nbDonneesTest,nomValeurPrediction,nomRegression,temp)

/***********************************************************************************************************************************************

FONCTION GENERIQUE A CHARGER

************************************************************************************************************************************************/

def regression(data: DataFrame, features: Array[(String)], label : String, nbDonneesEntrainement : Integer , nbDonneesTest : Integer,nomValeurPrediction : String, nomRegression : String, listeSauvegarde : Array[(String,Long)] ) : Array[(String,Long)] = {

		val echantillonEntrainement = data.orderBy(rand()).limit(nbDonneesEntrainement) 
		val echantillonsTest = data.orderBy(rand()).limit(nbDonneesTest)
		//Création des features des deux echantillons
		val assembler = new VectorAssembler()
		assembler.setInputCols(features) // Colonne que l'on veut utiliser pour la regression
		assembler.setOutputCol("features")
		val entrainement = assembler.transform(echantillonEntrainement)
		val test = assembler.transform(echantillonsTest)

		// Création de la regression à réaliser
		val lr = new LinearRegression().setLabelCol(label)
		//Entrainement pour determiner le modèle
		val model = lr.fit(entrainement)
		val r2 = model.summary.r2 //Qualité d'ajustement r2
		//Applications sur les données de test ppur determiner les predictions
		val predictions = model.transform(test)
		//Evaluation de la regression
		val regEval = new RegressionEvaluator().setLabelCol(label).setMetricName("rmse")
		val eval = regEval.evaluate(predictions)  // Erreur quadratique

		//On stocke les résultats
		val resultat = predictions.drop("features").withColumnRenamed("prediction",nomValeurPrediction).withColumn("id",monotonically_increasing_id)
		val indicateurs = sc.parallelize(Seq((r2,eval))).toDF("r2","erreurQuadratique")

		val temp = listeSauvegarde.clone
		val listeSauvegarde2 = saveDfToCsv(resultat,"REGRESSION-"+nomRegression+"-resultat.csv",temp)
		val temp2 = listeSauvegarde2.clone
		val listeSauvegarde3 = saveDfToCsv(indicateurs,"REGRESSION-"+nomRegression+"-indicateurs.csv",temp2)

		listeSauvegarde3

}

