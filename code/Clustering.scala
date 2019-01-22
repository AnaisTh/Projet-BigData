import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler

/***********************************************************************************************************************************************
************************************  CLUSTERING DES CATEGORIES ********************************************************************************
************************************************************************************************************************************************/
//K-Means : Regroupement des catégories de produits qui se ressemblent de par les résultats pour les vendre ensemble

/*
Avant d'effectuer les k-means, je normalise les valeurs afin que toutes les variables aient le même poids. 
Sinon, c'est uniquement le nombre de commande pour chaque catégorie qui est utilisé pour prendre la decision
*/

//On recherche les extremums de chaque colonne
val extremums = nbProduitsCategoriesSuperieurMoyenne.agg(
	expr("min(nbCommandes)"),
	expr("max(nbCommandes)"),
	expr("min(prixMoyen)"),
	expr("max(prixMoyen)"),
	expr("min(fraisMoyen)"),
	expr("max(fraisMoyen)")).head()

//On normalise en faisant colonne / (max - min)
val temp1 = nbProduitsCategoriesSuperieurMoyenne.withColumn("nbCommandes-normalisé",col("nbCommandes").divide(extremums.getLong(1)-extremums.getLong(0)))
val temp2 = temp1.withColumn("prixMoyen-normalisé",col("prixMoyen").divide(extremums.getDouble(3)-extremums.getDouble(2)))
val donneesNormalises = temp2.withColumn("fraisMoyen-normalisé",col("fraisMoyen").divide(extremums.getDouble(5)-extremums.getDouble(4)))

//Création nouvelle colonne feature pour l'étude avec les valeurs à étudier
val assembler = new VectorAssembler()
assembler.setInputCols(Array("nbCommandes-normalisé","prixMoyen-normalisé","fraisMoyen-normalisé")) // Colonne que l'on veut étudier
assembler.setOutputCol("features")
val donneesEtudeKmeans = assembler.transform(donneesNormalises)

val nbCluster = 6
val kmeans = new KMeans().setK(nbCluster).setSeed(1L)
//Apprentissage
val model = kmeans.fit(donneesEtudeKmeans)
// Predictions
val predictions = model.transform(donneesEtudeKmeans)
// Affiche les resultats
println("Centres des clusters Centers: ")
model.clusterCenters.foreach(println)
//Classement des individus
val resultatsPrediction = model.summary.predictions
val clustersProduits = resultatsPrediction.drop("features")

val temp = liste.clone
val liste = saveDfToCsv(clustersProduits,"clustersProduits-"+nbCluster+".csv",temp)
