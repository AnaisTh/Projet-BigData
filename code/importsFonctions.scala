
/***********************************************************************************************************************************************
************************************ IMPORT UTILES ******************************************************************************************
************************************************************************************************************************************************/

import org.apache.spark.sql.DataFrame
import java.io._
import java.nio.file._
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.Files.copy
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler


//initialisation d'une  liste pour stocker les temps d'execution des sauvegardes des résultats
val liste = Array.empty[(String, Long)]

/***********************************************************************************************************************************************
************************************ FONCTIONS UTILES ******************************************************************************************
************************************************************************************************************************************************/

//Fonction permettant de déplacer un fichier d'une adresse à l'autre
def moveFileTo(file: Path, targetDir: Path): Path = {
    val filename = file.getFileName
    val newLocation = targetDir.resolve(filename)
    Files.move(file, newLocation, REPLACE_EXISTING)
    newLocation
  }

//Fonction permettant de supprimer un repertoire selon son adresse
def supprimerRepertoire(pathRepertoire : String){
 		val suppr = new File(pathRepertoire)
    	suppr.listFiles.foreach( f => f.delete)
    	suppr.delete
 	}

//Fonction permettant d'enregistrer une DF en csv. On stocke les temps d'executions dans une liste
def saveDfToCsv(df: DataFrame, 
	nomFichier: String, 
	liste : Array[(String,Long)] ): Array[(String,Long)] = {
	
	val t0 = System.nanoTime()
	val sep = ","
	
    // Repertoire temporaire de travail
    val repertoireTravail = repertoireStockageProjet+"temporaire/"
    //Sous repertoire de travail de travail, pour le traitement de ce fichier particulier
    val repertoireTemporaire = repertoireTravail+nomFichier
    //On s'assure que le repertoire de résulats existe
    
    if(!Files.exists(Paths.get(new File(repertoireResultats).getPath))){
    	Files.createDirectory(Paths.get(new File(repertoireResultats).getPath))
    }

    //On stocke le fichier dnas le repertoire temporaire -> 
    df.repartition(1).write.format("com.databricks.spark.csv").option("header", true).option("delimiter", sep).
        save(repertoireTemporaire)
    
    //On récupère le repertoire temporaire 
    val dir = new File(repertoireTemporaire)
  	val newFileRgex = "part-00000.*.csv"
  	//On récupère le fichier qui vient d'être ajouté
    val pathFichierRecherche = dir.listFiles.filter(_.getName.toString.matches(newFileRgex))(0).toString
    //On va déplacer le fichier :
    //On connait son adresse, mais il faut ajouter le chemin entier
    //On le place dans le repertoire de travail auquel on ajoute le path de spark, et le nom du fichier
	val path = Files.move(
		Paths.get(new File(repertoireSpark+pathFichierRecherche).getPath),
		Paths.get(repertoireSpark+repertoireResultats+nomFichier),
		StandardCopyOption.REPLACE_EXISTING)
	
    supprimerRepertoire(repertoireTemporaire)
	supprimerRepertoire(repertoireTravail)

	val t1 = System.nanoTime()
	val time = t1 - t0
	val liste2 = liste :+ (nomFichier,time/1000000000)
	liste2
}

//Fonction permettant d'enregistrer la lsite des temps d'exécutions
def saveTimes(liste : Array[(String,Long)]){
	val times_df = sc.parallelize(liste).toDF("fichier","temps (sec)")
	saveDfToCsv(times_df,"TEMPS.csv",liste)
}
