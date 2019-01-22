/***********************************************************************************************************************************************
************************************ FONCTIONS UTILES ******************************************************************************************
************************************************************************************************************************************************/


import org.apache.spark.sql.DataFrame
import java.io._
import java.nio.file._
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.Files.copy

//Supprimer les variables : ":reset"
//Supprimer un tempView spark.catalog.dropTempView("NOM")
// Enregistrer la liste des temps : saveTimes(liste)


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
	//Path d'installation de SPARK
	val pathSpark = "C:"+File.separator+"spark"+File.separator
	//Path dans lequel sont stockés les fichiers de résultats
    val repertoireResultats = "Projet-BigData"+File.separator+"résultats"+File.separator 
    //Path de travail pour enregistrer les fichiers
    val repertoireTravail = "Projet-BigData"+File.separator+"temporaire"+File.separator
    //Sous path de travail, pour le traitement de ce fichier particulier
    val repertoireTemporaire = repertoireTravail+nomFichier
    //On s'assure que le repertoire de résulats existe
    
    if(!Files.exists(Paths.get(new File(repertoireResultats).getPath))){
    	Files.createDirectory(Paths.get(new File(repertoireResultats).getPath))
    }

    //On stocke le fichier dnas le repertoire temporaire -> 
    df.repartition(1).write.
        format("com.databricks.spark.csv").
        option("header", true).
        option("delimiter", sep).
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
		Paths.get(new File(pathSpark+pathFichierRecherche).getPath),
		Paths.get(pathSpark+repertoireResultats+nomFichier),
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
