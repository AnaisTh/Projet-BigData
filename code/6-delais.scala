/***********************************************************************************************************************************************

ETUDE DES DELAIS

************************************************************************************************************************************************/

//Mettre un simple filtre sur le dataframe crée une dataset donc passage par une vue SQL pour obtenir un DF plutot que de refaire des casts
//val commandesLivrees = commandes.filter($"order_status" === "delivered")
commandes_clients_localisation.createOrReplaceTempView("TEMPORAIRE")

val commandesLivrees  = sql("SELECT * FROM TEMPORAIRE WHERE order_status == 'delivered'")

val commandesLivreesDiffDate = commandesLivrees.
withColumn("DelaiLogistiqueJ",
	(unix_timestamp(commandesLivrees.col("dateValidationLogistique")) - unix_timestamp(commandesLivrees.col("dateAchat")))/3600/24).
withColumn("DelaiLivraisonJ",
	(unix_timestamp(commandesLivrees.col("dateLivraisonEffective")) - unix_timestamp(commandesLivrees.col("dateAchat")))/3600/24).
filter($"DelaiLogistiqueJ">0).
filter($"DelaiLivraisonJ" >0 )
//Certaines commandes on des incohérences de délais, ou des absences de valeurs, donc filtre sur ces infos

val delaisMoyensEtat = commandesLivreesDiffDate.groupBy("Etat").agg(
expr("avg(DelaiLivraisonJ) AS delaiLivraisonMoyenJ"),
expr("avg(DelaiLogistiqueJ) AS delaiLogistiqueMoyenJ")
)

val temp = liste.clone
val liste = saveDfToCsv(delaisMoyensEtat,"ETAT-delaisMoyensEtats.csv",temp)
