/***********************************************************************************************************************************************
************************************ VENDEURS **************************************************************************************************
************************************************************************************************************************************************/

val nbVendeursParVille =  vendeurs_localisation.groupBy("Etat","Ville").agg(expr("count(seller_id) as nbVendeurs")).sort(desc("nbVendeurs"))
val nbVendeursParEtat =  vendeurs_localisation.groupBy("Etat").agg(expr("count(seller_id) as nbVendeurs")).sort(desc("nbVendeurs"))


val temp = liste.clone
val liste =	saveDfToCsv(nbVendeursParVille,"VILLE-nbVendeursParVille.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(nbVendeursParEtat,"ETAT-nbVendeursParEtat.csv",temp)



