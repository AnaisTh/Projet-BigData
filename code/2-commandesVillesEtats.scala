/***********************************************************************************************************************************************
************************************  NOMBRE DE CLIENTS ET DE COMMANDES PAR VILLES ET ETATS *****************************************************
************************************************************************************************************************************************/

//Calcul du nombre de commandes par états
val nbCommandesParEtat =  commandes_clients_localisation.groupBy("Etat").agg(expr("count(order_id) as nbCommandes")).sort(desc("nbCommandes"))
//Calcul du nombre de commandes par ville
val nbCommandesParVilleTemp =  commandes_clients_localisation.groupBy("Ville").agg(expr("count(order_id) as nbCommandes")).sort(desc("nbCommandes"))

//on cherche le nombre moyen de commande par ville
val nbMoyen = nbCommandesParVilleTemp.agg(avg("nbCommandes"))
val limite = nbMoyen.select(col("avg(nbCommandes)")).first.getDouble(0)
//pour ne garder que les ventes au dessus de la moyenne (mieux qu'une valeur fixée aléatoirement)
val nbCommandesParVille = nbCommandesParVilleTemp.filter($"nbCommandes">limite)

//On fait la liste des villes que l'on garde ici pour réutiliser par la suite
val villesConservees = nbCommandesParVille.select("Ville")
val listeVille = villesConservees.select('Ville).as[String].collect

//Ppur filter une DF selon la colonne Ville et cette liste de villes gardées
// -> df.filter(!col("Ville").isin(listeVille : _*))

//Enregistrement des résultats
val temp = liste.clone
val liste =	saveDfToCsv(nbCommandesParVille,"VILLE-nbCommandesParVille.csv", temp )
val temp = liste.clone
val liste = saveDfToCsv(nbCommandesParEtat,"ETAT-nbCommandesParEtat.csv",temp)


/***********************************************************************************************************************************************
************************************  INFORMATIONS GENERALES SUR LES COMMANDES *****************************************************************
************************************************************************************************************************************************/

// Recherche d'informations pour chaque commande : prix total des produits, frais total
val infosGeneralesParCommandes = infos_commandes_clients_localisations.groupBy("order_id", "Ville", "Etat").agg(
	expr("sum(price) AS prixProduits"), 
	expr("sum(freight_value) AS fraisTotal"),
	expr("(sum(freight_value) + sum(price)) AS prixTotal")
).coalesce(3)

val infosGeneralesCommandes = infosGeneralesParCommandes.agg(
	expr("avg(prixProduits) AS prixMoyenProduits"),
	expr("avg(fraisTotal) AS fraisMoyen")
)

//Infos générales réparties selon les états
val infosGeneralesCommandesEtats =  infosGeneralesParCommandes.
groupBy("Etat").
agg(
	expr("avg(prixProduits) AS prixMoyenProduits"),
	expr("avg(fraisTotal) AS fraisMoyen")
).sort(desc("prixMoyenProduits"))

//Infos générales réparties selon les villes
val infosGeneralesCommandesVilles =  infosGeneralesParCommandes.
groupBy("Ville").
agg(
	expr("avg(prixProduits) AS prixMoyenProduits"),
	expr("avg(fraisTotal) AS fraisMoyen")
).sort(desc("prixMoyenProduits"))

//Enregistrement des résultats
val temp = liste.clone
val liste =	saveDfToCsv(infosGeneralesCommandes,"GENERAL-infosGeneralesCommandes.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(infosGeneralesCommandesVilles,"VILLE-infosGeneralesCommandesVilles.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(infosGeneralesCommandesEtats,"ETAT-infosGeneralesCommandesEtats.csv",temp)

