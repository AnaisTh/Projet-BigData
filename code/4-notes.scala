/***********************************************************************************************************************************************

INFORMATIONS SUR LES NOTES DES COMMANDES
Calcul des notes moyennes selon les villes et les états, mais aussi les notes des catégories

************************************************************************************************************************************************/


//Jointure avis - commande_client_localisation -> permet d'avoir les informations client des avis
val avisCommandesClients = commandes_clients_localisation.join(avis,"order_id").coalesce(3)
//Note moyenne donnée aux commandes 
val noteMoyenne = avisCommandesClients.agg(
	expr("avg(review_score) AS noteMoyenne")
)
//Note moyenne des commandes par état
val noteMoyenneEtat = avisCommandesClients.groupBy("Etat").agg(
	expr("avg(review_score) AS noteMoyenneEtat")
)
val noteMoyenneVille = avisCommandesClients.groupBy("Ville").agg(
	expr("avg(review_score) AS noteMoyenneVille")
)
val temp = liste.clone
val liste = saveDfToCsv(noteMoyenne,"GENERAL-noteMoyenne.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(noteMoyenneEtat,"ETAT-noteMoyenneEtat.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(noteMoyenneVille,"VILLE-noteMoyenneVille.csv",temp)


//Jointure avis - produits_infos_commandes_clients_localisation -> permet d'avoir les notes des commandes avec les catégories des produits
// /!\ la note est sur la commande générale -> on considère donc que la note est valable pour toutes les catégories de produits de la commande

val avis_produits_infos_commandes_clients_localisation = produits_infos_commandes_clients_localisation.join(avis,"order_id").coalesce(3)

//Note moyenne des catégories de produit
val noteMoyenneCategorieProduit = avis_produits_infos_commandes_clients_localisation.groupBy("product_category_name_english").agg(
	expr("avg(review_score) AS noteMoyenneCategorieProduit")
)
val temp = liste.clone
val liste = saveDfToCsv(noteMoyenneCategorieProduit,"CATEGORIES-noteMoyenneCategorieProduit.csv",temp)
