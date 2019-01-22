import org.apache.spark.sql.functions._


/***********************************************************************************************************************************************
************************************ PREPARATION DES DONNEES************************************************************************************
************************************************************************************************************************************************/

val clients = spark.read.format("csv").option("header", "true").load("BIG-DATA/donnees/olist_customers_dataset.csv").drop("customer_zip_code_prefix")
val commandes = spark.read.format("csv").option("header", "true").load("BIG-DATA/donnees/olist_orders_dataset.csv").drop("order_purchase_timestamp","  order_approved_at","order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date","order_approved_at")
val etats = spark.read.format("csv").option("header", "true").load("BIG-DATA/donnees/abreviations_etats.csv")
val contenusCommandes = spark.read.format("csv").option("header", "true").load("BIG-DATA/donnees/olist_order_items_dataset.csv").drop("shipping_limit_date")
val paiements = spark.read.format("csv").option("header", "true").load("BIG-DATA/donnees/olist_order_payments_dataset.csv")
val avis = spark.read.format("csv").option("header", "true").load("BIG-DATA/donnees/olist_order_reviews_dataset.csv").drop("review_comment_title","review_comment_message","review_creation_date","review_answer_timestamp")
val produitsEspagnols = spark.read.format("csv").option("header", "true").load("BIG-DATA/donnees/olist_products_dataset.csv").drop("product_description_lenght","product_name_lenght","product_photos_qty","product_weight_g","product_length_cm","product_height_cm","product_width_cm")
val vendeurs = spark.read.format("csv").option("header", "true").load("BIG-DATA/donnees/olist_sellers_dataset.csv").drop("seller_zip_code_prefix")
val produitsTraduction = spark.read.format("csv").option("header", "true").load("BIG-DATA/donnees/product_category_name_translation.csv")
val geoloc = spark.read.format("csv").option("header", "true").load("BIG-DATA/donnees/olist_geolocation_dataset.csv")
val produits = produitsEspagnols.join(produitsTraduction,"product_category_name").drop("product_category_name")

//initialisation d'une  liste pour stocker les temps d'execution des sauvegardes des résultats
val liste = Array.empty[(String, Long)]

/***********************************************************************************************************************************************
************************************  NOMBRE DE CLIENTS ET DE COMMANDES PAR VILLES ET ETATS *****************************************************
************************************************************************************************************************************************/

//Jointure clients-etats -> Permet d'obtenir les noms des états exacts
val clientsEtats = clients.join(etats, col("customer_state")===col("code_etat")).drop("customer_state","code_etat").withColumnRenamed("customer_city","Ville")
//Jointure clients-commandes -> Permet d'obtenir les clients ayant effectués chaque commande
val commandesClientsLocalisation = clientsEtats.join(commandes,"customer_id").coalesce(3)

//Calcul du nombre de commandes par états
val nbCommandesParEtat =  commandesClientsLocalisation.groupBy("Etat").agg(expr("count(order_id) as nbCommandes")).sort(desc("nbCommandes"))
//Calcul du nombre de commandes par ville
val nbCommandesParVilleTemp =  commandesClientsLocalisation.groupBy("Ville").agg(expr("count(order_id) as nbCommandes")).sort(desc("nbCommandes"))

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

//Jointure commande-contenuCommande -> permet d'avoir le détail des commandes et le client
val contenuCommandesClients = contenusCommandes.join(commandesClientsLocalisation,"order_id").coalesce(3)

// Recherche d'informations pour chaque commande : prix total des produits, frais total
val infosGeneralesParCommandes = contenuCommandesClients.groupBy("order_id", "Ville", "Etat").agg(
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


/***********************************************************************************************************************************************
************************************  INFORMATIONS SUR LES PRODUITS DES COMMANDES **************************************************************
************************************************************************************************************************************************/

//Jointure contenuCommande-produits -> pour ajouter le détail des produits de chaque commande
val produitsCommandes = contenuCommandesClients.join(produits,"product_id").coalesce(3)

//Nombre de commandes par catégorie de produits, avec le prix moyen et les frais de ports moyens
val nbProduitsCategoriesTemp = produitsCommandes.groupBy("product_category_name_english").
agg(
	expr("count(*) AS nbCommandes"),
	expr("avg(price) AS prixMoyen"),
	expr("avg(freight_value) AS fraisMoyen")
).sort(desc("nbCommandes")).coalesce(3)

//on cherche le nombre de vente moyen
val venteMoyenne = nbProduitsCategoriesTemp.agg(avg("nbCommandes"))
val limite = venteMoyenne.select(col("avg(nbCommandes)")).first.getDouble(0)
//pour ne garder que les ventes au dessus de la moyenne (mieux qu'une valeur fixée aléatoirement)
val nbProduitsCategories = nbProduitsCategoriesTemp.filter($"nbCommandes">limite)

//Enregistrement des résultats
val temp = liste.clone
val liste = saveDfToCsv(nbProduitsCategories,"CATEGORIES-nbProduitsCategories.csv",temp)


/***********************************************************************************************************************************************
************************************  NOTES DES COMMANDES **************************************************************************************
************************************************************************************************************************************************/

//Jointure avis - commandesClientsLocalisation -> permet d'avoir les informations client des avis
val avisCommandesClients = commandesClientsLocalisation.join(avis,"order_id").coalesce(3)
//Note moyenne donnée aux commandes 
val noteMoyenne = avisCommandesClients.agg(
	expr("avg(review_score) AS noteMoyenne")
)
//Note moyenne des commandes par état
val noteMoyenneEtat = avisCommandesClients.groupBy("Etat").agg(
	expr("avg(review_score) AS noteMoyenneEtat")
)
val noteMoyenneVille = avisCommandesClients.groupBy("Ville").agg(
	expr("avg(review_score) AS noteMoyenneEtat")
)
val temp = liste.clone
val liste = saveDfToCsv(noteMoyenne,"GENERAL-noteMoyenne.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(noteMoyenneEtat,"ETAT-noteMoyenneEtat.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(noteMoyenneVille,"VILLE-noteMoyenneVille.csv",temp)


//Jointure avis - produitsCommandes -> permet d'avoir les notes des commandes avec les catégories des produits
// /!\ la note est sur la commande générale -> on considère donc que la note est valable pour toutes les catégories de produits de la commande

val avisProduitsCommandes = produitsCommandes.join(avis,"order_id").coalesce(3)
//Note moyenne des catégories de produit
val noteMoyenneCategorieProduit = avisProduitsCommandes.groupBy("product_category_name_english").agg(
	expr("avg(review_score) AS noteMoyenneCategorieProduit")
)
val temp = liste.clone
val liste = saveDfToCsv(noteMoyenneCategorieProduit,"CATEGORIES-noteMoyenneCategorieProduit.csv",temp)

/***********************************************************************************************************************************************
************************************ VENDEURS **************************************************************************************************
************************************************************************************************************************************************/

//Jointure vendeurs - etats -> Permet d'obtenir les noms des états exacts
val jointureVendeursEtats = vendeurs.join(etats, col("seller_state")===col("code_etat")).drop("seller_state","code_etat").withColumnRenamed("seller_city","Ville")

val nbVendeursParVille =  jointureVendeursEtats.groupBy("Etat","Ville").agg(expr("count(seller_id) as nbVendeurs")).sort(desc("nbVendeurs"))
val nbVendeursParEtat =  jointureVendeursEtats.groupBy("Etat").agg(expr("count(seller_id) as nbVendeurs")).sort(desc("nbVendeurs"))


val temp = liste.clone
val liste =	saveDfToCsv(nbVendeursParVille,"VILLE-nbVendeursParVille.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(nbVendeursParEtat,"ETAT-nbVendeursParEtat.csv",temp)


/***********************************************************************************************************************************************
************************************ INFORMATIONS SUR LES LOCALISATIONS DES VILLES *************************************************************
************************************************************************************************************************************************/

//Moyenne des latitutes et longitudes pour les placer sur les cartes

val localisationVilles = geoloc.groupBy("geolocation_city").agg(
expr("avg(geolocation_lat) AS latitude"),
expr("avg(geolocation_lng) AS longitude")).sort(asc("geolocation_city")).coalesce(3)

val temp = liste.clone
val liste = saveDfToCsv(localisationVilles,"VILLE-localisationVilles.csv",temp)


/***********************************************************************************************************************************************
************************************ INFORMATIONS SUR LES PAIEMENTS ****************************************************************************
************************************************************************************************************************************************/

//Jointure des paiements et des commandes 
val paiementsCommandesClients = commandesClientsLocalisation.join(paiements,"order_id").coalesce(3)
//Calcul du nombre moyen de mode de paiements demandés ppur chaque commande
val nbModePaiements = (paiementsCommandesClients.groupBy("order_id").agg(expr("count(payment_sequential) AS nbModePaiements")).agg(expr("avg(nbModePaiements) AS nbModePaiementsMoyen")))

// Informations sur les modes de paiements utilisés

//Calcul du nombre d'utlisation de chaque mode de paiements
val nbUtilisationModePaiement = paiementsCommandesClients.groupBy("payment_type").agg(expr("count(*) AS nombreUtilisation")).sort(desc("nombreUtilisation"),desc("payment_type"))
//Utilisation de chaque mode de paiement pour chaque état
val nbUtilisationModePaiementEtat = paiementsCommandesClients.groupBy("Etat","payment_type").agg(expr("count(*) AS nombreUtilisation")).sort(asc("Etat"))

val essai = nbUtilisationModePaiementEtat.map(ligne => )

val temp = liste.clone
val liste = saveDfToCsv(nbUtilisationModePaiement,"nbUtilisationModePaiement.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(nbUtilisationModePaiementEtat,"nbUtilisationModePaiementEtat.csv",temp)


//Informations sur les montants des paiements
//Nombre de versements moyen
val nbVersementsMoyen = paiementsCommandesClients.agg(expr("avg(payment_installments) AS nbVersementsMoyen"))
//Nombre de versements moyens selon les modes
val nbVersementsMoyenMode = paiementsCommandesClients.groupBy("payment_type").agg(expr("avg(payment_installments) AS nbVersementsMoyenMode"))
//Nombre de versement moyen selon les états
val nbVersementMoyenEtat = paiementsCommandesClients.groupBy("Etat").agg(expr("avg(payment_installments) AS nbVersementsMoyenEtat"))
//Nombre de versement moyen selon les états pour les cartes de crédits
val nbVersementMoyenEtatCarte = paiementsCommandesClients.filter($"payment_type" === "credit_card").groupBy("Etat").agg(expr("avg(payment_installments) AS nbVersementsMoyenEtatCarte"))

val temp = liste.clone
val liste = saveDfToCsv(nbVersementsMoyen,"nbVersementsMoyen.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(nbVersementsMoyenMode,"nbVersementsMoyenMode.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(nbVersementMoyenEtat,"nbVersementMoyenEtat.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(nbVersementMoyenEtatCarte,"nbVersementMoyenEtatCarte.csv",temp)



/***********************************************************************************************************************************************
************************************ SAUVEGARDE DES RESULTATS **********************************************************************************
************************************************************************************************************************************************/

saveTimes(liste)





/***********************************************************************************************************************************************
************************************ JOINTURES DES INDICATEURS *********************************************************************************
************************************************************************************************************************************************/
/*
JOINTURE DES DONNEES SUR LES ETATS 
*/
val indicateursEtats = nbCommandesParEtat.join(infosGeneralesCommandesEtats, "Etat").join(noteMoyenneEtat, "Etat").join(nbVendeursParEtat, "Etat")
val temp = liste.clone
val liste = saveDfToCsv(indicateursEtats,"GLOBAL_indicateursEtats.csv",temp)

/*
JOINTURE DES DONNEES SUR LES VILLES 
*/
val indicateursVilles= nbCommandesParVille.join(infosGeneralesCommandesVilles, "Ville").join(noteMoyenneVille, "Ville").join(nbVendeursParVille, "Ville")
val temp = liste.clone
val liste = saveDfToCsv(indicateursVilles,"GLOBAL_indicateursVilles.csv",temp)

/*
JOINTURE DES DONNEES SUR LES CATEGORIES 
*/
val indicateursCategorie = nbProduitsCategories.join(noteMoyenneCategorieProduit, "product_category_name_english")
val temp = liste.clone
val liste = saveDfToCsv(indicateursCategorie,"GLOBAL_indicateursCategorie.csv",temp)

/*
JOINTURE DES DONNEES SUR LES TYPES DE PAIEMENTS 
*/
val indicateursTypesPaiements = nbUtilisationModePaiement.join(nbVersementsMoyenMode,"payment_type")
val temp = liste.clone
val liste = saveDfToCsv(nbVersementsMoyen,"GLOBAL_indicateursTypesPaiements.csv",temp)




