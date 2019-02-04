/***********************************************************************************************************************************************
************************************ OUVERTURE DES DONNEES SOURCES *****************************************************************************
************************************************************************************************************************************************/

val clients = spark.read.format("csv").option("header", "true").load(repertoireDonneesSources+"olist_customers_dataset.csv").drop("customer_zip_code_prefix")
val commandes = spark.read.format("csv").option("header", "true").load(repertoireDonneesSources+"olist_orders_dataset.csv").drop("order_estimated_delivery_date","order_approved_at").
withColumnRenamed("order_purchase_timestamp","dateAchat").
withColumnRenamed("order_delivered_carrier_date","dateValidationLogistique").
withColumnRenamed("order_delivered_customer_date","dateLivraisonEffective")
val etats = spark.read.format("csv").option("header", "true").load(repertoireDonneesSources+"abreviations_etats.csv")
val infos = spark.read.format("csv").option("header", "true").load(repertoireDonneesSources+"olist_order_items_dataset.csv").drop("shipping_limit_date")
val paiements = spark.read.format("csv").option("header", "true").load(repertoireDonneesSources+"olist_order_payments_dataset.csv")
val avis = spark.read.format("csv").option("header", "true").load(repertoireDonneesSources+"olist_order_reviews_dataset.csv").drop("review_comment_title","review_comment_message","review_creation_date","review_answer_timestamp")
val produitsEspagnols = spark.read.format("csv").option("header", "true").load(repertoireDonneesSources+"olist_products_dataset.csv").drop("product_description_lenght","product_name_lenght","product_photos_qty")
val vendeurs = spark.read.format("csv").option("header", "true").load(repertoireDonneesSources+"olist_sellers_dataset.csv").drop("seller_zip_code_prefix")
val produitsTraduction = spark.read.format("csv").option("header", "true").load(repertoireDonneesSources+"product_category_name_translation.csv")
val geoloc = spark.read.format("csv").option("header", "true").load(repertoireDonneesSources+"olist_geolocation_dataset.csv")
val produits = produitsEspagnols.join(produitsTraduction,"product_category_name").drop("product_category_name")


/***********************************************************************************************************************************************
************************************  PREPARATION DES INFORMATIONS *****************************************************************************
************************************************************************************************************************************************/
//Jointure clients-etats -> Permet d'obtenir les noms des états exacts
val clients_etats = clients.join(etats, col("customer_state")===col("code_etat")).drop("customer_state","code_etat").withColumnRenamed("customer_city","Ville")
//Jointure clients-commandes -> Permet d'obtenir les clients ayant effectués chaque commande
val commandes_clients_localisation = clients_etats.join(commandes,"customer_id").coalesce(3)

//Jointure commande-contenuCommande -> permet d'avoir le détail des commandes et le client
val infos_commandes_clients_localisations = infos.join(commandes_clients_localisation,"order_id").coalesce(3)

//Jointure vendeurs - etats -> Permet d'obtenir les noms des états exacts
val vendeurs_localisation = vendeurs.join(etats, col("seller_state")===col("code_etat")).drop("seller_state","code_etat").withColumnRenamed("seller_city","Ville")

//Jointure contenuCommande-produits -> pour ajouter le détail des produits de chaque commande
val produits_infos_commandes_clients_localisation = infos_commandes_clients_localisations.join(produits,"product_id").coalesce(3)
//Jointure contenuCommande-produits-vendeurs
val produits_infos_commandes_clients_localisation_vendeurs = produits_infos_commandes_clients_localisation.join(vendeurs_localisation.withColumnRenamed("Etat","EtatVendeur").withColumnRenamed("Ville","VilleVendeur"), "seller_id")



/***********************************************************************************************************************************************
************************************ INFORMATIONS SUR LES LOCALISATIONS DES VILLES *************************************************************
************************************************************************************************************************************************/

val localisationVilles = (geoloc.groupBy("geolocation_city","geolocation_state").agg(
expr("avg(geolocation_lat) AS latitudeVille"),
expr("avg(geolocation_lng) AS longitudeVille")).
sort(asc("geolocation_city")).coalesce(3)).
join(etats,col("geolocation_state")===col("code_etat")).drop("code_etat","geolocation_state")


val temp = liste.clone
val liste = saveDfToCsv(localisationVilles,"VILLE-localisationVilles.csv",temp)






