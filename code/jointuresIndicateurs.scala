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

/*
JOINTURE DES DONNEES SUR LES TYPES DE PAIEMENTS SELON LES ETATS
*/

-> Selon les ETATS

nbUtilisationModePaiementEtat.printSchema
nbVersementMoyenEtat.printSchema
nbVersementMoyenEtatCarte.printSchema



val indicateurGeneraux = 
Générales
infosGeneralesCommandes
noteMoyenne
nbVersementsMoyen


