
/***********************************************************************************************************************************************
************************************ INFORMATIONS SUR LES PAIEMENTS ****************************************************************************
************************************************************************************************************************************************/

//Jointure des paiements et des commandes 
val paiementsCommandesClients = commandes_clients_localisation.join(paiements,"order_id").coalesce(3)


//Calcul du nombre d'utlisation de chaque mode de paiements
val nbUtilisationModePaiement = paiementsCommandesClients.groupBy("payment_type").agg(expr("count(*) AS nombreUtilisation")).sort(desc("nombreUtilisation"),desc("payment_type"))
//Utilisation de chaque mode de paiement pour chaque état
val nbUtilisationModePaiementEtat = paiementsCommandesClients.groupBy("Etat","payment_type").agg(expr("count(*) AS nombreUtilisation")).sort(asc("Etat"))


val temp = liste.clone
val liste = saveDfToCsv(nbUtilisationModePaiement,"PAIEMENT-nbUtilisationModePaiement.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(nbUtilisationModePaiementEtat,"PAIEMENT-nbUtilisationModePaiementEtat.csv",temp)


//Informations sur les montants des paiements
//Nombre de versements moyen
val nbVersementsMoyen = paiementsCommandesClients.agg(expr("avg(payment_installments) AS nbVersementsMoyen"))
//Nombre de versements moyens selon les modes
val nbVersementsMoyenMode = paiementsCommandesClients.groupBy("payment_type").agg(expr("avg(payment_installments) AS nbVersementsMoyenMode"))
//Nombre de versement moyen selon les états pour les cartes de crédits
val nbVersementMoyenEtatCarte = paiementsCommandesClients.filter($"payment_type" === "credit_card").groupBy("Etat").agg(expr("avg(payment_installments) AS nbVersementsMoyenEtatCarte"))

val montantMoyenVersementEtatCarte = paiementsCommandesClients.filter($"payment_type" === "credit_card").groupBy("Etat").agg(expr("avg(payment_value) AS montantMoyenVersementEtatCarte"))


//Ajouter le montant moyen des versements par état -> etude de la solvabilité des états

val temp = liste.clone
val liste = saveDfToCsv(nbVersementsMoyen,"PAIEMENT-nbVersementsMoyen.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(nbVersementsMoyenMode,"PAIEMENT-nbVersementsMoyenMode.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(nbVersementMoyenEtatCarte,"ETAT-nbVersementMoyenEtatCarte.csv",temp)
val temp = liste.clone
val liste = saveDfToCsv(montantMoyenVersementEtatCarte,"ETAT-montantMoyenVersementEtatCarte.csv",temp)