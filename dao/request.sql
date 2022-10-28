-- Script with all SQL request use for this project (except creation and drop)
-- 
-- Project: Cojoden avance
-- =======
-- 
-- __authors__     = ("Aurélie RAOUL")
-- __contact__     = ("aurelie.raoul@yahoo.fr")
-- __copyright__   = "MIT"
-- __date__        = "2022-10-01"
-- __version__     = "1.0.0"

use cojoden_avance;

-- 191 594 rôles renseignés il s'agit uniquement de PEINTRE ?
SELECT count(creer.role) as nb, creer.role
FROM creer
GROUP BY creer.role
ORDER BY nb DESC;

-- Il y a 2 672 rôles différents
SELECT count(distinct(creer.role))
FROM creer;

-- 191 594 rôles renseignés
SELECT count(*) as nb
FROM creer
WHERE creer.role is not NULL
ORDER BY nb DESC;

SELECT distinct(creer.oeuvre)
FROM creer  
WHERE creer.role  LIKE '%sculpteur%';

SELECT composer.oeuvre
FROM materiaux_technique, composer
WHERE composer.materiaux = materiaux_technique.id  
AND mat_search LIKE '%agate%';

SELECT oeuvre.ref, oeuvre.titre, oeuvre.type, oeuvre.annee_debut, oeuvre.inscriptions, oeuvre.texte FROM oeuvre
WHERE oeuvre.ref in (
	SELECT distinct(creer.oeuvre)
	FROM creer  
	WHERE creer.role  LIKE '%peintre%')
  
AND  oeuvre.ref in (
  SELECT composer.oeuvre
  FROM materiaux_technique, composer
  WHERE composer.materiaux = materiaux_technique.id  
  AND mat_search LIKE '%agate%')
  
LIMIT 100;

SELECT distinct(domaine) FROM domaine LIMIT 100;


SELECT distinct(materiaux_technique) FROM materiaux_technique LIMIT 100;

SELECT count(materiaux_technique) as nb, materiaux_technique
FROM materiaux_technique
GROUP BY materiaux_technique
ORDER BY nb DESC;


SELECT count(distinct(type)) as nb, type
FROM oeuvre
GROUP BY type
ORDER BY nb DESC;

SELECT distinct(role) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE nom_search LIKE '%RAOUL%' AND role is not NULL;