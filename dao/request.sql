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


SELECT museo, nom, ville.ville, latitude, longitude, count(ref) as nb_oeuvres, count(artiste) as nb_artistes
FROM musee
inner join ville
on ville.id = musee.ville

inner join oeuvre
on museo = lieux_conservation

inner join creer
on ref = creer.oeuvre

GROUP BY museo
ORDER BY nb_oeuvres DESC
LIMIT 100;

SELECT museo, nom, ville.ville, latitude, longitude, count(ref) as nb_oeuvres, count(artiste) as nb_artistes 
FROM musee 
inner join ville on ville.id = musee.ville 
inner join oeuvre on museo = lieux_conservation 
inner join creer on ref = creer.oeuvre 
WHERE museo in (SELECT distinct(lieux_conservation) FROM oeuvre WHERE titre LIKE '%RAOUL%' ) 
AND museo in (SELECT distinct(museo) FROM musee INNER JOIN ville ON ville.id = musee.ville WHERE ville_search LIKE '%BRIEUC%' ) 
AND museo in (SELECT distinct(lieux_conservation) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE role LIKE '%PEINTRE%' ) 
GROUP BY museo 
ORDER BY nb_oeuvres DESC 
LIMIT 100;

SELECT museo, nom, ville.ville, latitude, longitude, count(ref) as nb_oeuvres, count(artiste) as nb_artistes 
FROM musee 
inner join ville on ville.id = musee.ville 
inner join oeuvre on museo = lieux_conservation 
inner join creer on ref = creer.oeuvre 
WHERE museo in (SELECT distinct(lieux_conservation) FROM oeuvre WHERE titre LIKE '%RAOUL%' ) 
AND museo in (SELECT distinct(museo) FROM musee INNER JOIN ville ON ville.id = musee.ville WHERE ville_search LIKE '%BRIEUC%' ) 
AND museo in (SELECT distinct(lieux_conservation) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE role LIKE '%PEINTRE%' ) 
GROUP BY museo 
ORDER BY nb_oeuvres DESC 
LIMIT 100;


SELECT museo, nom, ville.ville, latitude, longitude, count(ref) as nb_oeuvres, count(artiste) as nb_artistes 
FROM musee 
inner join ville on ville.id = musee.ville 
inner join oeuvre on museo = lieux_conservation 
inner join creer on ref = creer.oeuvre 
WHERE museo in (SELECT distinct(lieux_conservation) FROM oeuvre WHERE titre LIKE '%RAOUL%' ) 
OR museo in (SELECT distinct(museo) FROM musee INNER JOIN ville ON ville.id = musee.ville WHERE ville_search LIKE '%BRIEUC%' ) 
OR museo in (SELECT distinct(lieux_conservation) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE role LIKE '%PEINTRE%' ) 
GROUP BY museo 
ORDER BY nb_oeuvres DESC 
LIMIT 100;

SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes 
FROM musee 
inner join ville on ville.id = musee.ville 
inner join oeuvre on museo = lieux_conservation 
inner join creer on ref = creer.oeuvre 
WHERE ville_search LIKE '%BRIEUC%' 
GROUP BY museo 
ORDER BY nb_oeuvres DESC 
LIMIT 100;

SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes 
FROM musee 
inner join ville on ville.id = musee.ville 
inner join oeuvre on museo = lieux_conservation 
inner join creer on ref = creer.oeuvre 
WHERE ville_search LIKE '%BREST%' 
GROUP BY museo 
ORDER BY nb_oeuvres DESC 
LIMIT 100;

SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes 
FROM musee 
inner join ville on ville.id = musee.ville 
inner join oeuvre on museo = lieux_conservation 
inner join creer on ref = creer.oeuvre 
WHERE ville_search LIKE '%BRIEUC%' 
GROUP BY museo 
ORDER BY nb_oeuvres DESC 
LIMIT 100;

SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes 
FROM musee inner join ville on ville.id = musee.ville inner join oeuvre on museo = lieux_conservation inner join creer on ref = creer.oeuvre 
WHERE ville_search LIKE '%BRIEUC%' GROUP BY museo ORDER BY nb_oeuvres DESC LIMIT 100;

SELECT distinct(lieux_conservation) 
FROM composer 
INNER JOIN materiaux_technique ON materiaux_technique.id = composer.materiaux 
INNER JOIN oeuvre ON oeuvre.ref = composer.oeuvre 
WHERE mat_search LIKE '%AGATE%';

SELECT distinct(oeuvre) 
FROM composer INNER JOIN materiaux_technique ON materiaux_technique.id = composer.materiaux 
WHERE mat_search LIKE '%PAPIER BARYTE%';

SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes 
FROM musee 
inner join ville on ville.id = musee.ville 
inner join oeuvre on museo = lieux_conservation 
inner join creer on ref = creer.oeuvre 
WHERE ville_search LIKE '%BRIEUC%' 
OR museo in (
    SELECT distinct(lieux_conservation) 
    FROM composer 
    INNER JOIN materiaux_technique ON materiaux_technique.id = composer.materiaux 
    INNER JOIN oeuvre ON oeuvre.ref = composer.oeuvre 
    WHERE mat_search LIKE '%AGATE%' ) 
GROUP BY museo 
ORDER BY nb_oeuvres DESC 
LIMIT 100;

SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes 
FROM musee 
inner join ville on ville.id = musee.ville 
inner join oeuvre on museo = lieux_conservation 
inner join creer on ref = creer.oeuvre 
WHERE ville_search LIKE '%BRIEUC%' 
AND museo in (
    SELECT distinct(lieux_conservation) 
    FROM composer 
    INNER JOIN materiaux_technique ON materiaux_technique.id = composer.materiaux 
    INNER JOIN oeuvre ON oeuvre.ref = composer.oeuvre 
    WHERE mat_search LIKE '%AGATE%' ) 
GROUP BY museo 
ORDER BY nb_oeuvres DESC 
LIMIT 100;

SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes 
FROM musee 
inner join ville on ville.id = musee.ville 
inner join oeuvre on museo = lieux_conservation 
inner join creer on ref = creer.oeuvre 
WHERE ville_search LIKE '%BRIEUC%' 
GROUP BY museo 
ORDER BY nb_oeuvres DESC 
LIMIT 100;


SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes 
FROM musee inner join ville on ville.id = musee.ville 
inner join oeuvre on museo = lieux_conservation 
inner join creer on ref = creer.oeuvre 
WHERE nom_search LIKE '%ART%' 
AND ville_search LIKE '%BREST%' 
GROUP BY museo 
ORDER BY nb_oeuvres 
DESC LIMIT 100;

SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes 
FROM musee 
inner join ville on ville.id = musee.ville 
inner join oeuvre on museo = lieux_conservation 
inner join creer on ref = creer.oeuvre 
WHERE `nom_search` LIKE '%ART%' 
AND ville_search LIKE '%BREST%' 
AND museo in (
	SELECT distinct(`lieux_conservation`) FROM creer 
    INNER JOIN artiste ON artiste.id = creer.artiste 
    WHERE `creer`.`role` LIKE '%PEINTRE%' ) 
GROUP BY museo 
ORDER BY nb_oeuvres DESC LIMIT 100;


SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes 
FROM musee 
inner join ville on ville.id = musee.ville 
inner join oeuvre on museo = lieux_conservation 
inner join creer on ref = creer.oeuvre 
WHERE ville_search LIKE '%BRIEUC%' 
AND museo in (SELECT distinct(`lieux_conservation`) FROM oeuvre WHERE `titre` LIKE '%RAOUL%' ) 
AND museo in (
	SELECT distinct(`lieux_conservation`) 
    FROM creer INNER JOIN artiste ON artiste.id = creer.artiste 
    WHERE `creer`.`role` LIKE '%PEINTRE%' ) 
GROUP BY museo 
ORDER BY nb_oeuvres DESC 
LIMIT 100;

SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes FROM musee inner join ville on ville.id = musee.ville inner join oeuvre on museo = lieux_conservation inner join creer on ref = creer.oeuvre WHERE ville_search LIKE '%BRIEUC%' GROUP BY museo ORDER BY nb_oeuvres DESC LIMIT 100;
SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes FROM musee inner join ville on ville.id = musee.ville inner join oeuvre on museo = lieux_conservation inner join creer on ref = creer.oeuvre WHERE ville_search LIKE '%PARIS%' AND museo in (SELECT distinct(`lieux_conservation`) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE `creer`.`role` LIKE '%PEINTRE%' ) GROUP BY museo ORDER BY nb_oeuvres DESC LIMIT 100;
SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes FROM musee inner join ville on ville.id = musee.ville inner join oeuvre on museo = lieux_conservation inner join creer on ref = creer.oeuvre WHERE ville_search LIKE '%LANNION%' GROUP BY museo ORDER BY nb_oeuvres DESC LIMIT 100;
SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes 
FROM musee 
inner join ville on ville.id = musee.ville 
inner join oeuvre on museo = lieux_conservation 
inner join creer on ref = creer.oeuvre 
WHERE ville_search LIKE '%PARIS%' 
AND museo in (SELECT distinct(`lieux_conservation`) FROM oeuvre WHERE `titre` LIKE '%RAOUL%' ) 
AND museo in (
	SELECT distinct(`lieux_conservation`) FROM creer 
    INNER JOIN artiste ON artiste.id = creer.artiste 
    WHERE `creer`.`role` LIKE '%PEINTRE%' ) 
GROUP BY museo ORDER BY nb_oeuvres DESC LIMIT 100;

SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes FROM musee inner join ville on ville.id = musee.ville inner join oeuvre on museo = lieux_conservation inner join creer on ref = creer.oeuvre WHERE (ville_search LIKE '%BRETAGNE%' OR departement LIKE '%BRETAGNE%' OR region1 LIKE '%BRETAGNE%') AND museo in (SELECT distinct(`lieux_conservation`) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE `nom_search` LIKE '%RAOUL%' AND `dit` LIKE '%RAOUL%' ) GROUP BY museo ORDER BY nb_oeuvres DESC LIMIT 100;
SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes FROM musee inner join ville on ville.id = musee.ville inner join oeuvre on museo = lieux_conservation inner join creer on ref = creer.oeuvre WHERE `nom_search` LIKE '%ART%' AND (ville_search LIKE '%BREST%') AND museo in (SELECT distinct(`lieux_conservation`) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE `creer`.`role` LIKE '%PEINTRE%' ) GROUP BY museo ORDER BY nb_oeuvres DESC LIMIT 100;

SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes FROM musee inner join ville on ville.id = musee.ville inner join oeuvre on museo = lieux_conservation inner join creer on ref = creer.oeuvre WHERE (ville_search LIKE '%BRIEUC%') AND museo in (SELECT distinct(`lieux_conservation`) FROM oeuvre WHERE `titre` LIKE '%RAOUL%' ) AND museo in (SELECT distinct(`lieux_conservation`) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE `creer`.`role` LIKE '%PEINTRE%' ) GROUP BY museo ORDER BY nb_oeuvres DESC LIMIT 100;
SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes FROM musee inner join ville on ville.id = musee.ville inner join oeuvre on museo = lieux_conservation inner join creer on ref = creer.oeuvre WHERE (ville_search LIKE '%BRETAGNE%') AND museo in (SELECT distinct(`lieux_conservation`) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE `creer`.`role` LIKE '%PEINTRE%' ) GROUP BY museo ORDER BY nb_oeuvres DESC LIMIT 100;

SELECT distinct(`lieux_conservation`) FROM concerner INNER JOIN domaine ON domaine.id = concerner.domaine INNER JOIN oeuvre ON oeuvre.ref = concerner.oeuvre WHERE `dom_search` LIKE '%AFRIQUE%';

SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes FROM musee inner join ville on ville.id = musee.ville inner join oeuvre on museo = lieux_conservation inner join creer on ref = creer.oeuvre WHERE (ville_search LIKE '%PARIS%') AND museo in (SELECT distinct(`lieux_conservation`) FROM concerner INNER JOIN domaine ON domaine.id = concerner.domaine INNER JOIN oeuvre ON oeuvre.ref = concerner.oeuvre WHERE `dom_search` LIKE '%AFRIQUE%' ) GROUP BY museo ORDER BY nb_oeuvres DESC LIMIT 100;

SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes FROM musee inner join ville on ville.id = musee.ville inner join oeuvre on museo = lieux_conservation inner join creer on ref = creer.oeuvre WHERE museo in (SELECT distinct(`lieux_conservation`) FROM oeuvre WHERE `titre` LIKE '%RAOUL%' ) GROUP BY museo ORDER BY nb_oeuvres DESC LIMIT 100;
SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes FROM musee inner join ville on ville.id = musee.ville inner join oeuvre on museo = lieux_conservation inner join creer on ref = creer.oeuvre WHERE museo in (SELECT distinct(`lieux_conservation`) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE `nom_search` LIKE '%RAOUL%' ) GROUP BY museo ORDER BY nb_oeuvres DESC LIMIT 100;

SELECT distinct(`lieux_conservation`) FROM oeuvre INNER JOIN creer     ON oeuvre.ref = creer.oeuvre     INNER JOIN artiste ON artiste.id = creer.artiste     WHERE `nom_search` LIKE '%RAOUL%';
SELECT distinct(`lieux_conservation`) FROM oeuvre INNER JOIN concerner ON oeuvre.ref = concerner.oeuvre INNER JOIN domaine ON domaine.id = concerner.domaine WHERE `dom_search` LIKE '%PEINTURE%';

SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes 
FROM musee inner join ville on ville.id = musee.ville inner join oeuvre on museo = lieux_conservation inner join creer on ref = creer.oeuvre 
WHERE museo in (
	SELECT distinct(`lieux_conservation`) FROM oeuvre INNER JOIN creer ON oeuvre.ref = creer.oeuvre INNER JOIN artiste ON artiste.id = creer.artiste WHERE `nom_search` LIKE '%RAOUL%' 
    ) GROUP BY museo ORDER BY nb_oeuvres DESC LIMIT 100;
    
SELECT distinct(`lieux_conservation`) FROM oeuvre INNER JOIN creer ON oeuvre.ref = creer.oeuvre INNER JOIN artiste ON artiste.id = creer.artiste WHERE `nom_search` LIKE '%RAOUL%';

SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes 
FROM musee inner join ville on ville.id = musee.ville inner join oeuvre on museo = lieux_conservation inner join creer on ref = creer.oeuvre 
WHERE (ville_search LIKE '%BRETAGNE%' OR departement LIKE '%BRETAGNE%' OR region1 LIKE '%BRETAGNE%') 
GROUP BY museo ORDER BY nb_oeuvres DESC LIMIT 100;

-- DELETE FROM table_name WHERE condition;
DELETE FROM VILLE WHERE id > '410';
INSERT INTO `VILLE` ( `ville_search`, `ville` , `departement`, `region1`) VALUES ('LANNION', 'Lannion', 'Côtes d\'Armor', 'Bretagne');

SELECT * FROM ville;

SELECT * FROM `VILLE` WHERE ville_search  LIKE '%LANNION%';

SELECT LAST_INSERT_ID() ;

select max(id) FROM ville;

ALTER TABLE ville auto_increment = 415;
ALTER TABLE domaine auto_increment = 143;
ALTER TABLE artiste auto_increment = 46237;
ALTER TABLE materiaux_technique auto_increment = 8463;

-- RENAME {DATABASE | SCHEMA} db_name TO new_db_name;
-- RENAME TABLE old_db.table TO new_db.table;
-- RENAME TABLE cojoden_avance.ville TO cojoden_avance.ville_save;

CREATE TABLE IF NOT EXISTS `VILLE` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `ville_search` VARCHAR(100) NOT NULL,
    `ville` VARCHAR(100) NOT NULL,
    `departement` VARCHAR(100) NULL,
    `region1` VARCHAR(100) NULL,
    `region2` VARCHAR(100) NULL,
    `pays` VARCHAR(100) NULL,
    PRIMARY KEY (`id`))
    ENGINE=MYISAM;

DROP TABLE CONCERNER;

CREATE TABLE IF NOT EXISTS `concerner` (
    `domaine` INT NOT NULL,
    `oeuvre` VARCHAR(100) NOT NULL,
    PRIMARY KEY (`domaine`, `oeuvre`),
    INDEX `fk_concerner_oeuvre_idx` (`oeuvre` ASC) VISIBLE,
    INDEX `fk_converner_domaine_idx` (`domaine` ASC) INVISIBLE,
    CONSTRAINT `fk_concerner_domaine`
        FOREIGN KEY (`domaine`)
        REFERENCES `DOMAINE` (`id`),
    CONSTRAINT `fk_concerner_oeuvre`
        FOREIGN KEY (`oeuvre`)
        REFERENCES `OEUVRE` (`ref`)
    )
    ENGINE=MYISAM;
    
SELECT distinct(`id`) FROM `VILLE` WHERE ville_search  LIKE '%BREST%';

SELECT museo, nom, ville.ville, latitude, longitude, count(distinct(ref)) as nb_oeuvres, count(distinct(artiste)) as nb_artistes FROM musee inner join ville on ville.id = musee.ville inner join oeuvre on museo = lieux_conservation inner join creer on ref = creer.oeuvre WHERE (ville_search LIKE '%BRETAGNE%' OR departement LIKE '%BRETAGNE%' OR region1 LIKE '%BRETAGNE%') GROUP BY museo ORDER BY nb_oeuvres DESC;

SELECT distinct(`lieux_conservation`) FROM oeuvre INNER JOIN creer ON oeuvre.ref = creer.oeuvre INNER JOIN artiste ON artiste.id = creer.artiste WHERE `nom_search` LIKE '%RAOUL%' AND `nom_dit` LIKE '%RAOUL%';

DELETE FROM METIER WHERE metier_search LIKE 'TEST_%';
SELECT * FROM METIER WHERE metier_search LIKE 'TEST_%';

SELECT * from ville where ville_search LIKE '%BREST%';
INSERT INTO `METIER` (`metier_search`, `metier`) VALUES ('TEST MAGICIEN', 'test_Magicien');
INSERT INTO `METIER` (`metier_search`, `metier`, `categorie`) VALUES ('TEST TESTEUR', 'test_Testeur', Informatique);