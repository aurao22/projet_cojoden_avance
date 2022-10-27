-- Script with all SQL database creation request
-- 
-- Project: Cojoden avance
-- =======
-- 
-- __authors__     = ("Aurélie RAOUL")
-- __contact__     = ("aurelie.raoul@yahoo.fr")
-- __copyright__   = "MIT"
-- __date__        = "2022-10-01"
-- __version__     = "1.0.0"
drop database if exists cojoden_avance;

create database cojoden_avance;

use cojoden_avance;

DROP TABLE IF EXISTS `CONCERNER`;
DROP TABLE IF EXISTS `COMPOSER`;
DROP TABLE IF EXISTS `CREER`;
DROP TABLE IF EXISTS `METIER`;

DROP TABLE IF EXISTS `OEUVRE`;

DROP TABLE IF EXISTS `ARTISTE`;
DROP TABLE IF EXISTS `MUSEE`;
DROP TABLE IF EXISTS `VILLE`;
DROP TABLE IF EXISTS `MATERIEAUX_TECHNIQUE`;
DROP TABLE IF EXISTS `DOMAINE`;

CREATE TABLE IF NOT EXISTS `METIER` (
  `metier_search` VARCHAR(100) NOT NULL,
  `metier` VARCHAR(255) NULL,
  `categorie` VARCHAR(255) NULL,
  PRIMARY KEY (`metier_search`))
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `ARTISTE` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `nom_search` VARCHAR(255) NOT NULL,
  `nom_naissance` VARCHAR(255) NOT NULL,
  `nom_dit` VARCHAR(255) NULL,
  `commentaire` TEXT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `VILLE` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `ville_search` VARCHAR(100) NOT NULL,
  `ville` VARCHAR(100) NOT NULL,
  `departement` VARCHAR(100) NULL,
  `region1` VARCHAR(100) NULL,
  `region2` VARCHAR(100) NULL,
  `pays` VARCHAR(100) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `MUSEE` (
  `museo` VARCHAR(100) NOT NULL,
  `nom_search` VARCHAR(100) NULL,
  `nom` VARCHAR(100) NULL,
  `latitude` VARCHAR(100) NULL,
  `longitude` VARCHAR(100) NULL,
  `plaquette_url` VARCHAR(1000) NULL,
  `ville` INT NULL,
  PRIMARY KEY (`museo`),
  CONSTRAINT `fk_musee_ville` FOREIGN KEY (`ville`) REFERENCES `VILLE` (`id`)
)
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `DOMAINE` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `dom_search` VARCHAR(255) NOT NULL,
  `domaine` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `MATERIEAUX_TECHNIQUE` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `mat_search` VARCHAR(100) NOT NULL,
  `materiaux_technique` VARCHAR(100) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `OEUVRE` (
  `ref` VARCHAR(100) NOT NULL,
  `titre` VARCHAR(1000) NULL,
  `type` VARCHAR(1000) NULL,
  `domaine` VARCHAR(1000) NULL,
  `texte` TEXT(10000) NULL,
  `annee_debut` VARCHAR(45) NULL,
  `annee_fin` VARCHAR(45) NULL,
  `inscriptions` TEXT NULL,
  `commentaires` TEXT NULL,
  `largeur_cm` INT NULL,
  `hauteur_cm` INT NULL,
  `profondeur_cm` INT NULL,
  `img_url` VARCHAR(1000) NULL,
  `lieux_conservation` VARCHAR(100) NOT NULL,
  `creation_lieux` INT NULL,
  PRIMARY KEY (`ref`),
  CONSTRAINT `fk_oeuvre_musee` FOREIGN KEY (`lieux_conservation`) REFERENCES `MUSEE` (`museo`),
  CONSTRAINT `fk_oeuvre_ville` FOREIGN KEY (`creation_lieux`) REFERENCES `VILLE` (`id`)
)
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `COMPOSER` (
  `oeuvre` VARCHAR(100) NOT NULL,
  `materiaux` INT NOT NULL,
  `complement` VARCHAR(1000) NULL,
  PRIMARY KEY (`oeuvre`, `materiaux`),
  INDEX `fk_composer_matiere_idx` (`materiaux` ASC) INVISIBLE,
  INDEX `fk_composer_oeuvre_idx` (`oeuvre` ASC) INVISIBLE,
  CONSTRAINT `fk_composer_oeuvre`
    FOREIGN KEY (`oeuvre`)
    REFERENCES `OEUVRE` (`ref`),
  CONSTRAINT `fk_composer_matiere`
    FOREIGN KEY (`materiaux`)
    REFERENCES `MATERIEAUX_TECHNIQUE` (`id`)
)
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `CREER` (
  `oeuvre` VARCHAR(100) NOT NULL,
  `artiste` INT NOT NULL,
  `role` VARCHAR(100) NOT NULL,
  PRIMARY KEY (`oeuvre`, `artiste`),
  INDEX `fk_creer_artiste_idx` (`artiste` ASC) VISIBLE,
  INDEX `fk_creer_oeuvre_idx` (`oeuvre` ASC) VISIBLE,
  INDEX `fk_creer_metier_idx` (`role` ASC) VISIBLE,
  CONSTRAINT `fk_creer_oeuvre`
    FOREIGN KEY (`oeuvre`)
    REFERENCES `OEUVRE` (`ref`),
  CONSTRAINT `fk_creer_artiste`
    FOREIGN KEY (`artiste`)
    REFERENCES `ARTISTE` (`id`),
  CONSTRAINT `fk_creer_metier`
    FOREIGN KEY (`role`)
    REFERENCES `METIER` (`metier_search`)
    )
ENGINE = InnoDB;



CREATE TABLE IF NOT EXISTS `CONCERNER` (
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
ENGINE = InnoDB;