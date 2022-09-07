-- Model: Cojoden avance
-- Version: 1.0
-- Project: Cojoden avance
-- Author: Aurélie RAOUL
drop database if exists cojoden_avance;

create database cojoden_avance;

use cojoden_avance;

DROP TABLE IF EXISTS `cojoden`.`METIER`;

CREATE TABLE IF NOT EXISTS `cojoden`.`METIER` (
  `metier_search` VARCHAR(100) NOT NULL,
  `metier` VARCHAR(255) NULL,
  `categorie` VARCHAR(255) NULL,
  PRIMARY KEY (`metier_search`))
ENGINE = InnoDB;

DROP TABLE IF EXISTS `cojoden`.`ARTISTE`;

CREATE TABLE IF NOT EXISTS `cojoden`.`ARTISTE` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `nom_search` VARCHAR(255) NOT NULL,
  `nom_naissance` VARCHAR(255) NOT NULL,
  `nom_dit` VARCHAR(255) NULL,
  `commentaire` TEXT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;

DROP TABLE IF EXISTS `cojoden`.`METIER_has_ARTISTE`;

CREATE TABLE IF NOT EXISTS `cojoden`.`METIER_has_ARTISTE` (
  `metier` VARCHAR(100) NOT NULL,
  `artiste` INT NOT NULL,
  PRIMARY KEY (`metier`, `artiste`),
  CONSTRAINT `fk_mear_metier` FOREIGN KEY (`metier`) REFERENCES `cojoden`.`METIER` (`metier_search`),
  CONSTRAINT `fk_mear_artiste` FOREIGN KEY (`artiste`) REFERENCES `cojoden`.`ARTISTE` (`id`)
)
ENGINE = InnoDB;

DROP TABLE IF EXISTS `cojoden`.`VILLE`;

CREATE TABLE IF NOT EXISTS `cojoden`.`VILLE` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `ville_search` VARCHAR(100) NOT NULL,
  `ville` VARCHAR(100) NOT NULL,
  `departement` VARCHAR(100) NULL,
  `region1` VARCHAR(100) NULL,
  `region2` VARCHAR(100) NULL,
  `pays` VARCHAR(100) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;

DROP TABLE IF EXISTS `cojoden`.`MUSEE`;

CREATE TABLE IF NOT EXISTS `cojoden`.`MUSEE` (
  `museo` VARCHAR(100) NOT NULL,
  `nom_search` VARCHAR(100) NULL,
  `nom` VARCHAR(100) NULL,
  `latitude` VARCHAR(100) NULL,
  `longitude` VARCHAR(100) NULL,
  `plaquette_url` VARCHAR(1000) NULL,
  `ville` INT NULL,
  PRIMARY KEY (`museo`),
  CONSTRAINT `fk_musee_ville` FOREIGN KEY (`ville`) REFERENCES `cojoden`.`VILLE` (`id`)
)
ENGINE = InnoDB;

DROP TABLE IF EXISTS `cojoden`.`MATERIEAUX_TECHNIQUE`;

CREATE TABLE IF NOT EXISTS `cojoden`.`MATERIEAUX_TECHNIQUE` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `mat_search` VARCHAR(100) NOT NULL,
  `materiaux_technique` VARCHAR(100) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;

DROP TABLE IF EXISTS `cojoden`.`OEUVRE`;

CREATE TABLE IF NOT EXISTS `cojoden`.`OEUVRE` (
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
  CONSTRAINT `fk_oeuvre_musee` FOREIGN KEY (`lieux_conservation`) REFERENCES `cojoden`.`MUSEE` (`museo`),
  CONSTRAINT `fk_oeuvre_ville` FOREIGN KEY (`creation_lieux`) REFERENCES `cojoden`.`VILLE` (`id`)
)
ENGINE = InnoDB;

DROP TABLE IF EXISTS `cojoden`.`COMPOSER`;

CREATE TABLE IF NOT EXISTS `cojoden`.`COMPOSER` (
  `oeuvre` VARCHAR(100) NOT NULL,
  `materiaux` INT NOT NULL,
  `complement` VARCHAR(1000) NULL,
  PRIMARY KEY (`oeuvre`, `materiaux`),
  CONSTRAINT `fk_oemat_oeuvre` FOREIGN KEY (`oeuvre`) REFERENCES `cojoden`.`OEUVRE` (`ref`),
  CONSTRAINT `fk_oemat_materiaux` FOREIGN KEY (`materiaux`) REFERENCES `cojoden`.`MATERIEAUX_TECHNIQUE` (`id`)
)
ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `cojoden`.`CREER` (
  `oeuvre` VARCHAR(100) NOT NULL,
  `artiste` INT NOT NULL,
  `role` VARCHAR(1000) NULL,
  PRIMARY KEY (`oeuvre`, `artiste`),
  CONSTRAINT `fk_creer_oeuvre` FOREIGN KEY (`oeuvre`) REFERENCES `cojoden`.`OEUVRE` (`ref`),
  CONSTRAINT `fk_creer_artiste` FOREIGN KEY (`artiste`) REFERENCES `cojoden`.`ARTISTE` (`id`)
)
ENGINE = InnoDB;