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

DROP TABLE IF EXISTS `cojoden`.`PRATIQUER`;

CREATE TABLE IF NOT EXISTS `cojoden`.`PRATIQUER` (
  `metier` VARCHAR(100) NOT NULL,
  `artiste` INT NOT NULL,
  PRIMARY KEY (`metier`, `artiste`),
  INDEX `fk_pratiquer_artiste_idx` (`artiste` ASC) VISIBLE,
  INDEX `fk_pratiquer_metier_idx` (`metier` ASC) VISIBLE,
  CONSTRAINT `fk_pratiquer_metier`
    FOREIGN KEY (`metier`)
    REFERENCES `cojoden`.`METIER` (`metier_search`),
  CONSTRAINT `fk_pratiquer_artiste`
    FOREIGN KEY (`artiste`)
    REFERENCES `cojoden`.`ARTISTE` (`id`)
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

DROP TABLE IF EXISTS `cojoden`.`DOMAINE`;
CREATE TABLE IF NOT EXISTS `cojoden`.`DOMAINE` (
  `domaine_search` VARCHAR(100) NOT NULL,
  `domaine` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`domaine_search`))
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
  INDEX `fk_composer_matiere_idx` (`materiaux` ASC) INVISIBLE,
  INDEX `fk_composer_oeuvre_idx` (`oeuvre` ASC) INVISIBLE,
  CONSTRAINT `fk_composer_oeuvre`
    FOREIGN KEY (`oeuvre`)
    REFERENCES `cojoden`.`OEUVRE` (`ref`),
  CONSTRAINT `fk_composer_matiere`
    FOREIGN KEY (`materiaux`)
    REFERENCES `cojoden`.`MATERIEAUX_TECHNIQUE` (`id`)
)
ENGINE = InnoDB;

DROP TABLE IF EXISTS `cojoden`.`CREER`;
CREATE TABLE IF NOT EXISTS `cojoden`.`CREER` (
  `oeuvre` VARCHAR(100) NOT NULL,
  `artiste` INT NOT NULL,
  `role` VARCHAR(1000) NULL,
  PRIMARY KEY (`oeuvre`, `artiste`),
  INDEX `fk_creer_artiste_idx` (`artiste` ASC) VISIBLE,
  INDEX `fk_creer_oeuvre_idx` (`oeuvre` ASC) VISIBLE,
  CONSTRAINT `fk_creer_oeuvre`
    FOREIGN KEY (`oeuvre`)
    REFERENCES `cojoden`.`OEUVRE` (`ref`),
  CONSTRAINT `fk_creer_artiste`
    FOREIGN KEY (`artiste`)
    REFERENCES `cojoden`.`ARTISTE` (`id`)
)
ENGINE = InnoDB;

DROP TABLE IF EXISTS `cojoden`.`CONCERNER`;

CREATE TABLE IF NOT EXISTS `cojoden`.`CONCERNER` (
  `domaine` VARCHAR(100) NOT NULL,
  `oeuvre` VARCHAR(100) NOT NULL,
  PRIMARY KEY (`domaine`, `oeuvre`),
  INDEX `fk_concerner_domaine_idx` (`oeuvre` ASC) VISIBLE,
  INDEX `fk_concerner_domaine_idx` (`domaine` ASC) VISIBLE,
  CONSTRAINT `fk_concerner_domaine`
    FOREIGN KEY (`domaine`)
    REFERENCES `cojoden`.`DOMAINE` (`domaine_search`),
  CONSTRAINT `fk_concerner_oeuvre`
    FOREIGN KEY (`oeuvre`)
    REFERENCES `cojoden`.`OEUVRE` (`ref`))
ENGINE = InnoDB