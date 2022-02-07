import mysql.connector

from ddbms_chat.sites import SITES
from ddbms_chat.tables import TABLES
from ddbms_chat.fragments import FRAGMENTS
from ddbms_chat.allocation import ALLOCATION

create_statements = [
    """
CREATE TABLE IF NOT EXISTS `L117`.`table` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(45) NOT NULL,
  `key` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC) VISIBLE)
ENGINE = InnoDB
""",
    """
CREATE TABLE IF NOT EXISTS `L117`.`site` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(45) NOT NULL,
  `ip` VARCHAR(15) NOT NULL,
  `user` VARCHAR(45) NOT NULL,
  `password` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC) VISIBLE,
  UNIQUE INDEX `ip_UNIQUE` (`ip` ASC) VISIBLE)
ENGINE = InnoDB
""",
    """
CREATE TABLE IF NOT EXISTS `L117`.`fragmentation` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(45) NOT NULL,
  `type` VARCHAR(2) NOT NULL,
  `logic` VARCHAR(45) NOT NULL,
  `parent` INT NOT NULL,
  `table` INT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC) VISIBLE,
  INDEX `table_idx` (`table` ASC) VISIBLE,
  CONSTRAINT `table`
    FOREIGN KEY (`table`)
    REFERENCES `L117`.`table` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
""",
    """
CREATE TABLE IF NOT EXISTS `L117`.`allocation` (
  `fragment` INT NOT NULL,
  `site` INT NOT NULL,
  PRIMARY KEY (`fragment`, `site`),
  INDEX `site_idx` (`site` ASC) VISIBLE,
  CONSTRAINT `fragment`
    FOREIGN KEY (`fragment`)
    REFERENCES `L117`.`fragmentation` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `site`
    FOREIGN KEY (`site`)
    REFERENCES `L117`.`site` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB

""",
]

# create db
for site in SITES:
    try:
        db = mysql.connector.connect(
            host=site.ip, user=site.user, password=site.password
        )
        cursor = db.cursor()
        cursor.execute("CREATE DATABASE L117")
        print(f"Successful at site {site.id}")
    except Exception as e:
        print(f"Failed at site {site.id}, {e}")

# create system catalog
for site in SITES:
    try:
        db = mysql.connector.connect(
            host=site.ip, user=site.user, password=site.password, database="L117"
        )
        cursor = db.cursor()
        for cs in create_statements:
            cursor.execute(cs)
        print(f"Successful at site {site.id}")
    except Exception as e:
        print(f"Failed at site {site.id}, {e}")
