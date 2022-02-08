CREATE TABLE IF NOT EXISTS `L117`.`fragment` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(45) NOT NULL,
  `type` VARCHAR(2) NOT NULL,
  `logic` VARCHAR(45) NOT NULL,
  `parent` INT NOT NULL,
  `table` INT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC) VISIBLE,
  INDEX `table_idx` (`table` ASC) VISIBLE,
  CONSTRAINT `fk_fragment_table`
    FOREIGN KEY (`table`)
    REFERENCES `L117`.`table` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
