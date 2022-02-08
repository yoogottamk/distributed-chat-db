CREATE TABLE IF NOT EXISTS `L117`.`column` (
  `id` INT NOT NULL,
  `table` INT NOT NULL,
  `name` VARCHAR(45) NOT NULL,
  `type` VARCHAR(10) NOT NULL,
  `pk` INT NOT NULL,
  `notnull` INT NOT NULL,
  `unique` INT NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_table_idx` (`table` ASC),
  CONSTRAINT `fk_column_table`
    FOREIGN KEY (`table`)
    REFERENCES `L117`.`table` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
