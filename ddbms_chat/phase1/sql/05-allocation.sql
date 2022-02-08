CREATE TABLE IF NOT EXISTS `L117`.`allocation` (
  `fragment` INT NOT NULL,
  `site` INT NOT NULL,
  PRIMARY KEY (`fragment`, `site`),
  INDEX `site_idx` (`site` ASC) VISIBLE,
  CONSTRAINT `fk_allocation_fragment`
    FOREIGN KEY (`fragment`)
    REFERENCES `L117`.`fragment` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_allocation_site`
    FOREIGN KEY (`site`)
    REFERENCES `L117`.`site` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
