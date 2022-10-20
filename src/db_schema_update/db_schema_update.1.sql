CREATE TABLE IF NOT EXISTS `meta`(
	`Lock` char(1) NOT NULL DEFAULT 'X',
	`schema_version` int NOT NULL DEFAULT 0,
	constraint PK_meta PRIMARY KEY (`Lock`),
	constraint CK_meta_Locked CHECK (`Lock`='X')
)
#---
CREATE TABLE IF NOT EXISTS `messages`(
	`id` int NOT NULL AUTO_INCREMENT,
	`message` TEXT NOT NULL,
	PRIMARY KEY (`id`), UNIQUE KEY `id` (`id`)
)
