use h5;

alter table to_rwds change `mobile` acct varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '奖励账号',
	  				add PRIMARY KEY (`id`),
	  				modify column `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
					modify column `pid` char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '交易id' after date,
					modify column `stat` tinyint(1) DEFAULT '0' COMMENT '奖励状态';

insert into to_rwds(user_id,acct,rwds,date) values((select id from users where mobile = '15524931419' or email_addr = '15524931419'),'15524931419',1000,'2020-11-06');
insert into to_rwds(user_id,acct,rwds,date) values((select id from users where mobile = '15102962881' or email_addr = '15102962881'),'15102962881',1000,'2020-11-06');
.
.
.
