package main

import (
	"log"
	"os"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/shopspring/decimal"
)

const (
	url = "root:rich_hst_777@(192.168.20.182:33067)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
	// url = "root:rich_hst_777@(192.168.2.177:33067)/H5_0813?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
	// url = "hst:rich_hst_777@(112.74.18.25:3306)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
	// url = "hsc:123456@(172.16.0.60:3306)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
	// url = "hsc:Hschain2020hkdb@(rm-j6c4plvy6m87cy88mdo.mysql.rds.aliyuncs.com:3306)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
)

type User struct {
	ID                           uint
	Acct                         string
	Usdts, FrozU, CmptPow, FrozP decimal.Decimal
}

func main() {

	var (
		errlog, inflog                *log.Logger
		err                           error
		sql_user_canc                 = "select id, coalesce(mobile,email_addr) acct, usdts, froz_u from users where froz_u > 0"
		sql_user_canc2                = "select id, coalesce(mobile,email_addr) acct, cmpt_pow, froz_p from users where froz_p > 0"
		sql_upd_user                  = "update users set froz_u = 0 where id = ? and froz_u > 0"
		sql_upd_user2                 = "update users u inner join pool_rigs pr on pr.rig_id = u.id inner join node_pools np on np.pool_id = pr.pool_id set np.sum_cmpt_pow = np.sum_cmpt_pow + ?, pr.cmpt_pow = pr.cmpt_pow + ?, u.cmpt_pow = u.cmpt_pow + ?, u.froz_p = 0 where id = ? and froz_p > 0"
		sql_upd_tpl                   = "update trd_power_lists set list_stat = 2 where user_id = ? and list_stat = 1 and kind = 1"
		sql_upd_tpl2                  = "update trd_power_lists set list_stat = 2 where user_id = ? and list_stat = 1 and kind = 2"
		users, users2                 []User
		totU, totP                    decimal.Decimal
		totCancOrders, totCancOrders2 int64
		start                         time.Time
		spendTime                     time.Duration
	)

	errlog = log.New(os.Stdout, "[Error] ", log.Ldate|log.Ltime|log.Lshortfile)
	inflog = log.New(os.Stdout, "[Info] ", log.Ldate|log.Ltime|log.Lshortfile)

	if db, err = InitMysql(url); err != nil {
		errlog.Printf("数据库连接失败：%s", err.Error())
		os.Exit(1)
	}
	defer db.Close()

	start = time.Now()

	if err := db.Raw(sql_user_canc).Scan(&users).Error; err != nil {
		errlog.Printf("查询待撤单求购用户信息失败：", err)
		return
	}
	if err := db.Raw(sql_user_canc2).Scan(&users2).Error; err != nil {
		errlog.Printf("查询待撤单卖出用户信息失败：", err)
		return
	}

	tx := db.Begin()
	defer tx.Rollback()
	for _, v := range users {
		if tx = tx.Exec(sql_upd_user, v.ID); tx.Error != nil || tx.RowsAffected != 1 {
			errlog.Printf("给用户%d-%s撤单失败，err：%v，tx.RowsAffected：%d.", v.ID, v.Acct, tx.Error, tx.RowsAffected)
			return
		}
		if tx = tx.Exec(sql_upd_tpl, v.ID); tx.Error != nil || tx.RowsAffected <= 0 {
			errlog.Printf("给用户%d-%s撤单失败，err：%v，tx.RowsAffected：%d.", v.ID, v.Acct, tx.Error, tx.RowsAffected)
			return
		}
		totCancOrders += tx.RowsAffected
		totU = totU.Add(v.FrozU)
	}
	for _, v := range users2 {
		if tx = tx.Exec(sql_upd_user2, v.FrozP, v.FrozP, v.FrozP, v.ID); tx.Error != nil || tx.RowsAffected != 3 {
			errlog.Printf("给用户%d-%s撤单失败，err：%v，tx.RowsAffected：%d.", v.ID, v.Acct, tx.Error, tx.RowsAffected)
			return
		}
		if tx = tx.Exec(sql_upd_tpl2, v.ID); tx.Error != nil || tx.RowsAffected <= 0 {
			errlog.Printf("给用户%d-%s撤单失败，err：%v，tx.RowsAffected：%d.", v.ID, v.Acct, tx.Error, tx.RowsAffected)
			return
		}
		totCancOrders2 += tx.RowsAffected
		totP = totP.Add(v.FrozP)
	}
	tx.Commit()

	spendTime = time.Since(start)
	inflog.Printf("执行时间：%v，求购撤单用户数量：%d，卖出撤单用户数量：%d，求购总撤单数量：%d，卖出总撤单数量：%d，返还USDT数量：%s，返还算力数量：%s.", spendTime, len(users), len(users2), totCancOrders, totCancOrders2, totU, totP)
}

// 连接数据库，初始化
func InitMysql(url string) (*gorm.DB, error) {

	logMode := true
	maxLifetime := 14400
	maxIdleConns := 10
	maxOpenConns := 100

	var err error
	if db, err = gorm.Open("mysql", url); err != nil {
		return nil, err
	}
	db = db.Set("gorm:table_options", "ENGINE=InnoDB CHARSET=utf8mb4 auto_increment=1")
	db.LogMode(logMode)
	db.DB().SetConnMaxLifetime(time.Duration(maxLifetime) * time.Second)
	db.DB().SetMaxIdleConns(maxIdleConns)
	db.DB().SetMaxOpenConns(maxOpenConns)
	return db, nil
}

var db *gorm.DB
