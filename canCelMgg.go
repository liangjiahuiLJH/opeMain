package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

var (
	db           *gorm.DB
	errlog       = log.New(os.Stdout, "[Err] ", log.Ldate|log.Ltime|log.Lshortfile)
	inflog       = log.New(os.Stdout, "[Info] ", log.Ldate|log.Ltime|log.Lshortfile)
	warlog       = log.New(os.Stdout, "[War] ", log.Ldate|log.Ltime|log.Lshortfile)
	bi, ei       int
	fails, sucs  int
	acct, suffix string
	start        time.Time
	spendTime    time.Duration
)

type User struct {
	ID uint
}

func main() {
	flag.IntVar(&bi, "bi", 0, "begin id of acccount")
	flag.IntVar(&ei, "ei", 0, "end id of acccount")
	flag.StringVar(&acct, "acct", "", "to cancel mortgage hst account")
	flag.StringVar(&suffix, "suffix", "", "to cancel mortgage hst account's suffix")
	flag.Parse()

	url := "root:rich_hst_777@(192.168.182.128:33067)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
	// url := "hsc:123456@(172.16.0.60:3306)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
	// url := "hsc:Hschain2020hkdb@(rm-j6c4plvy6m87cy88mdo.mysql.rds.aliyuncs.com:3306)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"

	db, err := InitMysql(url)
	if err != nil {
		errlog.Printf("数据库连接失败！%s", err)
		os.Exit(1)
	}
	defer db.Close()

	start = time.Now()

	sql_sel := "select id from users where coalesce(mobile,email_addr) = ?"
	sqls := []string{}
	sqls = append(sqls, "update users set hsts = hsts + mgg_hsts, mgg_hsts = 0, is_mining_pool = 0 where id = ? and (is_mining_pool = 2 or is_mining_pool = 3)") // sql_upd_user
	sqls = append(sqls, "delete from ssu_form_exp_relts where exp_id = ?")                                                                                       // sql_del_fe
	sqls = append(sqls, "delete from node_pools where pool_id = ?")                                                                                              // sql_del_np
	sqls = append(sqls, "delete from pool_rigs where pool_id = ?")                                                                                               // sql_del_pr
	sqls = append(sqls, "delete from ssu_unls where user_id = ? and typ = 0")                                                                                    // sql_del_su
	rowsAffected := []int64{1, 1, 1, 0, 1}
	failsAcct := []string{}
	user := User{}
	for ui := bi; ui <= ei; ui++ {
		curAcct := fmt.Sprintf("%s%02d%s", acct, ui, suffix)
		if err := db.Raw(sql_sel, curAcct).Scan(&user).Error; err != nil {
			errlog.Println("查询奖励信息错误：%s！", err)
			continue
		}
		tx := db.Begin()
		for i, sql := range sqls {
			if tx = tx.Exec(sql, user.ID); tx.Error != nil || tx.RowsAffected != rowsAffected[i] {
				errlog.Printf("sql语句执行失败，err：%v，影响记录数：%d。", tx.Error, tx.RowsAffected)
				tx.Rollback()
				fails = fails + 1
				failsAcct = append(failsAcct, curAcct)
				goto nextCycle
			}
		}
		tx.Commit()
		sucs = sucs + 1
	nextCycle:
	}
	spendTime = time.Since(start)
	inflog.Printf("执行时间：%v，期望撤销%d个账号的抵押，其中%d个账号的抵押被成功撤销，%d个账号的抵押撤销失败.", spendTime, ei-bi+1, sucs, fails)
	if fails != 0 {
		inflog.Printf("失败账号：%s.", strings.Join(failsAcct, "\",\""))
	}
}

// 连接数据库，初始化
func InitMysql(url string) (*gorm.DB, error) {

	inflog.Printf("mysql url:%s.", url)

	var err error
	if db, err = gorm.Open("mysql", url); err != nil {
		return nil, err
	}
	db = db.Set("gorm:table_options", "ENGINE=InnoDB CHARSET=utf8mb4 auto_increment=1")
	db.LogMode(true)
	db.DB().SetMaxIdleConns(10)
	db.DB().SetMaxOpenConns(100)
	return db, nil
}
