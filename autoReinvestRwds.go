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
	"github.com/satori/go.uuid"
	"github.com/shopspring/decimal"
)

type User struct {
	ID     uint
	UserID uint
	Acct   string
	Rwds   decimal.Decimal
	Stat   uint
	Hst0s  decimal.Decimal
}

var (
	db              *gorm.DB
	errlog          = log.New(os.Stdout, "[Err] ", log.Ldate|log.Ltime|log.Lshortfile)
	inflog          = log.New(os.Stdout, "[Info] ", log.Ldate|log.Ltime|log.Lshortfile)
	warlog          = log.New(os.Stdout, "[War] ", log.Ldate|log.Ltime|log.Lshortfile)
	date, acct, ctn string
	tot             decimal.Decimal
	start           time.Time
	spendTime       time.Duration
)

func main() {

	flag.StringVar(&date, "date", "", "rwds/subsidy date")
	flag.StringVar(&acct, "acct", "", "rwds/subsidy transferring account")
	flag.Parse()
	// params validator 参数校验
	if date == "" {
		errlog.Printf("奖励/补贴记录执行日期参数错误：nil")
		os.Exit(1)
	}

	url := "root:rich_hst_777@(192.168.182.128:33067)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
	// url := "hsc:Hschain2020hkdb@(rm-j6c4plvy6m87cy88mdo.mysql.rds.aliyuncs.com:3306)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"

	db, err := InitMysql(url)
	if err != nil {
		errlog.Printf("数据库连接失败！%s", err)
		os.Exit(1)
	}
	defer db.Close()

	start = time.Now()

	// 查询待奖励用户信息
	users := []User{}
	if err := db.Raw("select id, coalesce(mobile,email_addr) acct, cmpt_pow from users where arr_rate = 1", date).Scan(&users).Error; err != nil {
		errlog.Println("查询奖励信息错误：%s！", err)
		os.Exit(1)
	}
	// 查询转出方余额
	tfrUser := User{}
	if err := db.Raw("select id user_id, hst0s, coalesce(mobile,email_addr) acct from users where mobile = ? or email_addr = ?", acct, acct).Scan(&tfrUser).Error; err != nil {
		errlog.Println("查询奖励信息错误：%s！", err)
		os.Exit(1)
	}

	for _, v := range users {
		tot = tot.Add(v.Rwds)
	}
	if tot.GreaterThan(tfrUser.Hst0s) {
		errlog.Printf("转出方账号余额%s不足%s.", tfrUser.Hst0s, tot)
		os.Exit(1)
	}

	inflog.Printf("共奖励%d人次，总计%sHST0，继续执行？y/n", len(users), tot)
	fmt.Scanln(&ctn)
	if ctn == "y" || ctn == "yes" {
		inflog.Printf("输入%s，继续执行....", ctn)
	} else {
		inflog.Printf("输入%s，终止程序.", ctn)
		os.Exit(1)
	}

	tx := db.Begin()
	defer tx.Rollback()
	rate1 := decimal.NewFromFloat(1.01)
	rate2 := decimal.NewFromFloat(0.01)
	var trAmount, fee decimal.Decimal
	sql_ins1 := "insert into trans_records(user_id,typ,from_acct,to_acct,pid,asset,amount,fee,remark) values(?,1,?,?,?,'hst0',?,?,'发布会补助')"
	sql_ins2 := "insert into trans_records(user_id,typ,from_acct,to_acct,pid,asset,amount,fee,remark) values(?,0,?,?,?,'hst0',?,0,'发布会补助')"
	sql_upd1 := "update users set hst0s = hst0s - ? where id = ? and hst0s >= ?"
	sql_upd2 := "update users set hst0s = hst0s + ? where id = ?"
	sql_upd3 := "update to_rwds set stat = 1, pid = ? where id = ?"
	for _, v := range users {
		// // 不同UUID包版本方法不同
		// pid1 := strings.ReplaceAll(uuid.Must(uuid.NewV4(), nil).String(), "-", "")
		// pid2 := strings.ReplaceAll(uuid.Must(uuid.NewV4(), nil).String(), "-", "")
		pid1 := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
		pid2 := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
		trAmount = v.Rwds.Mul(rate1)
		fee = v.Rwds.Mul(rate2)
		if err := tx.Exec(sql_ins1, tfrUser.UserID, tfrUser.Acct, v.Acct, pid1, trAmount, fee).Error; err != nil {
			errlog.Println(err)
			os.Exit(1)
		}
		if err := tx.Exec(sql_ins2, v.UserID, tfrUser.Acct, v.Acct, pid2, v.Rwds).Error; err != nil {
			errlog.Println(err)
			os.Exit(1)
		}
		if tx = tx.Exec(sql_upd1, v.Rwds, tfrUser.UserID, v.Rwds); tx.Error != nil {
			errlog.Println(tx.Error)
			os.Exit(1)
		}
		if tx.RowsAffected != 1 {
			errlog.Printf("更新用户hst0余额失败，操作影响记录条数：%d.", tx.RowsAffected)
			os.Exit(1)
		}
		if tx = tx.Exec(sql_upd2, v.Rwds, v.UserID); tx.Error != nil {
			errlog.Println(tx.Error)
			os.Exit(1)
		}
		if tx.RowsAffected != 1 {
			errlog.Printf("更新用户hst0余额失败，操作影响记录条数：%d.", tx.RowsAffected)
			os.Exit(1)
		}
		if tx = tx.Exec(sql_upd3, pid2, v.ID); tx.Error != nil {
			errlog.Println(tx.Error)
			os.Exit(1)
		}
		if tx.RowsAffected != 1 {
			errlog.Printf("更新奖励记录奖励状态失败，操作影响记录条数：%d.", tx.RowsAffected)
			os.Exit(1)
		}
	}
	tx.Commit()

	spendTime = time.Since(start)
	inflog.Printf("执行时间：%v，共奖励%d人次，总计%sHST0.", spendTime, len(users), tot)

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
