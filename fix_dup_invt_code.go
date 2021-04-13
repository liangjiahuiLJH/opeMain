package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"regexp"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

type User struct {
	UserID   uint
	Acct     string
	InvtCode string
}

type DupInvtCode struct {
	InvtCode string
}

var db *gorm.DB

func main() {
	errlog := log.New(os.Stdout, "[Error] ", log.Ldate|log.Ltime|log.Lshortfile)
	inflog := log.New(os.Stdout, "[Info] ", log.Ldate|log.Ltime|log.Lshortfile)
	// sql_dup_invt_code := "select invt_code, count(1) from users group by invt_code having count(1) > 1"
	sql_dup_invt_code := "select invt_code, count(1) from users_copy1 group by invt_code having count(1) > 1"
	// sql_user := "select id user_id, coalesce(mobile,email_addr) acct, invt_code from users where invt_code in (?)"
	sql_user := "select id user_id, coalesce(mobile,email_addr) acct, invt_code from users_copy1 where invt_code in (?)"
	// sql_upd := "update users set invt_code = ? where id = ?"
	sql_upd := "update users_copy1 set invt_code = ? where id = ?"
	dupInvtCodes := []DupInvtCode{}
	users := []User{}
	dupInvtCodeStrs := []string{}
	newInvtCodes := make(map[string]interface{})
	var rowsAffected int64
	var ctn string
	var err error
	var start time.Time
	var spendTime time.Duration

	if db, err = InitMysql(); err != nil {
		errlog.Printf("数据库连接失败：%s", err.Error())
		os.Exit(1)
	}
	defer db.Close()

	start = time.Now()

	if err := db.Raw(sql_dup_invt_code).Scan(&dupInvtCodes).Error; err != nil {
		errlog.Printf("查询邀请码重复信息失败：%s", err.Error())
		os.Exit(1)
	}
	for _, v := range dupInvtCodes {
		dupInvtCodeStrs = append(dupInvtCodeStrs, v.InvtCode)
	}

	if err := db.Raw(sql_user, dupInvtCodeStrs).Scan(&users).Error; err != nil {
		errlog.Printf("查询邀请码重复用户信息失败：%s", err.Error())
		os.Exit(1)
	}

	inflog.Printf("查询到的重复邀请码数：%d，重复邀请码用户数：%d，是否继续修复用户重复邀请码？y/n.\n", len(dupInvtCodeStrs), len(users))
	fmt.Scanln(&ctn)
	if ctn == "y" || ctn == "yes" {
		inflog.Printf("输入%s，继续执行....", ctn)
	} else {
		inflog.Printf("输入%s，结束程序.", ctn)
		os.Exit(1)
	}

	var invtCode string
	var count int
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i, _ := range users {
		// 生成随机邀请码：HSC+手机号后两位+四位随机数 或 HSC+六位随机数
		if isMobileAcct(users[i].Acct) {
			len := len(users[i].Acct)
			// rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		createInvtCode1:
			invtCode = "HSC" + users[i].Acct[len-2:] + fmt.Sprintf("%04v", rnd.Int31n(10000))
			if err := db.Table("users").Where("invt_code = ?", invtCode).Count(&count).Error; err != nil {
				errlog.Printf("查询是否有重复邀请码错误，err：%v.", err)
				os.Exit(1)
			}
			if _, ok := newInvtCodes[invtCode]; count != 0 || ok {
				goto createInvtCode1
			}
			newInvtCodes[invtCode] = nil
		} else {
		createInvtCode2:
			invtCode = "HSC" + fmt.Sprintf("%06v", rnd.Int31n(1000000))
			if err := db.Table("users").Where("invt_code = ?", invtCode).Count(&count).Error; err != nil {
				errlog.Printf("查询是否有重复邀请码错误，err：%v.", err)
				os.Exit(1)
			}
			if _, ok := newInvtCodes[invtCode]; count != 0 || ok {
				goto createInvtCode2
			}
			newInvtCodes[invtCode] = nil
		}
		users[i].InvtCode = invtCode
	}

	tx := db.Begin()
	defer tx.Rollback()

	for _, v := range users {
		if tx = tx.Exec(sql_upd, v.InvtCode, v.UserID); tx.Error != nil || tx.RowsAffected != 1 {
			errlog.Printf("更新用户邀请码失败，err：%v，tx.RowsAffected：%d.", tx.Error, tx.RowsAffected)
			os.Exit(1)
		}
		rowsAffected = rowsAffected + tx.RowsAffected
	}
	inflog.Printf("查询到的重复邀请码数：%d，更新的用户邀请码数：%d，是否确认更新用户重复邀请码（执行事务）？y/n.\n", len(dupInvtCodeStrs), rowsAffected)
	fmt.Scanln(&ctn)
	if ctn == "y" || ctn == "yes" {
		inflog.Printf("输入%s，继续执行....", ctn)
	} else {
		inflog.Printf("输入%s，结束程序.", ctn)
		os.Exit(1)
	}

	tx.Commit()

	spendTime = time.Since(start)
	inflog.Printf("执行时间：%v，重复邀请码数量：%d，更新用户数量：%d.", spendTime, len(dupInvtCodeStrs), rowsAffected)

}

// 连接数据库，初始化
func InitMysql() (*gorm.DB, error) {

	logMode := true
	maxLifetime := 14400
	maxIdleConns := 10
	maxOpenConns := 100

	// url := "root:rich_hst_777@(192.168.182.131)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
	url := "root:rich_hst_777@(192.168.182.128:33067)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
	// url := "hsc:Hschain2020hkdb@(rm-j6c4plvy6m87cy88mdo.mysql.rds.aliyuncs.com:3306)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
	fmt.Printf("mysql url:%v\n", url)

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

var mobileRegexp = regexp.MustCompile("^\\d{11}$")

func isMobileAcct(acct string) (isMobile bool) {
	if mobileRegexp.MatchString(acct) {
		isMobile = true
	}
	return
}
