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
	"github.com/satori/uuid"
	"github.com/shopspring/decimal"
)

type User struct {
	UserID  uint
	Acct    string
	HST     decimal.Decimal `gorm:"column:hsts"`
	HSC     decimal.Decimal `gorm:"column:hsc"`
	HSCLock decimal.Decimal `gorm:"column:hsc_lock"`
	HSCCir  decimal.Decimal `gorm:"column:hsc_cir"`
	Lss     []EcoGovUpgLockStatus
	USDT    decimal.Decimal `gorm:"column:usdts"`
	HST0    decimal.Decimal `gorm:"column:hst0s"`
	ChgNum  uint
}

type EcoGovUpgLockStatus struct {
	ID       uint
	UserID   uint
	Lock     decimal.Decimal
	Unlock   decimal.Decimal
	LockRate decimal.Decimal `gorm:"-"`
	LockDate string
	Created  string          `gorm:"-"`
	Updated  string          `gorm:"-"`
	UnlDay   decimal.Decimal `gorm:"-"`
}

// 交易记录
type TransRecord struct {
	ID         uint            `gorm:"primary_key" json:"id" form:"user_id"`
	Pid        string          `json:"pid"`
	UserID     uint            `json:"user_id" gorm:"column:user_id"`
	FromAcct   string          `json:"from_acct"`
	FromAddr   string          `json:"from_addr"`
	ToAcct     string          `json:"to_acct"`
	ToAddr     string          `json:"to_addr"`
	Asset      string          `json:"asset"`
	Amount     decimal.Decimal `json:"amount"`
	Fee        decimal.Decimal `json:"fee"`
	Typ        uint            `json:"typ"`
	Stat       uint            `json:"stat"`
	Remark     string          `json:"remark"`
	ExchAmount decimal.Decimal `json:"exch_amount"`
	TxHash     string          `json:"tx_hash"`
	Created    string          `json:"created" form:"created" gorm:"-"`
	Updated    *time.Time      `json:"updated" gorm:"-"`
}

var (
	db                                           *gorm.DB
	errlog                                       = log.New(os.Stdout, "[Err] ", log.Ldate|log.Ltime|log.Lshortfile)
	inflog                                       = log.New(os.Stdout, "[Info] ", log.Ldate|log.Ltime|log.Lshortfile)
	warlog                                       = log.New(os.Stdout, "[War] ", log.Ldate|log.Ltime|log.Lshortfile)
	fromAcctsStr, toAcct, ctn                    string
	fromAccts                                    []string
	remUnlock                                    decimal.Decimal
	start                                        time.Time
	spendTime                                    time.Duration
	url, remark                                  string
	totHST, totHSC, totHSCLock, totUSDT, totHST0 decimal.Decimal
	transUSDTHST0                                bool
)

func main() {

	// flag.StringVar(&date, "date", "", "rwds/subsidy date")
	flag.StringVar(&fromAcctsStr, "fromAccts", "", "转出方账号，逗号分隔，如：\"18138440982,13512542584\"")
	flag.StringVar(&toAcct, "toAcct", "", "接收方账号，如：\"12345678901\"")
	flag.StringVar(&remark, "remark", "", "转账备注：\"系统转账\"")
	flag.BoolVar(&transUSDTHST0, "transUSDTHST0", false, "是否转USDT和HST0，如true表示转USDT和HST0")
	// flag.StringVar(&url, "url", "", "url to connect to mysql, e.g.:\"root:rich_hst_777@(192.168.182.128:33067)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true\"")
	flag.Parse()
	// params validator 参数校验
	// if url == "" {
	// 	errlog.Printf("数据库连接url不可为空，例子：\"root:rich_hst_777@(192.168.182.128:33067)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true\"")
	// 	os.Exit(1)
	// }
	// if remark == "" {
	// 	errlog.Printf("转账备注不可为空！")
	// 	os.Exit(1)
	// }
	fromAccts = strings.Split(fromAcctsStr, ",")

	url := "root:rich_hst_777@(192.168.182.128:33067)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
	// url := "hsc:Hschain2020hkdb@(rm-j6c4plvy6m87cy88m90110.mysql.rds.aliyuncs.com:3306)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"

	db, err := InitMysql(url)
	if err != nil {
		errlog.Printf("数据库连接失败！%s", err)
		os.Exit(1)
	}
	defer db.Close()

	start = time.Now()

	var sql_fu string
	if transUSDTHST0 {
		sql_fu = "select id user_id, hsts, hsc, hsc_lock, hsc - hsc_lock hsc_cir, usdts, hst0s, coalesce(mobile,email_addr) acct from users where coalesce(mobile,email_addr) in (?)"
	} else {
		sql_fu = "select id user_id, hsts, hsc, hsc_lock, hsc - hsc_lock hsc_cir, coalesce(mobile,email_addr) acct from users where coalesce(mobile,email_addr) in (?)"
	}
	sql_lss := "select id, user_id, `lock`, `unlock` from eco_gov_upg_lock_statuses where user_id in (?) and `lock` <> `unlock` order by user_id, lock_date"
	sql_tu := "select id user_id, coalesce(mobile,email_addr) acct from users where coalesce(mobile,email_addr) = ?"
	// 查询待转出用户信息
	fromUsers := []User{}
	if err := db.Raw(sql_fu, fromAccts).Scan(&fromUsers).Error; err != nil {
		errlog.Println("查询转出用户错误：%s！", err)
		os.Exit(1)
	}
	if len(fromUsers) != len(fromAccts) {
		errlog.Println("len(fromUsers) :%v，len(fromAccts):%v，转出用户不存在！", len(fromUsers), len(fromAccts))
		os.Exit(1)
	}
	fids := []uint{}
	for _, fu := range fromUsers {
		fids = append(fids, fu.UserID)
	}
	lss := []EcoGovUpgLockStatus{}
	if err := db.Raw(sql_lss, fids).Scan(&lss).Error; err != nil {
		errlog.Println("查询转出用户锁仓状态记录错误：%s！", err)
		os.Exit(1)
	}
	fuMap := make(map[uint]*User)
	for i, u := range fromUsers {
		fuMap[u.UserID] = &fromUsers[i]
	}
	for i, ls := range lss {
		fuMap[ls.UserID].Lss = append(fuMap[ls.UserID].Lss, lss[i])
	}
	for _, fu := range fuMap {
		totHST = totHST.Add(fu.HST)
		totHSC = totHSC.Add(fu.HSC)
		totHSCLock = totHSCLock.Add(fu.HSCLock)
		totUSDT = totUSDT.Add(fu.USDT)
		totHST0 = totHST0.Add(fu.HST0)
		if remUnlock, fu.ChgNum = UnlockSpecifiedAmount(fu.HSCLock, fu.Lss); !remUnlock.Equal(decimal.Zero) {
			inflog.Printf("剩余无法从锁仓状态记录中扣除余额：%v，该剩余量不为0。（转账需要余额：%v）", remUnlock, fu.HSCLock)
			os.Exit(1)
		}
	}
	tu := User{}
	if err := db.Raw(sql_tu, toAcct).Scan(&tu).Error; err != nil {
		errlog.Println("查询接收用户错误：%s！", err)
		os.Exit(1)
	}

	inflog.Printf("转出方：%s，人次：%d，总计%sHST，%sHSC，%s锁仓HSC，%sUSDT，%sHST0，继续执行？y/n", fromAcctsStr, len(fromUsers), totHST, totHSC, totHSCLock, totUSDT, totHST0)
	fmt.Scanln(&ctn)
	if ctn == "y" || ctn == "yes" {
		inflog.Printf("输入%s，继续执行....", ctn)
	} else {
		inflog.Printf("输入%s，终止程序.", ctn)
		os.Exit(1)
	}

	tx := db.Begin()
	defer tx.Rollback()
	// rate1 := decimal.NewFromFloat(1.01)
	// rate2 := decimal.NewFromFloat(0.01)
	// var trAmount, fee decimal.Decimal
	sql_upd_fu := "update users u left join pool_rigs pr on pr.rig_id = u.id left join node_pools np on np.pool_id = pr.pool_id set u.hsts = u.hsts - ?, u.hsc = u.hsc - ?, u.hsc_lock = u.hsc_lock - ?, pr.hl = pr.hl - ?, np.shl = np.shl - ?, u.usdts = u.usdts - ?, u.hst0s = u.hst0s - ? where u.id = ? and u.hsts >= ? and u.hsc >= ? and u.hsc_lock >= ? and round(u.hsc - ?,4) >= round(u.hsc_lock - ?,4) and (pr.pool_id is null or pr.hl >= ?) and (np.node_id is null or np.shl >= ?) and u.usdts >= ? and u.hst0s >= ?" // 更新转出方余额"
	// sql_upd_fu := "update users u left join pool_rigs pr on pr.rig_id = u.id left join node_pools np on np.pool_id = pr.pool_id set u.hsts = u.hsts - ?, u.hsc = u.hsc - ?, u.hsc_lock = u.hsc_lock - ?, pr.hl = pr.hl - ?, np.shl = np.shl - ? where u.id = ? and u.hsts >= ? and u.hsc >= ? and u.hsc_lock >= ? and round(u.hsc - ?,4) >= round(u.hsc_lock - ?,4) and (pr.pool_id is null or pr.hl >= ?) and (np.node_id is null or np.shl >= ?) "                                                                                // 更新转出方余额"
	sql_upd_tu := "update users u left join pool_rigs pr on pr.rig_id = u.id left join node_pools np on np.pool_id = pr.pool_id set u.hsts = u.hsts + ?, u.hsc = u.hsc + ?, u.hsc_lock = u.hsc_lock + ?, pr.hl = pr.hl + ?, np.shl = np.shl + ?, u.usdts = u.usdts + ?, u.hst0s = u.hst0s + ? where u.id = ?"
	// sql_upd_tu := "update users u left join pool_rigs pr on pr.rig_id = u.id left join node_pools np on np.pool_id = pr.pool_id set u.hsts = u.hsts + ?, u.hsc = u.hsc + ?, u.hsc_lock = u.hsc_lock + ?, pr.hl = pr.hl + ?, np.shl = np.shl + ? where u.id = ?"
	sql_upd_ls := "update eco_gov_upg_lock_statuses set `unlock` = ?, lock_rate = ? where id = ? and `lock` >= ?"
	sql_upd_touser_ls := "insert into eco_gov_upg_lock_statuses(user_id,`lock`,lock_date) values(?,?,curdate()) on duplicate key update `lock` = `lock` + values(`lock`), lock_rate = (`lock`-`unlock`)/`lock`" // 插入/更新到账用户锁仓状态
	sql_ins_ur := "insert into eco_gov_upg_unlock_records(user_id,typ,amount) values(?,4,?)"                                                                                                                    // 插入转账方解锁记录

	for _, fu := range fromUsers {
		if tx = tx.Exec(sql_upd_fu, fu.HST, fu.HSC, fu.HSCLock, fu.HSCLock, fu.HSCLock, fu.USDT, fu.HST0, fu.UserID, fu.HST, fu.HSC, fu.HSCLock, fu.HSC, fu.HSCLock, fu.HSCLock, fu.HSCLock, fu.USDT, fu.HST0); tx.Error != nil || tx.RowsAffected != 1 && tx.RowsAffected != 3 {
			// if tx = tx.Exec(sql_upd_fu, fu.HST, fu.HSC, fu.HSCLock, fu.HSCLock, fu.HSCLock, fu.UserID, fu.HST, fu.HSC, fu.HSCLock, fu.HSC, fu.HSCLock, fu.HSCLock, fu.HSCLock); tx.Error != nil || tx.RowsAffected != 1 && tx.RowsAffected != 3 {
			errlog.Println(err)
			os.Exit(1)
		}
		trs := []TransRecord{}
		if fu.HST.IsPositive() {
			pid := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
			trs = append(trs, TransRecord{UserID: fu.UserID, Typ: 1, FromAcct: fu.Acct, ToAcct: tu.Acct, Pid: pid, Asset: "hst", Amount: fu.HST, Fee: decimal.Zero, Remark: remark}) // 插入转账记录
			trs = append(trs, TransRecord{UserID: tu.UserID, Typ: 0, FromAcct: fu.Acct, ToAcct: tu.Acct, Pid: pid, Asset: "hst", Amount: fu.HST, Fee: decimal.Zero, Remark: remark}) // 插入转账记录
		}
		if fu.HSCLock.IsPositive() {
			pid := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
			trs = append(trs, TransRecord{UserID: fu.UserID, Typ: 1, FromAcct: fu.Acct, ToAcct: tu.Acct, Pid: pid, Asset: "hsc_lock", Amount: fu.HSCLock, Fee: decimal.Zero, Remark: remark}) // 插入转账记录
			trs = append(trs, TransRecord{UserID: tu.UserID, Typ: 0, FromAcct: fu.Acct, ToAcct: tu.Acct, Pid: pid, Asset: "hsc_lock", Amount: fu.HSCLock, Fee: decimal.Zero, Remark: remark}) // 插入转账记录
		}
		if fu.HSCCir.IsPositive() {
			pid := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
			trs = append(trs, TransRecord{UserID: fu.UserID, Typ: 1, FromAcct: fu.Acct, ToAcct: tu.Acct, Pid: pid, Asset: "hsc_cir", Amount: fu.HSCCir, Fee: decimal.Zero, Remark: remark}) // 插入转账记录
			trs = append(trs, TransRecord{UserID: tu.UserID, Typ: 0, FromAcct: fu.Acct, ToAcct: tu.Acct, Pid: pid, Asset: "hsc_cir", Amount: fu.HSCCir, Fee: decimal.Zero, Remark: remark}) // 插入转账记录
		}
		if transUSDTHST0 && fu.USDT.IsPositive() {
			pid := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
			trs = append(trs, TransRecord{UserID: fu.UserID, Typ: 1, FromAcct: fu.Acct, ToAcct: tu.Acct, Pid: pid, Asset: "usdt", Amount: fu.USDT, Fee: decimal.Zero, Remark: remark}) // 插入转账记录
			trs = append(trs, TransRecord{UserID: tu.UserID, Typ: 0, FromAcct: fu.Acct, ToAcct: tu.Acct, Pid: pid, Asset: "usdt", Amount: fu.USDT, Fee: decimal.Zero, Remark: remark}) // 插入转账记录
		}
		if transUSDTHST0 && fu.HST0.IsPositive() {
			pid := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
			trs = append(trs, TransRecord{UserID: fu.UserID, Typ: 1, FromAcct: fu.Acct, ToAcct: tu.Acct, Pid: pid, Asset: "hst0", Amount: fu.HST0, Fee: decimal.Zero, Remark: remark}) // 插入转账记录
			trs = append(trs, TransRecord{UserID: tu.UserID, Typ: 0, FromAcct: fu.Acct, ToAcct: tu.Acct, Pid: pid, Asset: "hst0", Amount: fu.HST0, Fee: decimal.Zero, Remark: remark}) // 插入转账记录
		}
		for _, tr := range trs {
			if err := tx.Create(&tr).Error; err != nil {
				errlog.Printf("创建交易记录失败：%v.", err)
				os.Exit(1)
			}
		}
		for _, ls := range fu.Lss {
			if tx = tx.Exec(sql_upd_ls, ls.Unlock, ls.LockRate, ls.ID, ls.Unlock); tx.Error != nil || tx.RowsAffected != 1 {
				if fu.ChgNum <= 0 && tx.RowsAffected == 0 {
					break
				}
				errlog.Printf("更新用户锁仓状态信息失败，err：%v，影响记录数：%d.", tx.Error, tx.RowsAffected)
				os.Exit(1)
			}
			fu.ChgNum--
		}
		if tx = tx.Exec(sql_ins_ur, fu.UserID, fu.HSCLock); tx.Error != nil || tx.RowsAffected != 1 {
			errlog.Printf("插入转账方解锁记录失败，err：%v，影响记录数：%d.", tx.Error, tx.RowsAffected)
			os.Exit(1)
		}
	}
	if tx = tx.Exec(sql_upd_tu, totHST, totHSC, totHSCLock, totHSCLock, totHSCLock, totUSDT, totHST0, tu.UserID); tx.Error != nil || tx.RowsAffected != 1 && tx.RowsAffected != 3 {
		// if tx = tx.Exec(sql_upd_tu, totHST, totHSC, totHSCLock, totHSCLock, totHSCLock, tu.UserID); tx.Error != nil || tx.RowsAffected != 1 && tx.RowsAffected != 3 {
		errlog.Println(err)
		os.Exit(1)
	}
	if tx = tx.Exec(sql_upd_touser_ls, tu.UserID, totHSCLock); tx.Error != nil || tx.RowsAffected != 1 && tx.RowsAffected != 2 {
		errlog.Printf("更新到账用户锁仓状态信息失败，err：%v，影响记录数：%d.", tx.Error, tx.RowsAffected)
		os.Exit(1)
	}
	tx.Commit()

	spendTime = time.Since(start)
	inflog.Printf("执行时间：%v，共转移HST：%sHST，流通HSC：%s流通HSC，锁仓HSC：%s锁仓HSC，USDT：%sU，HST0：%sHST0.", spendTime, totHST, totHSC, totHSCLock, totUSDT, totHST0)

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

// 解锁指定数量
func UnlockSpecifiedAmount(amount decimal.Decimal, lss []EcoGovUpgLockStatus) (decimal.Decimal, uint) {
	zero := decimal.NewFromInt(0)
	chgNum := uint(0)
	for i, ls := range lss {
		if amount.Equal(zero) {
			break
		}
		if ls.Lock.Sub(ls.Unlock).GreaterThanOrEqual(amount) {
			lss[i].Unlock = ls.Unlock.Add(amount)
			lss[i].LockRate = (ls.Lock.Sub(lss[i].Unlock)).Div(ls.Lock).Round(4)
			amount = zero
		} else {
			amount = amount.Sub(ls.Lock.Sub(ls.Unlock))
			lss[i].Unlock = ls.Lock
			lss[i].LockRate = zero
		}
		chgNum++
	}
	return amount, chgNum
}
