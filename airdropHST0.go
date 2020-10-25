package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/satori/go.uuid"
	"github.com/shopspring/decimal"
)

type User struct {
	ID           uint
	Hst0s        decimal.Decimal
	Acct         string
	InvtID       *uint
	InvtPoolID   *uint
	InvtPoolAcct *string
	InvtNodeID   *uint
	InvtNodeAcct *string
}

type TransRecord struct {
	Typ        uint
	Pid        string
	UserID     uint
	Asset      string
	Amount     decimal.Decimal
	ExchAmount decimal.Decimal
	Remark     string
}

type rate struct {
	InvterRwdsRate, InvtPoolRwdsRate, InvtNodeRwdsRate, TodCmptPowCoe decimal.Decimal
}

var mobileRegexp = regexp.MustCompile("^\\d{11}$")

func main() {

	var (
		errlog, inflog               *log.Logger
		user                         User
		rwdUsers                     []User
		err                          error
		userAcct, toUsersStr, remark string
		rwdsInt                      int64
		rwds, hst0PerRwd, hst0Sum    decimal.Decimal
		// toNum                                                 int
		toUsers                                               []string
		ctn                                                   string
		invterRwds, invtPoolRwds, invtNodeRwds, todCmptPowCoe decimal.Decimal
		rates                                                 rate
		sql_user                                              = "select id, coalesce(mobile,email_addr) acct, hst0s from users where mobile = ? or email_addr = ?"
		sql_rwdUsers                                          = "select u1.id, coalesce(u1.mobile,u1.email_addr) acct from users u1 left join users u2 on u1.invt_id = u2.id and u2.is_mining_rig = 1 where ( u1.mobile in (?) or u1.email_addr in (?) ) and u1.is_mining_rig = 1 order by field (u1.mobile,?), u1.email_addr,?"
		sql_rate                                              = "select invter_rwds_rate, invt_pool_rwds_rate, invt_node_rwds_rate, tod_cmpt_pow_coe from configs"
		sql_invt_pool1                                        = "select pr2.pool_id invt_pool_id, coalesce(u.mobile,u.email_addr) invt_pool_acct from pool_rigs pr1 inner join pool_rigs pr2 on pr1.pool_id = pr2.rig_id inner join pool_rigs pr3 on pr2.pool_id = pr3.rig_id inner join users u on u.id = pr2.rig_id where pr1.rig_id = ? and pr2.rig_id != pr2.pool_id"
		sql_invt_pool2                                        = "select ipr.invt_pool_id invt_pool_id, coalesce(u.mobile,u.email_addr) invt_pool_acct from invt_pool_relts ipr inner join users u on u.id = ipr.be_invted_pool_id inner join pool_rigs pr on pr.pool_id = ipr.be_invted_pool_id where pr.rig_id = ?"
		sql_invt_pool3                                        = "select fe.form_id invt_pool_id, coalesce(u.mobile,u.email_addr) invt_pool_acct from ssu_form_exp_relts fe inner join users u on u.id = fe.exp_id inner join pool_rigs pr on pr.pool_id = fe.exp_id where pr.rig_id = ?"
		sql_invt_node                                         = "select son.ori_id invt_node_id, coalesce(u.mobile,u.email_addr) invt_node_acct from pool_rigs pr inner join node_pools np on pr.pool_id = np.pool_id inner join ssu_ori_new_nodes son on son.new_id = np.node_id inner join users u on u.id = son.new_id where pr.rig_id = ?"
		sql_upd_user                                          = "update users set hst0s = hst0s - ? where id = ? and hst0s >= ?"
		sql_upd_rwd                                           = "update users u inner join pool_rigs pr on pr.rig_id = u.id inner join node_pools np on np.pool_id = pr.pool_id set u.cmpt_pow = u.cmpt_pow + ?, pr.cmpt_pow = pr.cmpt_pow + ?, np.sum_cmpt_pow = np.sum_cmpt_pow + ?, accu_invt_pool_rwds = accu_invt_pool_rwds + ?, accu_invt_node_rwds = accu_invt_node_rwds + ? where id = ?"
		trs                                                   []TransRecord
		pid, pid2, pid3                                       string
		rwdUsersStr                                           string
		start                                                 time.Time
		spendTime                                             time.Duration
	)
	flag.StringVar(&userAcct, "acct", "", "acct")
	flag.StringVar(&toUsersStr, "to_users", "", "to rwds hst0 users array which is string and split by comma")
	flag.Int64Var(&rwdsInt, "rwd", 0, "airdrop cmpt_pow rwd")
	flag.StringVar(&remark, "remark", "", "remark")
	flag.Parse()

	errlog = log.New(os.Stdout, "[Error] ", log.Ldate|log.Ltime|log.Lshortfile)
	inflog = log.New(os.Stdout, "[Info] ", log.Ldate|log.Ltime|log.Lshortfile)

	// params validator 参数校验
	// toUsersStr = strings.ReplaceAll(toUsersStr, ",,", ",")
	if toUsersStr == "" {
		errlog.Printf("奖励/转账用户数参数错误：nil")
		os.Exit(1)
	}
	// toNum = strings.Count(toUsersStr, ",") + 1
	toUsers = strings.Split(toUsersStr, ",")

	rwds = decimal.NewFromInt(rwdsInt)
	if rwds.Equal(decimal.Zero) {
		errlog.Printf("奖励数未输入或为0错误：%v", rwds)
		os.Exit(1)
	}

	if db, err = InitMysql(); err != nil {
		errlog.Printf("数据库连接失败：%s", err.Error())
		os.Exit(1)
	}
	defer db.Close()

	start = time.Now()

	if err := db.Raw(sql_user, userAcct, userAcct).Scan(&user).Error; err != nil {
		errlog.Printf("查询空投方用户失败：%s", err.Error())
		os.Exit(1)
	}

	rwds = decimal.NewFromInt(rwdsInt)
	if err := db.Raw(sql_rwdUsers, toUsers, toUsers, toUsers, toUsers).Scan(&rwdUsers).Error; err != nil {
		errlog.Printf("查询奖励/到账用户失败：%s", err.Error())
		os.Exit(1)
	}
	for _, v := range rwdUsers {
		rwdUsersStr += v.Acct + ","
	}
	inflog.Printf("len(rwdUsers):%d,len(toUsers):%d.", len(rwdUsers), len(toUsers))
	if len(rwdUsers) != len(toUsers) {
		inflog.Printf("查询到的矿工用户数与想要奖励的用户数不一致，是否继续执行奖励？y/n.\n查询结果：%s.\n输入用户信息：%s.\n", rwdUsersStr, strings.Join(toUsers, ","))
		fmt.Scanln(&ctn)
		if ctn == "y" || ctn == "yes" {
			inflog.Printf("输入%s，继续执行....", ctn)
		} else {
			inflog.Printf("输入%s，结束程序.", ctn)
			os.Exit(1)
		}
	}

	if err := db.Raw(sql_rate).Scan(&rates).Error; err != nil {
		errlog.Printf("查询邀请奖励、今日算力系数失败：%s", err.Error())
		os.Exit(1)
	}
	if rates.InvterRwdsRate.Equal(decimal.Zero) || rates.InvtPoolRwdsRate.Equal(decimal.Zero) || rates.InvtNodeRwdsRate.Equal(decimal.Zero) || rates.TodCmptPowCoe.Equal(decimal.Zero) {
		errlog.Printf("邀请奖励、今日算力系数为零错误")
		os.Exit(1)
	}
	invterRwds, invtPoolRwds, invtNodeRwds, todCmptPowCoe = rates.InvterRwdsRate.Mul(rwds), rates.InvtPoolRwdsRate.Mul(rwds), rates.InvtNodeRwdsRate.Mul(rwds), rates.TodCmptPowCoe
	inflog.Printf("invterRwds:%s, invtPoolRwds:%s, invtNodeRwds:%s, todCmptPowCoe:%s.", invterRwds, invtPoolRwds, invtNodeRwds, todCmptPowCoe)

	hst0PerRwd = rwds.Mul(todCmptPowCoe)
	hst0Sum = hst0PerRwd.Mul(decimal.NewFromInt(int64(len(rwdUsers))))
	if user.Hst0s.LessThan(hst0Sum) {
		errlog.Printf("用户HST0余额%s少于奖励总数%s*%d=%s！", user.Hst0s, rwds, len(rwdUsers), hst0Sum)
		os.Exit(1)
	}

	user.Acct = HideAcct(user.Acct)
	for i, _ := range rwdUsers {
		if err := db.Raw(sql_invt_pool1, rwdUsers[i].ID).Scan(&rwdUsers[i]).Error; err != nil && err.Error() != RecordNotFound {
			errlog.Printf("查询用户推荐矿池失败，sql_invt_pool1：%s", err.Error())
			os.Exit(1)
		}
		if rwdUsers[i].InvtPoolID == nil {
			if err := db.Raw(sql_invt_pool2, rwdUsers[i].ID).Scan(&rwdUsers[i]).Error; err != nil && err.Error() != RecordNotFound {
				errlog.Printf("查询用户推荐矿池失败，sql_invt_pool2：%s", err.Error())
				os.Exit(1)
			}
		}
		if rwdUsers[i].InvtPoolID == nil {
			if err := db.Raw(sql_invt_pool3, rwdUsers[i].ID).Scan(&rwdUsers[i]).Error; err != nil && err.Error() != RecordNotFound {
				errlog.Printf("查询用户推荐矿池失败，sql_invt_pool3：%s", err.Error())
				os.Exit(1)
			}
		}
		if err := db.Raw(sql_invt_node, rwdUsers[i].ID).Scan(&rwdUsers[i]).Error; err != nil && err.Error() != RecordNotFound {
			errlog.Printf("查询用户推荐节点失败，sql_invt_node：%s", err.Error())
			os.Exit(1)
		}
		pid = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
		pid2 = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
		rwdUsers[i].Acct = HideAcct(rwdUsers[i].Acct)
		tr_user := TransRecord{Typ: 9, Pid: pid, UserID: user.ID, Asset: "hst0", Amount: hst0PerRwd, Remark: fmt.Sprintf("消耗%sHST0帮用户%s兑换%sT算力，%s****%s", hst0PerRwd, rwdUsers[i].Acct, rwds, pid2[:3], pid2[28:])}
		tr_rwdUser := TransRecord{Typ: 9, Pid: pid2, UserID: rwdUsers[i].ID, Asset: "hst0", ExchAmount: rwds, Remark: fmt.Sprintf("来自用户%s消耗%sHST0兑换的%sT算力，%s****%s", user.Acct, hst0PerRwd, rwds, pid[:3], pid[28:])}
		trs = append(trs, tr_user)
		trs = append(trs, tr_rwdUser)
		if rwdUsers[i].InvtID != nil {
			rwdUsers[i].Acct = HideAcct(rwdUsers[i].Acct)
			pid3 = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
			tr_invt_user := TransRecord{Typ: 6, Pid: pid3, UserID: *rwdUsers[i].InvtID, Asset: "cmpt_pow", Amount: invterRwds, Remark: "矿工：" + rwdUsers[i].Acct}
			trs = append(trs, tr_invt_user)
		}
		if rwdUsers[i].InvtPoolID != nil {
			*rwdUsers[i].InvtPoolAcct = HideAcct(*rwdUsers[i].InvtPoolAcct)
			pid3 = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
			tr_invt_pool := TransRecord{Typ: 11, Pid: pid3, UserID: *rwdUsers[i].InvtPoolID, Asset: "cmpt_pow", Amount: invtPoolRwds, Remark: "推荐矿池主：" + *rwdUsers[i].InvtPoolAcct}
			trs = append(trs, tr_invt_pool)
		}
		if rwdUsers[i].InvtNodeID != nil {
			*rwdUsers[i].InvtNodeAcct = HideAcct(*rwdUsers[i].InvtNodeAcct)
			pid3 = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
			tr_invt_node := TransRecord{Typ: 14, Pid: pid, UserID: *rwdUsers[i].InvtNodeID, Asset: "cmpt_pow", Amount: invtNodeRwds, Remark: "推荐节点：" + *rwdUsers[i].InvtNodeAcct}
			trs = append(trs, tr_invt_node)
		}
	}

	tx := db.Begin()
	defer tx.Rollback()
	for _, v := range trs {
		if err = tx.Create(&v).Error; err != nil {
			errlog.Printf("创建交易记录失败，失败原因：%s", err.Error())
			os.Exit(1)
		}
	}
	for _, v := range rwdUsers {
		if tx = tx.Exec(sql_upd_user, hst0PerRwd, user.ID, hst0PerRwd); tx.Error != nil || tx.RowsAffected != 1 {
			errlog.Printf("更新空投方余额失败，err：%v，tx.RowsAffected：%d.", tx.Error, tx.RowsAffected)
			os.Exit(1)
		}
		if tx = tx.Exec(sql_upd_rwd, rwds, rwds, rwds, 0, 0, v.ID); tx.Error != nil || tx.RowsAffected != 3 {
			errlog.Printf("更新奖励方余额失败，err：%v，tx.RowsAffected：%d.", tx.Error, tx.RowsAffected)
			os.Exit(1)
		}
		if v.InvtID != nil {
			if tx = tx.Exec(sql_upd_rwd, invterRwds, invterRwds, invterRwds, 0, 0, v.InvtID); tx.Error != nil || tx.RowsAffected != 3 {
				errlog.Printf("更新奖励方推荐人余额失败，err：%v，tx.RowsAffected：%d.", tx.Error, tx.RowsAffected)
				os.Exit(1)
			}
		}
		if v.InvtPoolID != nil {
			if tx = tx.Exec(sql_upd_rwd, invtPoolRwds, invtPoolRwds, invtPoolRwds, invtPoolRwds, 0, v.InvtPoolID); tx.Error != nil || tx.RowsAffected != 3 {
				errlog.Printf("更新奖励方推荐矿池余额失败，err：%v，tx.RowsAffected：%d.", tx.Error, tx.RowsAffected)
				os.Exit(1)
			}
		}
		if v.InvtNodeID != nil {
			if tx = tx.Exec(sql_upd_rwd, invtNodeRwds, invtNodeRwds, invtNodeRwds, 0, invtNodeRwds, v.InvtNodeID); tx.Error != nil || tx.RowsAffected != 3 {
				errlog.Printf("更新奖励方推荐节点余额失败，err：%v，tx.RowsAffected：%d.", tx.Error, tx.RowsAffected)
				os.Exit(1)
			}
		}
	}
	tx.Commit()
	spendTime = time.Since(start)
	inflog.Printf("执行时间：%v，使用HST0空投算力成功，今日算力系数为：%s，空投方%s消耗%sHST0，奖励%d个用户每人%sT：%s.", spendTime, todCmptPowCoe, userAcct, hst0Sum, len(rwdUsers), rwds, rwdUsersStr)
}

var db *gorm.DB

// 连接数据库，初始化
func InitMysql() (*gorm.DB, error) {

	logMode := true
	maxLifetime := 14400
	maxIdleConns := 10
	maxOpenConns := 100

	// url := "root:rich_hst_777@(192.168.182.131)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
	url := "root:rich_hst_777@(192.168.211.128:33067)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
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

// 获取数据库连接
func GetDB() *gorm.DB {
	return db
}

// 记录没找到错误返回信息
const (
	RecordNotFound = "record not found"
)

func IsMobileAcct(acct string) (isMobile bool) {
	if mobileRegexp.MatchString(acct) {
		isMobile = true
	}
	return
}

func HideAcct(acct string) string {
	if IsMobileAcct(acct) {
		acct = acct[:3] + "****" + acct[7:]
	} else {
		sparkle := "****************************************************************"
		lenAcct := len(acct)
		acct = acct[:2] + sparkle[:lenAcct-7] + acct[lenAcct-5:]
		// reg := regexp.MustCompile(`(.{2})(.*)(.{5})(@.*)`)
		// acct = reg.ReplaceAllString(acct, `${1}****${3}${4}`)
	}
	return acct
}
