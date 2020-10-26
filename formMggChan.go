package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/shopspring/decimal"
)

type FormPool struct {
	ID, IsMiningPool    uint
	MggHsts, UnlockHsts decimal.Decimal
	ToUnlock            decimal.Decimal
	AdjMggHsts          decimal.Decimal
	EffeRigNum          uint
	TotCp               decimal.Decimal
	Penc                decimal.Decimal
}

type SsuCfgs struct {
	FormHsts  decimal.Decimal
	EffeRigCp decimal.Decimal
}

const (
	url = "root:rich_hst_777@(192.168.182.131:33067)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
	// url = "root:rich_hst_777@(192.168.211.128:33067)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
	// url = "hsc:Hschain2020hkdb@(rm-j6c4plvy6m87cy88mdo.mysql.rds.aliyuncs.com:3306)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
)

var (
	penc       = []decimal.Decimal{decimal.NewFromFloat(1), decimal.NewFromFloat(0.9), decimal.NewFromFloat(0.7), decimal.NewFromFloat(0.4)}
	totCp      = []decimal.Decimal{decimal.NewFromFloat(6000), decimal.NewFromFloat(4500), decimal.NewFromFloat(3000), decimal.NewFromFloat(1500)}
	effeRigNum = []uint{20, 15, 10, 5}
)

func main() {

	var (
		errlog, inflog, deblog *log.Logger
		err                    error
		sql_form_to_unlock     = "select id, is_mining_pool , mgg_hsts, unlock_hsts, cond.effe_rig_num, cond.tot_cp from users u left join (select pool_id, sum(if(cmpt_pow>=?,1,0)) effe_rig_num, sum(cmpt_pow) tot_cp from pool_rigs group by pool_id) cond on cond.pool_id = u.id where is_mining_pool = 3 and mgg_hsts - unlock_hsts > 0"
		adjMggInt              int64
		adjMgg                 decimal.Decimal
		sql_form_mggs          = "select form_hsts, effe_rig_cp from ssu_cfgs"
		ssuCfgs                SsuCfgs
		formPools              []FormPool
		ctn                    string
		toUnlock               decimal.Decimal
		sql_upd_user           = "update users set unlock_hsts = ?, mgg_hsts = ?, hsts = hsts + ?, is_mining_pool = ? where id = ? and is_mining_pool = 3 and mgg_hsts - unlock_hsts >= ?"
		sql_ins_unl            = "insert into ssu_unls(user_id,typ,amount) values(?,2,?)"
		sql_upd_mh             = "update ssu_cfgs set form_hsts = ?"
		start                  time.Time
		spendTime              time.Duration
		sql_upd_rules          = "update ssu_unlock_pool_rules set effe_rig_num = (case when penc = 40 then 5 when penc = 70 then 10 when penc = 90 then 15 when penc = 100 then 20 end), tot_cp = effe_rig_num * ?"
		effeRigCp              decimal.Decimal
		erc                    int64
	)

	errlog = log.New(os.Stdout, "[Error] ", log.Ldate|log.Ltime|log.Lshortfile)
	inflog = log.New(os.Stdout, "[Info] ", log.Ldate|log.Ltime|log.Lshortfile)
	deblog = log.New(os.Stdout, "[Deb] ", log.Ldate|log.Ltime|log.Lshortfile)

	flag.Int64Var(&adjMggInt, "adj_mgg", 0, "adjust mgg hsts for upgrade formal")
	flag.Int64Var(&erc, "erc", 0, "effe rig cp")
	flag.Parse()
	adjMgg = decimal.NewFromInt(adjMggInt)
	if adjMgg.Equal(decimal.Zero) {
		errlog.Printf("调整后正式矿池抵押数量 未输入或为0错误：%v", adjMgg)
		os.Exit(1)
	}
	effeRigCp = decimal.NewFromInt(erc)
	if effeRigCp.Equal(decimal.Zero) {
		errlog.Printf("调整后有效矿工算力要求 未输入或为0错误：%v", erc)
		os.Exit(1)
	}

	inflog.Printf("mysql url:%v\n", url)
	inflog.Printf("输入调整后正式矿池抵押数量为%sHST，确认吗？y/n.\n", adjMgg)
	fmt.Scanln(&ctn)
	if ctn != "y" && ctn != "yes" {
		inflog.Printf("输入%s，结束程序.", ctn)
		os.Exit(1)
	}

	if db, err = InitMysql(url); err != nil {
		errlog.Printf("数据库连接失败：%s", err.Error())
		os.Exit(1)
	}
	defer db.Close()

	start = time.Now()

	if err := db.Raw(sql_form_mggs).Scan(&ssuCfgs).Error; err != nil {
		errlog.Printf("查询当前升级正式矿池抵押数量失败：%s", err.Error())
		os.Exit(1)
	}

	if err := db.Raw(sql_form_to_unlock, effeRigCp).Scan(&formPools).Error; err != nil {
		errlog.Printf("查询待释放正式矿池用户失败：%s", err.Error())
		os.Exit(1)
	}

	deblog.Printf("%v.", formPools)

	for i, v := range formPools {
		if v.MggHsts.Equals(decimal.Zero) {
			errlog.Printf("用户抵押HST数量为0错误，v：%v.", v)
			os.Exit(1)
		}
		for i2, v2 := range penc {
			if v.EffeRigNum >= effeRigNum[i2] && v.TotCp.GreaterThanOrEqual(totCp[i2]) {
				formPools[i].Penc = v2
				break
			}
		}
		toUnlock = v.MggHsts.Sub(v.UnlockHsts).Sub(adjMgg.Sub(adjMgg.Mul(formPools[i].Penc)))
		if toUnlock.GreaterThan(decimal.Zero) {
			formPools[i].ToUnlock = toUnlock
			formPools[i].UnlockHsts = adjMgg.Mul(formPools[i].Penc)
		} else {
			formPools[i].UnlockHsts = adjMgg.Sub(v.MggHsts.Sub(v.UnlockHsts))
		}
		formPools[i].MggHsts = adjMgg
		if formPools[i].Penc.Equal(decimal.NewFromInt(1)) {
			formPools[i].IsMiningPool = 5
			formPools[i].UnlockHsts = decimal.Zero
			formPools[i].MggHsts = decimal.Zero
		}
	}

	tx := db.Begin()
	defer tx.Rollback()
	for _, v := range formPools {
		if tx = tx.Exec(sql_upd_user, v.UnlockHsts, v.MggHsts, v.ToUnlock, v.IsMiningPool, v.ID, v.ToUnlock); tx.Error != nil || tx.RowsAffected != 1 {
			errlog.Printf("更新用户HST抵押数量、HST余额失败，err：%v，tx.RowsAffected：%d.", tx.Error, tx.RowsAffected)
			os.Exit(1)
		}
		if v.ToUnlock.GreaterThan(decimal.Zero) {
			if tx = tx.Exec(sql_ins_unl, v.ID, v.ToUnlock); tx.Error != nil || tx.RowsAffected != 1 {
				errlog.Printf("插入释放记录失败，err：%v，tx.RowsAffected：%d.", tx.Error, tx.RowsAffected)
				os.Exit(1)
			}
		}
	}
	if tx = tx.Exec(sql_upd_mh, adjMgg); tx.Error != nil || tx.RowsAffected != 1 {
		errlog.Printf("更新升级正式矿池抵押HST数量失败，err：%v，tx.RowsAffected：%d.", tx.Error, tx.RowsAffected)
		os.Exit(1)
	}
	if tx = tx.Exec(sql_upd_rules, ssuCfgs.EffeRigCp); tx.Error != nil || tx.RowsAffected != 4 {
		errlog.Printf("更新升级正式矿池规则失败，err：%v，tx.RowsAffected：%d.", tx.Error, tx.RowsAffected)
		os.Exit(1)
	}
	tx.Commit()
	spendTime = time.Since(start)
	inflog.Printf("执行时间：%v，更新后升级正式矿池抵押HST数量：%s，释放用户数：%d.", spendTime, adjMgg, len(formPools))

	formPoolsStr := ""
	for _, v := range formPools {
		formPoolsStr += fmt.Sprintf(",%d", v.ID)
	}
	deblog.Printf("formPoolsStr:%s.", formPoolsStr)

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
