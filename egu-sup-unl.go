package main

import (
	"log"
	"os"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/shopspring/decimal"
)

// HSC生态治理升级之解锁用户数据
type EcoGovUpgUnlockUser struct {
	ID              uint
	InvterID        uint
	PoolID          uint
	NodeID          uint
	Lss             []EcoGovUpgLockStatus
	Urs             []EcoGovUpgUnlockRecord
	HscLock         decimal.Decimal
	Unlock          decimal.Decimal
	InvterAccUnls   decimal.Decimal
	InvterAccUnlock decimal.Decimal
	PoolAccUnls     decimal.Decimal
	PoolAccUnlock   decimal.Decimal
	NodeAccUnls     decimal.Decimal
	NodeAccUnlock   decimal.Decimal
	HscWaltAddr     string
	SupUnl          decimal.Decimal
}

// HSC生态治理升级之用户HSC初始化
type EcoGovUpgCfg struct {
	K    string
	V    string
	Cmnt string
}

// HSC生态治理升级之用户解锁记录
type EcoGovUpgUnlockRecord struct {
	UserID     uint            `json:"user_id"`
	Typ        uint            `json:"typ"`
	Amount     decimal.Decimal `json:"amount"`
	AssoAmount decimal.Decimal `json:"asso_amount"`
	IsWithd    bool            `json:"-"`
	Created    string          `gorm:"-" json:"created"`
	Updated    string          `gorm:"-" json:"updated"`
}

// HSC生态治理升级之锁仓状态表
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

func main() {
	// url := "root:rich_hst_777@(192.168.182.131)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
	// url := "root:rich_hst_777@(192.168.182.128:33067)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
	url := "hsc:Hschain2020hkdb@(rm-j6c4plvy6m87cy88m90110.mysql.rds.aliyuncs.com:3306)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
	if db, err = InitMysql(url); err != nil {
		errlog.Printf("数据库连接失败：%s", err.Error())
		os.Exit(1)
	}
	defer db.Close()

	startTime := time.Now()
	sql_cfgs := "select k, v from eco_gov_upg_cfgs where k in ('invter_acc_rate','pool_acc_rate','node_acc_rate')"
	sql_user := "select u.id, u.hsc_lock, round(rls.amount - u.hsc_acc_info->'$.per.unl_td',4) sup_unl, u.invt_id invter_id, pr.pool_id, np.node_id, u.hsc_acc_info->'$.walt_addr' hsc_walt_addr from users u left join fixbug.release20200322 rls on rls.user_id = u.id left join pool_rigs pr on pr.rig_id = u.id left join node_pools np on np.pool_id = pr.pool_id where u.hsc_acc_info->'$.per.unl_td' < rls.amount or u.hsc_lock > 0"
	sql_lss := "select id, user_id, `lock`, `unlock`, lock_rate, datediff(curdate(),lock_date) unl_day, lock_date from eco_gov_upg_lock_statuses where `lock` > `unlock` order by lock_date"

	// 查询今日解锁比例
	cfgs := []EcoGovUpgCfg{}
	var invterAccRate, poolAccRate, nodeAccRate decimal.Decimal
	if err := db.Raw(sql_cfgs).Scan(&cfgs).Error; err != nil {
		errlog.Printf("err:%v.", err)
		return
	}
	var err error
	for _, cfg := range cfgs {
		switch cfg.K {
		case "invter_acc_rate":
			invterAccRate, err = decimal.NewFromString(cfg.V)
		case "pool_acc_rate":
			poolAccRate, err = decimal.NewFromString(cfg.V)
		case "node_acc_rate":
			nodeAccRate, err = decimal.NewFromString(cfg.V)
		default:
			errlog.Printf("logic err")
			return
		}
		if err != nil {
			errlog.Printf("将配置由string转化为Decimal类型错误，err:%v.", err)
			return
		}
	}
	users := []EcoGovUpgUnlockUser{}
	if err := db.Raw(sql_user).Scan(&users).Error; err != nil {
		errlog.Printf("err:%v.", err)
		return
	}
	lss := []EcoGovUpgLockStatus{}
	if err := db.Raw(sql_lss).Scan(&lss).Error; err != nil {
		errlog.Printf("err:%v.", err)
		return
	}

	// 处理用户今日应解锁数据
	userMap := make(map[uint]*EcoGovUpgUnlockUser)
	for i, u := range users {
		userMap[u.ID] = &users[i]
	}
	for i, ls := range lss {
		userMap[ls.UserID].Lss = append(userMap[ls.UserID].Lss, lss[i])
	}

	// 处理用户下属今日释放总量
	zero := decimal.NewFromInt(0)
	type accTd struct {
		Invter decimal.Decimal
		Pool   decimal.Decimal
		Node   decimal.Decimal
	}
	accTdMap := make(map[uint]*accTd)
	for _, u := range userMap {
		if u.SupUnl.LessThan(zero) {
			u.SupUnl = decimal.Zero
		}
		if !u.SupUnl.IsZero() {
			if _, ok := userMap[u.InvterID]; !ok && u.InvterID != 0 {
				accTdMap[u.InvterID] = &accTd{}
			}
			if _, ok := userMap[u.PoolID]; !ok && u.PoolID != 0 {
				accTdMap[u.PoolID] = &accTd{}
			}
			if _, ok := userMap[u.NodeID]; !ok && u.NodeID != 0 {
				accTdMap[u.NodeID] = &accTd{}
			}
		}
	}

	// 用户每日释放
	// minimum := decimal.NewFromFloat(0.0001) // 系统位数最小值
	bugDate := "2021-03-20"
	for uid, u := range userMap {
		for i, ls := range u.Lss {
			if ls.LockDate[:10] == bugDate && !u.SupUnl.IsZero() {
				// if ls.Lock.Sub(ls.Unlock).LessThan(u.HscLock) || ls.Lock.Sub(ls.Unlock).LessThan(u.SupUnl) {
				// 	errlog.Printf("err")
				// 	return
				// }
				u.Unlock = u.SupUnl
				u.Lss[i].Unlock = ls.Unlock.Add(u.SupUnl)
				u.Lss[i].LockRate = (ls.Lock.Sub(u.Lss[i].Unlock)).Div(ls.Lock).Round(4)

				userMap[uid].Urs = append(userMap[uid].Urs, EcoGovUpgUnlockRecord{UserID: uid, Typ: 0, Amount: u.Unlock, AssoAmount: u.HscLock})
				if _, ok := userMap[u.InvterID]; ok {
					userMap[u.InvterID].InvterAccUnls = userMap[u.InvterID].InvterAccUnls.Add(u.Unlock)
				} else if _, ok = accTdMap[u.InvterID]; ok {
					accTdMap[u.InvterID].Invter = accTdMap[u.InvterID].Invter.Add(u.Unlock)
				}
				if _, ok := userMap[u.PoolID]; ok {
					userMap[u.PoolID].PoolAccUnls = userMap[u.PoolID].PoolAccUnls.Add(u.Unlock)
				} else if _, ok = accTdMap[u.PoolID]; ok {
					accTdMap[u.PoolID].Pool = accTdMap[u.PoolID].Pool.Add(u.Unlock)
				}
				if _, ok := userMap[u.NodeID]; ok {
					userMap[u.NodeID].NodeAccUnls = userMap[u.NodeID].NodeAccUnls.Add(u.Unlock)
				} else if _, ok = accTdMap[u.NodeID]; ok {
					accTdMap[u.NodeID].Node = accTdMap[u.NodeID].Node.Add(u.Unlock)
				}
			}
		}
	}

	// 用户加速释放
	var remUnlock decimal.Decimal
	for uid, u := range userMap {
		// 邀请人加速释放
		u.InvterAccUnlock = u.InvterAccUnls.Mul(invterAccRate).Round(4)
		remUnlock, _ = UnlockSpecifiedAmount(u.InvterAccUnlock, userMap[uid].Lss)
		u.InvterAccUnlock = u.InvterAccUnlock.Sub(remUnlock)
		if u.InvterAccUnlock.GreaterThan(zero) {
			userMap[uid].Urs = append(userMap[uid].Urs, EcoGovUpgUnlockRecord{UserID: uid, Typ: 1, Amount: u.InvterAccUnlock, AssoAmount: u.InvterAccUnls})
		}
		// 矿池加速释放
		u.PoolAccUnlock = u.PoolAccUnls.Mul(poolAccRate).Round(4)
		remUnlock, _ = UnlockSpecifiedAmount(u.PoolAccUnlock, userMap[uid].Lss)
		u.PoolAccUnlock = u.PoolAccUnlock.Sub(remUnlock)
		if u.PoolAccUnlock.GreaterThan(zero) {
			userMap[uid].Urs = append(userMap[uid].Urs, EcoGovUpgUnlockRecord{UserID: uid, Typ: 2, Amount: u.PoolAccUnlock, AssoAmount: u.PoolAccUnls})
		}
		// 节点加速释放
		u.NodeAccUnlock = u.NodeAccUnls.Mul(nodeAccRate).Round(4)
		remUnlock, _ = UnlockSpecifiedAmount(u.NodeAccUnlock, userMap[uid].Lss)
		u.NodeAccUnlock = u.NodeAccUnlock.Sub(remUnlock)
		if u.NodeAccUnlock.GreaterThan(zero) {
			userMap[uid].Urs = append(userMap[uid].Urs, EcoGovUpgUnlockRecord{UserID: uid, Typ: 3, Amount: u.NodeAccUnlock, AssoAmount: u.NodeAccUnls})
		}
	}

	// 处理用户释放数量是否待提币
	for _, u := range users {
		if u.HscWaltAddr == "" {
			for i, _ := range u.Urs {
				u.Urs[i].IsWithd = true
			}
		}
	}

	// 更新数据库：1-更新用户解锁资产；2-更新锁仓状态记录；3-插入解锁记录；
	// sql_init_td := "update users set hsc_acc_info = json_set(hsc_acc_info,'$.per.unl_td',0,'$.invter.unl_td',0,'$.invter.acc_td',0,'$.pool.unl_td',0,'$.pool.acc_td',0,'$.node.unl_td',0,'$.node.acc_td',0)"
	sql_upd_user := "update users u left join pool_rigs pr on pr.rig_id = u.id left join node_pools np on np.pool_id = pr.pool_id set u.hsc_lock = u.hsc_lock - ?, u.hsc_acc_info = json_set(u.hsc_acc_info,'$.per.unl_td',round(?+u.hsc_acc_info->'$.per.unl_td',4),'$.per.unl_tt',round(u.hsc_acc_info->'$.per.unl_tt'+?,4),'$.invter.unl_td',round(?+u.hsc_acc_info->'$.invter.unl_td',4),'$.invter.acc_td',round(?+u.hsc_acc_info->'$.invter.acc_td',4),'$.invter.acc_tt',round(u.hsc_acc_info->'$.invter.acc_tt'+?,4), '$.pool.unl_td',round(?+u.hsc_acc_info->'$.pool.unl_td',4),'$.pool.acc_td',round(?+u.hsc_acc_info->'$.pool.acc_td',4),'$.pool.acc_tt',round(u.hsc_acc_info->'$.pool.acc_tt'+?,4), '$.node.unl_td',round(?+u.hsc_acc_info->'$.node.unl_td',4),'$.node.acc_td',round(?+u.hsc_acc_info->'$.node.acc_td',4),'$.node.acc_tt',round(u.hsc_acc_info->'$.node.acc_tt'+?,4) ), pr.hl = pr.hl - ?, np.shl = np.shl - ? where u.id = ? and u.hsc_lock >= ? and (pr.pool_id is null or pr.hl >= ?) and (np.node_id is null or np.shl >= ?)"
	sql_upd_ls := "update eco_gov_upg_lock_statuses set `unlock` = ?, lock_rate = ? where id = ? and `lock` >= ?"
	sql_upd_recd := "update records set v = v + ? where k = 'hsc_cir'"
	sql_upd_td := "update users set hsc_acc_info = json_set(hsc_acc_info,'$.invter.unl_td',?+hsc_acc_info->'$.invter.unl_td','$.pool.unl_td',?+hsc_acc_info->'$.pool.unl_td','$.node.unl_td',?+hsc_acc_info->'$.node.unl_td') where id = ?"
	var totUnlock, hscCirInc decimal.Decimal
	tx := db.Begin()
	defer tx.Rollback()
	// if err := tx.Exec(sql_init_td).Error; err != nil {
	// 	errlog.Printf("err:%v.", err)
	// 	return
	// }
	for uid, u := range userMap {
		totUnlock = u.Unlock.Add(u.InvterAccUnlock).Add(u.PoolAccUnlock).Add(u.NodeAccUnlock)
		hscCirInc = hscCirInc.Add(totUnlock)
		deblog.Printf("u.ID:%v，u.Unlock:%v, u.InvterAccUnlock:%v, u.InvterAccUnls:%v, u.PoolAccUnlock:%v, u.PoolAccUnls:%v, u.NodeAccUnlock:%v, u.NodeAccUnls:%v.", u.ID, u.Unlock, u.InvterAccUnlock, u.InvterAccUnls, u.PoolAccUnlock, u.PoolAccUnls, u.NodeAccUnlock, u.NodeAccUnls)
		if !totUnlock.IsZero() {
			if tx = tx.Exec(sql_upd_user, totUnlock, u.Unlock, u.Unlock, u.InvterAccUnls, u.InvterAccUnlock, u.InvterAccUnlock, u.PoolAccUnls, u.PoolAccUnlock, u.PoolAccUnlock, u.NodeAccUnls, u.NodeAccUnlock, u.NodeAccUnlock, totUnlock, totUnlock, uid, totUnlock, totUnlock, totUnlock); tx.Error != nil || tx.RowsAffected != 1 && tx.RowsAffected != 3 {
				errlog.Printf("tx.Error:%v,tx.RowsAffected:%v.", tx.Error, tx.RowsAffected)
				return
			}
			for _, ls := range u.Lss {
				if tx = tx.Exec(sql_upd_ls, ls.Unlock, ls.LockRate, ls.ID, ls.Unlock); tx.Error != nil || tx.RowsAffected != 1 && ls.Lock.LessThan(ls.Unlock) {
					errlog.Printf("tx.Error:%v,tx.RowsAffected:%v.", tx.Error, tx.RowsAffected)
					return
				}
			}
			for _, ur := range u.Urs {
				if err := tx.Create(&ur).Error; err != nil {
					errlog.Printf("err:%v.", err)
					return
				}
			}
		}
	}
	// 03-17：用户今日没有解锁仍然要显示下属今日解锁总量
	for uid, u := range accTdMap {
		if tx = tx.Exec(sql_upd_td, u.Invter, u.Pool, u.Node, uid); tx.Error != nil || tx.RowsAffected != 1 {
			errlog.Printf("tx.Error:%v,tx.RowsAffected:%d.", tx.Error, tx.RowsAffected)
			return
		}
	}
	if tx = tx.Exec(sql_upd_recd, hscCirInc); tx.Error != nil || tx.RowsAffected != 1 {
		errlog.Printf("tx.Error:%v,tx.RowsAffected:%v.", tx.Error, tx.RowsAffected)
		return
	}
	tx.Commit()

	inflog.Printf("已释放！消耗时间：%v，补充释放HSC：%v。", time.Since(startTime), hscCirInc)
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

var (
	db     *gorm.DB
	err    error
	deblog = log.New(os.Stdout, "[Deb] ", log.Ldate|log.Ltime|log.Lshortfile)
	inflog = log.New(os.Stdout, "[Info] ", log.Ldate|log.Ltime|log.Lshortfile)
	// War = log.New(os.Stdout, "[Warning] ", log.Ldate|log.Ltime|log.Lshortfile)
	errlog = log.New(os.Stdout, "[Error] ", log.Ldate|log.Ltime|log.Lshortfile)
)

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
