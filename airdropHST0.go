package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

func main() {
	userID := flag.Int("user_id", 0, "user_id")
	toUsers := flag.String("to_users", "", "to rwds hst0 users array which is string and split by comma")
	// todo CpAmount param

	sql_user := "select user, hst0s"

	var toUser *model.User
	var rwdsRelID uint
	var rwdsRelAcct string
	if params.ToAcct != nil {
		toUser = &model.User{}
		sql_to_user := "select id, coalesce(mobile,email_addr) acct, is_mining_rig from users where mobile = ? or email_addr = ?"
		if err := db.Raw(sql_to_user, params.ToAcct, params.ToAcct).Scan(toUser).Error; err != nil && err.Error() != model.RecordNotFound {
			errlog.Println(err)
			httpRes.ErrRes(c, httpRes.Mysql_Query, "数据库查询错误！")
			return
		} else if err != nil {
			warStr := "该用户不存在"
			warlog.Printf("用户%d给他人兑换算力：%s：%s！", user.ID, warStr, params.ToAcct)
			httpRes.ErrRes(c, httpRes.Exch_Cp_User_Not_Exists, warStr+"，请确认后重新输入！")
			return
		}
		if !toUser.IsMiningRig {
			warStr := "该用户未激活矿机"
			warlog.Printf("用户%d给他人兑换算力：%s：%s！", user.ID, warStr, params.ToAcct)
			httpRes.ErrRes(c, httpRes.Exch_Cp_Rig_Not_Acted, warStr+"，不可给其兑换算力！")
			return
		}
		rwdsRelID = toUser.ID
		toUser.Acct = utils.HideAcct(toUser.Acct)
		*params.ToAcct = utils.HideAcct(*params.ToAcct)
		rwdsRelAcct = toUser.Acct
	} else {
		rwdsRelID = user.ID
		rwdsRelAcct = user.Acct
	}

	invter := &model.User{}
	invtPool := &model.User{}
	invtNode := &model.User{}
	if err := db.Raw("select invt_id id from users u inner join pool_rigs pr on pr.rig_id = u.invt_id where u.id = ?", rwdsRelID).Scan(invter).Error; err != nil && err.Error() != model.RecordNotFound {
		errlog.Println(err)
		httpRes.ErrRes(c, httpRes.Mysql_Query, "数据库查询错误！")
		return
	}
	if err := db.Raw("select fe.form_id id, coalesce(u.mobile,u.email_addr) acct from ssu_form_exp_relts fe inner join users u on u.id = fe.exp_id inner join pool_rigs pr on pr.pool_id = fe.exp_id where pr.rig_id = ?", rwdsRelID).Scan(invtPool).Error; err != nil && err.Error() != model.RecordNotFound {
		errlog.Println(err)
		httpRes.ErrRes(c, httpRes.Mysql_Query, "数据库查询错误！")
		return
	}
	if invtPool.ID == 0 {
		if err := db.Raw("select pr2.pool_id id, coalesce(u.mobile,u.email_addr) acct from pool_rigs pr1 inner join pool_rigs pr2 on pr1.pool_id = pr2.rig_id inner join pool_rigs pr3 on pr2.pool_id = pr3.rig_id inner join users u on u.id = pr2.rig_id where pr1.rig_id = ? and pr2.rig_id != pr2.pool_id", rwdsRelID).Scan(invtPool).Error; err != nil && err.Error() != model.RecordNotFound {
			errlog.Println(err)
			httpRes.ErrRes(c, httpRes.Mysql_Query, "数据库查询错误！")
			return
		}
	}
	if invtPool.ID == 0 {
		if err := db.Raw("select ipr.invt_pool_id id, coalesce(u.mobile,u.email_addr) acct from invt_pool_relts ipr inner join users u on u.id = ipr.be_invted_pool_id inner join pool_rigs pr on pr.pool_id = ipr.be_invted_pool_id where pr.rig_id = ?", rwdsRelID).Scan(invtPool).Error; err != nil && err.Error() != model.RecordNotFound {
			errlog.Println(err)
			httpRes.ErrRes(c, httpRes.Mysql_Query, "数据库查询错误！")
			return
		}
	}
	if err := db.Raw("select son.ori_id id, coalesce(u.mobile,u.email_addr) acct from pool_rigs pr inner join node_pools np on pr.pool_id = np.pool_id inner join ssu_ori_new_nodes son on son.new_id = np.node_id inner join users u on u.id = son.new_id where pr.rig_id = ?", rwdsRelID).Scan(invtNode).Error; err != nil && err.Error() != model.RecordNotFound {
		errlog.Println(err)
		httpRes.ErrRes(c, httpRes.Mysql_Query, "数据库查询错误！")
		return
	}
	if invtPool.ID != 0 {
		invtPool.Acct = utils.HideAcct(invtPool.Acct)
	}
	if invtNode.ID != 0 {
		invtNode.Acct = utils.HideAcct(invtNode.Acct)
	}

	var sql1, sql_upd_to_user string
	if toUser != nil {
		if params.Asset == "hst" {
			sql1 = fmt.Sprintf("update users set hsts = hsts - '%s' where id = %d and hsts >= accu_dig_hsts + %s", params.Amount, user.ID, params.Amount)
		} else {
			sql1 = fmt.Sprintf("update users set hst0s = hst0s - '%s' where id = %d and hst0s >= '%s'", params.Amount, user.ID, params.Amount)
		}
		sql_upd_to_user = fmt.Sprintf("update users set cmpt_pow = cmpt_pow + '%s' where id = %d", cmptPows, rwdsRelID)
	} else {
		if params.Asset == "hst" {
			sql1 = fmt.Sprintf("update users set hsts = hsts - '%s', cmpt_pow = cmpt_pow + '%s' where id = %d and hsts >= accu_dig_hsts + '%s'", params.Amount, cmptPows, user.ID, params.Amount)
		} else {
			sql1 = fmt.Sprintf("update users set hst0s = hst0s - '%s', cmpt_pow = cmpt_pow + '%s' where id = %d and hst0s >= '%s'", params.Amount, cmptPows, user.ID, params.Amount)
		}
	}
	sql2 := "update pool_rigs pr inner join node_pools np on pr.pool_id = np.pool_id set pr.cmpt_pow = pr.cmpt_pow + ?, np.sum_cmpt_pow = np.sum_cmpt_pow + ? where pr.rig_id = ?"
	// sql3 := "update users u1 inner join users u2 on u2.invt_id = u1.id set u1.cmpt_pow = u1.cmpt_pow + ? where u2.id = ?"
	sql3 := "update users set cmpt_pow = cmpt_pow + ?, accu_invt_pool_rwds = accu_invt_pool_rwds + ? where id = ?"
	sql5 := "update users set cmpt_pow = cmpt_pow + ?, accu_invt_node_rwds = accu_invt_node_rwds + ? where id = ?"
	pid := strings.ReplaceAll(uuid.Must(uuid.NewV4(), nil).String(), "-", "")
	pid = strings.ReplaceAll(pid, "-", "")
	var tr1, tr5 *model.TransRecord
	if toUser != nil {
		tr1 = &model.TransRecord{Typ: 9, Pid: pid, UserID: user.ID, Asset: params.Asset, Amount: amount}
		pid2 := strings.ReplaceAll(uuid.Must(uuid.NewV4(), nil).String(), "-", "")
		tr5 = &model.TransRecord{Typ: 9, Pid: pid2, UserID: rwdsRelID, Asset: params.Asset, ExchAmount: cmptPows}
		if params.Asset == "hst" {
			tr1.Remark = fmt.Sprintf("消耗%sHST帮用户%s兑换%sT算力，%s****%s", amount, *params.ToAcct, cmptPows.Round(4), pid2[:3], pid2[28:])
			tr5.Remark = fmt.Sprintf("来自用户%s消耗%sHST兑换的%sT算力，%s****%s", user.Acct, amount, cmptPows.Round(4), pid[:3], pid[28:])
		} else {
			tr1.Remark = fmt.Sprintf("消耗%sHST0帮用户%s兑换%sT算力，%s****%s", amount, *params.ToAcct, cmptPows.Round(4), pid2[:3], pid2[28:])
			tr5.Remark = fmt.Sprintf("来自用户%s消耗%sHST0兑换的%sT算力，%s****%s", user.Acct, amount, cmptPows.Round(4), pid[:3], pid[28:])
		}
	} else {
		tr1 = &model.TransRecord{Typ: 9, Pid: pid, UserID: user.ID, Asset: params.Asset, Amount: amount, ExchAmount: cmptPows}
	}

	pid = strings.ReplaceAll(uuid.Must(uuid.NewV4(), nil).String(), "-", "")
	tr2 := &model.TransRecord{Typ: 6, Pid: pid, UserID: invter.ID, Asset: "cmpt_pow", Amount: cmptPows.Mul(invterRwdsRate), Remark: "矿工：" + rwdsRelAcct}
	pid = strings.ReplaceAll(uuid.Must(uuid.NewV4(), nil).String(), "-", "")
	tr3 := &model.TransRecord{Typ: 11, Pid: pid, UserID: invtPool.ID, Asset: "cmpt_pow", Amount: cmptPows.Mul(invtPoolRwdsRate), Remark: "推荐矿池主：" + invtPool.Acct}
	pid = strings.ReplaceAll(uuid.Must(uuid.NewV4(), nil).String(), "-", "")
	tr4 := &model.TransRecord{Typ: 14, Pid: pid, UserID: invtNode.ID, Asset: "cmpt_pow", Amount: cmptPows.Mul(invtNodeRwdsRate), Remark: "推荐节点：" + invtNode.Acct}

	tx := db.Begin()
	if tx = tx.Exec(sql1); tx.Error != nil || tx.RowsAffected != 1 {
		errlog.Println(tx.Error)
		tx.Rollback()
		if tx.Error != nil {
			httpRes.ErrRes(c, httpRes.Mysql_Upd, "数据库更新错误！")
		} else {
			addt := map[string]interface{}{"$asset": strings.ToUpper(params.Asset)}
			httpRes.ErrAddtRes(c, httpRes.Exch_Cp_Insufficient_Balance, strings.ToUpper(params.Asset)+"余额不足！", addt)
		}
		return
	}
	if sql_upd_to_user != "" {
		if tx = tx.Exec(sql_upd_to_user); tx.Error != nil || tx.RowsAffected != 1 {
			errlog.Println(tx.Error)
			tx.Rollback()
			httpRes.ErrRes(c, httpRes.Mysql_Upd, "数据库更新错误！")
			return
		}
	}
	if err := tx.Exec(sql2, cmptPows, cmptPows, rwdsRelID).Error; err != nil {
		errlog.Println(err)
		tx.Rollback()
		httpRes.ErrRes(c, httpRes.Mysql_Upd, "数据库更新错误！")
		return
	}
	if err := tx.Create(tr1).Error; err != nil {
		errlog.Println(err)
		tx.Rollback()
		httpRes.ErrRes(c, httpRes.Mysql_Create, "数据库创建交易记录错误！")
		return
	}
	if toUser != nil {
		if err := tx.Create(tr5).Error; err != nil {
			errlog.Println(err)
			tx.Rollback()
			httpRes.ErrRes(c, httpRes.Mysql_Create, "数据库创建交易记录错误！")
			return
		}
	}

	if invter.ID != 0 {
		if err := tx.Exec(sql3, cmptPows.Mul(invterRwdsRate), 0, invter.ID).Error; err != nil {
			errlog.Println(err)
			tx.Rollback()
			httpRes.ErrRes(c, httpRes.Mysql_Upd, "数据库更新错误！")
			return
		}
		if err := tx.Exec(sql2, cmptPows.Mul(invterRwdsRate), cmptPows.Mul(invterRwdsRate), invter.ID).Error; err != nil {
			errlog.Println(err)
			tx.Rollback()
			httpRes.ErrRes(c, httpRes.Mysql_Upd, "数据库更新错误！")
			return
		}
		if err := tx.Create(tr2).Error; err != nil {
			errlog.Println(err)
			tx.Rollback()
			httpRes.ErrRes(c, httpRes.Mysql_Create, "数据库创建交易记录错误！")
			return
		}
	}
	if invtPool.ID != 0 {
		if err := tx.Exec(sql3, cmptPows.Mul(invtPoolRwdsRate), cmptPows.Mul(invtPoolRwdsRate), invtPool.ID).Error; err != nil {
			errlog.Println(err)
			tx.Rollback()
			httpRes.ErrRes(c, httpRes.Mysql_Upd, "数据库更新错误！")
			return
		}
		if err := tx.Exec(sql2, cmptPows.Mul(invtPoolRwdsRate), cmptPows.Mul(invtPoolRwdsRate), invtPool.ID).Error; err != nil {
			errlog.Println(err)
			tx.Rollback()
			httpRes.ErrRes(c, httpRes.Mysql_Upd, "数据库更新错误！")
			return
		}
		if err := tx.Create(tr3).Error; err != nil {
			errlog.Println(err)
			tx.Rollback()
			httpRes.ErrRes(c, httpRes.Mysql_Create, "数据库创建交易记录错误！")
			return
		}
	}
	if invtNode.ID != 0 {
		if err := tx.Exec(sql5, cmptPows.Mul(invtNodeRwdsRate), cmptPows.Mul(invtNodeRwdsRate), invtNode.ID).Error; err != nil {
			errlog.Println(err)
			tx.Rollback()
			httpRes.ErrRes(c, httpRes.Mysql_Upd, "数据库更新错误！")
			return
		}
		if err := tx.Exec(sql2, cmptPows.Mul(invtNodeRwdsRate), cmptPows.Mul(invtNodeRwdsRate), invtNode.ID).Error; err != nil {
			errlog.Println(err)
			tx.Rollback()
			httpRes.ErrRes(c, httpRes.Mysql_Upd, "数据库更新错误！")
			return
		}
		if err := tx.Create(tr4).Error; err != nil {
			errlog.Println(err)
			tx.Rollback()
			httpRes.ErrRes(c, httpRes.Mysql_Create, "数据库创建交易记录错误！")
			return
		}
	}
	tx.Commit()
}

var db *gorm.DB

// 连接数据库，初始化
func InitMysql() (*gorm.DB, error) {

	logMode := true
	maxLifetime := 14400
	maxIdleConns := 10
	maxOpenConns := 100

	url := "root:rich_hst_777@(192.168.182.131)/h5?charset=utf8&parseTime=true&loc=Local&multiStatements=true"
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
