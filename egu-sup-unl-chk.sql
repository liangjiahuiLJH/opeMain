-- select sum(amount) from `eco_gov_upg_unlock_records` where created >= '2021-03-22 19:50:00'

-- sql1 矿机、邀请、矿池、节点页面显示统计值
select
sum(round(u1.hsc_acc_info->'$.per.unl_td'-u2.hsc_acc_info->'$.per.unl_td',4)) per_unl_td,
sum(round(u1.hsc_acc_info->'$.invter.unl_td'-u2.hsc_acc_info->'$.invter.unl_td',4)) invter_unl_td,
sum(round(u1.hsc_acc_info->'$.pool.unl_td'-u2.hsc_acc_info->'$.pool.unl_td',4)) pool_unl_td,
sum(round(u1.hsc_acc_info->'$.node.unl_td'-u2.hsc_acc_info->'$.node.unl_td',4)) node_unl_td,
sum(round(u1.hsc_acc_info->'$.per.unl_tt'-u2.hsc_acc_info->'$.per.unl_tt',4)) per_unl_tt,
sum(round(u1.hsc_acc_info->'$.invter.acc_td'-u2.hsc_acc_info->'$.invter.acc_td',4)) invter_acc_td,
sum(round(u1.hsc_acc_info->'$.pool.acc_td'-u2.hsc_acc_info->'$.pool.acc_td',4)) pool_acc_td,
sum(round(u1.hsc_acc_info->'$.node.acc_td'-u2.hsc_acc_info->'$.node.acc_td',4)) node_acc_td,
sum(round(u1.hsc_acc_info->'$.invter.acc_tt'-u2.hsc_acc_info->'$.invter.acc_tt',4)) invter_acc_tt,
sum(round(u1.hsc_acc_info->'$.pool.acc_tt'-u2.hsc_acc_info->'$.pool.acc_tt',4)) pool_acc_tt,
sum(round(u1.hsc_acc_info->'$.node.acc_tt'-u2.hsc_acc_info->'$.node.acc_tt',4)) node_acc_tt
from users u1 inner join `users-egu-fix-bug` u2 on u1.id = u2.id;

-- sql2 矿机、邀请、矿池、节点页面显示具体值
select u1.id,
round(u1.hsc_acc_info->'$.per.unl_td'-u2.hsc_acc_info->'$.per.unl_td',4) per_unl_td,
round(u1.hsc_acc_info->'$.invter.unl_td'-u2.hsc_acc_info->'$.invter.unl_td',4) invter_unl_td,
round(u1.hsc_acc_info->'$.pool.unl_td'-u2.hsc_acc_info->'$.pool.unl_td',4) pool_unl_td,
round(u1.hsc_acc_info->'$.node.unl_td'-u2.hsc_acc_info->'$.node.unl_td',4) node_unl_td,
round(u1.hsc_acc_info->'$.per.unl_tt'-u2.hsc_acc_info->'$.per.unl_tt',4) per_unl_tt,
round(u1.hsc_acc_info->'$.invter.acc_td'-u2.hsc_acc_info->'$.invter.acc_td',4) invter_acc_td,
round(u1.hsc_acc_info->'$.pool.acc_td'-u2.hsc_acc_info->'$.pool.acc_td',4) pool_acc_td,
round(u1.hsc_acc_info->'$.node.acc_td'-u2.hsc_acc_info->'$.node.acc_td',4) node_acc_td,
round(u1.hsc_acc_info->'$.invter.acc_tt'-u2.hsc_acc_info->'$.invter.acc_tt',4) invter_acc_tt,
round(u1.hsc_acc_info->'$.pool.acc_tt'-u2.hsc_acc_info->'$.pool.acc_tt',4) pool_acc_tt,
round(u1.hsc_acc_info->'$.node.acc_tt'-u2.hsc_acc_info->'$.node.acc_tt',4) node_acc_tt
from users u1 inner join `users-egu-fix-bug` u2 on u1.id = u2.id;

-- sql3 是否按期望值补发
select u.id, round(u.hsc_acc_info->'$.per.unl_td',4) unl_td, rls.amount, round(u.hsc_acc_info->'$.per.unl_tt',4) from users u inner join fixbug.release20200322 rls on rls.user_id = u.id where round(u.hsc_acc_info->'$.per.unl_td' - 0.0001,4) <> rls.amount and round(u.hsc_acc_info->'$.per.unl_td',4) <> rls.amount;

-- sql4、5、6 按日期分析解锁统计值
select date_format(created,'%Y-%m-%d'), sum(amount) from `eco_gov_upg_unlock_records` group by date_format(created,'%Y-%m-%d'); -- 执行后解锁统计
select round(sum(`lock`*0.001),4) + round(sum(`lock`*0.001)*(0.15+0.03+0.02),4) from eco_gov_upg_lock_statuses where lock_date = '2021-03-21'; -- 21号锁仓释放
select date_format(created,'%Y-%m-%d'), sum(amount) from `eco_gov_upg_unlock_records-egu-fix-bug` group by date_format(created,'%Y-%m-%d'); -- 未执行前解锁统计


select sum(u2.hsc_lock-u1.hsc_lock) from users u1 inner join `users-egu-fix-bug` u2 on u1.id = u2.id;


