# -
В данной курсовой работе, я собирала данные о балансах студентов за каждый прожитый ими день. Так же sql запрос содержит информацию о количестве, которое менялось под влиянием транзакций (оплат, начислений, корректирующих списаний) и уроков (списаний с баланса по мере прохождения уроков). 

Курсовая:
-- шаг1

with first_payments as(
      select user_id
      , date_trunc('day', min(transaction_datetime)) first_payment_date
      from skyeng_db.payments
where status_name = 'success'
group by 1
),

--шаг 2

all_dates as (
select distinct date_trunc('day',classes.class_start_datetime) as dt
    from skyeng_db.classes
where class_start_datetime < '2017-01-01'
),

--шаг 3

all_dates_by_user as (
select user_id, dt
    from all_dates
join first_payments
  on all_dates.dt >= first_payments.first_payment_date
), 

--шаг 4

payments_by_dates as (
      select user_id
      , date_trunc('day', transaction_datetime) as payment_date
      , sum(classes) transaction_balance_change
from skyeng_db.payments
where status_name = 'success'
group by 1,2
),

--шаг 5

payments_by_dates_cumsum as (
      select all_dates_by_user.user_id, transaction_balance_change, dt, SUM(COALESCE(transaction_balance_change,0)) OVER (PARTITION BY all_dates_by_user.user_id ORDER BY all_dates_by_user.dt) transaction_balance_change_cs
from all_dates_by_user
      left join payments_by_dates 
      on payments_by_dates.payment_date = all_dates_by_user.dt and all_dates_by_user.user_id = payments_by_dates.user_id
      ),

--шаг 6

classes_by_dates as (
 select user_id
     , date_trunc('day',class_start_datetime) class_date
     , count(id_class) * -1 as classes
     from skyeng_db.classes
where class_type != 'trial'
group by user_id, class_date
),

-- шаг 7

classes_by_dates_dates_cumsum as(
      select all_dates_by_user.user_id, all_dates_by_user.dt, classes,
SUM(COALESCE(classes,0)) OVER (PARTITION BY all_dates_by_user.user_id ORDER BY all_dates_by_user.dt) classes_cs
     from all_dates_by_user
     left join classes_by_dates
on classes_by_dates.class_date = all_dates_by_user.dt and all_dates_by_user.user_id = classes_by_dates.user_id
),

--шаш 8

balances as (
     select c.user_id, c.dt, transaction_balance_change, transaction_balance_change_cs, classes, classes_cs, (classes_cs + transaction_balance_change_cs) balance
     from classes_by_dates_dates_cumsum c
     join payments_by_dates_cumsum p
     on c.dt = p.dt and c.user_id = p.user_id and c.user_id = p.user_id
)
