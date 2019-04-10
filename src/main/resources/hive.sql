-- 1,行转列:explode(list)
-- 2,抽样select * from bucket_users tablesample(bucket 1 out of 4 on user_id);
-- 3,列转行sum(case order_dow when '0' then 1 else 0 end) as dow_0,
-- 4,over开窗函数
--    1>生成排序的序列号row_number() over(partition by user_id order by prod_buy_cnt desc) as row_num,
--    2>聚类函数 count(1) over(partition by user_id)
-- 5,collect_list,collect_set,(select user_id,collect_list(order_id) as order_ids)




--将【一句话一行】 变成 【一个单词一行】
select explode(split(sentence,' ')) as word from article

--reduce 对单词做统计
select word,count(1) as cnt
from(
select explode(split(sentence,' ')) as word from article
) t
group by word
limit 100;


--爆款商品有哪些?
--top N 出现次数最多的N单词
select word,count(1) as cnt
from(
select explode(split(sentence,' ')) as word from article
) t
group by word
order by cnt desc
limit 100

--分区，partition，hive表名/子文件夹
根据日期做partition，每天一个partition，每天的数据会存到一个文件夹里面。相当于将数据按日期划分
如果只想要查询昨天的数据，只需用对应查询昨天日期的文件夹下的数据

--分桶bucket
10bucket 把数据划分10份 1/10 只需要拿一份

--内部表
--本地导入数据到hive中:   [建表，只建立元数据信息+hdfs hive目录下给一个表名文件夹，里面没有数据]
create table article(sentence string)
row format delimited fields terminated by '\n';

--从本地导入数据：相当于将path数据 hadoop fs -put /hive/warehouse/badou.db/article
load data local inpath '/mnt/hgfs/code/mr/mr_wc/The_Man_of_Property.txt'
into table article;


--外部表
create external table art1(sentence string)
row format delimited fields terminated by '\n'
stored as textfile
location '/tmp_data';

--在hive/warehouse/badou.db下没有art1这个文件夹
--但是能表art1查询到数据
select * from art1 limit 3;

--外部数据源 /data/ext/The_Man_of_Property.txt 数据未发生变化
--删除表：
drop table art1;
--发现，数据元信息被删除了，但是在hdfs路径/data/ext下的数据还存在。


--partition建表
create table art_dt(sentence string)
partitioned by(dt string)
row format delimited fields terminated by '\n';
--从hive表中的数据插入到新表（分区表）中
insert overwrite table art_dt partition(dt='20180821')
select * from article limit 100;
insert overwrite table art_dt partition(dt='20180822')
select * from article limit 100;

--查看当前表中的partitions：
show partitions art_dt;

select * from art_dt where dt between '20180821' and '20180822' limit 10

--partition 实际工作中怎么产生，用在什么数据上？

--1. 每天都会产生用户浏览，点击，收藏，购买的记录。
--按照天的方式去存储数据，按天做partition

--2. app,m,pc  logs/dt=20180922/type=app
--             logs/dt=20180922/type=m
--             logs/dt=20180922/type=pc

-- 数据库 用户的属性，年龄，性别，blog
-- 每天有新增用户，修改信息 dt=20180922 dt=20180923 大量信息冗余（重复）了

-- overwrite 7  每天做overwrite dt = 20180922，这天中的数据包含这天之前的所有用户信息
-- 当天之前所有的全量数据。 存7个分区，冗余七份，防止丢失数据。

--udata
create table udata(
user_id string,
item_id string,
rating string,
`timestamp` string
)row format delimited fields terminated by '\t';

load data local inpath '/mnt/hgfs/data/ml-100k/u.data'
into table udata;

--hive查询时显示表头
set hive.cli.print.header=true;

 --bucket
 set hive.enforce.bucketing = true; --number of mappers: 1; number of reducers: 4
 create table bucket_users(user_id int,
item_id string,
rating string,
`timestamp` string)
clustered by(user_id)
into 4 buckets;

insert overwrite table bucket_users
select
cast(user_id as int) as user_id,
item_id,
rating,
`timestamp`
from udata;

--sample 2,3572 from 10,0000
select * from bucket_users tablesample(bucket 1 out of 4 on user_id);


-- 采样数据，放入新创建表
 create table tmp1
 as select *
 from bucket_users
 tablesample(bucket 1 out of 4 on user_id);
 --1/2 bucket 只有map，user_id%2=1

 --采样90%
 select * from udata where user_id%10>0 limit 10;

 --order_products 订单和产品的信息
 order_id,product_id,add_to_cart_order,reordered
 create table order_products_prior(
 order_id string,
 product_id string,
 add_to_cart_order string,
 reordered string)
 row format delimited fields terminated by ',';
 load data local inpath '/mnt/hgfs/data/order_products__prior.csv'
into table order_products_prior;

--orders 订单表：订单和用户的信息  dow=day of week;order_number购买订单的先后顺序
order_id,user_id,eval_set,order_number,order_dow,order_hour_of_day,days_since_prior_order
create table orders(
order_id string,
user_id string,
eval_set string,
order_number string,
order_dow string,
order_hour_of_day string,
days_since_prior_order string
)row format delimited fields terminated by ',';
load data local inpath '/mnt/hgfs/data/orders.csv'
into table orders;

-- 统计每个用户购买过多少个商品
--1. 每个用户的每个订单的商品数量 【订单中商品数量】
select order_id,count(1) as prod_cnt
from order_products_prior
group by order_id
order by prod_cnt desc
limit 10;

--2.将每个订单的商品数量带给user  join
order_id prod_cnt table1
order_id user_id  table2 orders

-- 这个用户在这个订单中购买多少商品prod_cnt
order_id user_id prod_cnt

select
t2.order_id as order_id,
t2.user_id as user_id,
t1.prod_cnt as prod_cnt
from orders t2
join
(select order_id,count(1) as prod_cnt
from order_products_prior
group by order_id)t1
on t2.order_id=t1.order_id
limit 10

--3. 这个用户user所有订单的商品总和
select
user_id,
sum(prod_cnt) as sum_prod_cnt
from(
select
t2.order_id as order_id,
t2.user_id as user_id,
t1.prod_cnt as prod_cnt
from orders t2
join(
select order_id,count(1) as prod_cnt
from order_products_prior
group by order_id
)t1
on t2.order_id=t1.order_id
)t12
group by user_id
order by sum_prod_cnt desc
limit 10;

--reudce调优
select order_id,count(1) as prod_cnt
from order_products_prior
group by order_id
limit 10
--number of mappers: 3; number of reducers: 3

--控制一个job会有多少个reducer来处理，依据多少的是文件的总大小，默认1g
set hive.exec.reducers.bytes.per.reducer=<number>
set hive.exec.reducers.bytes.per.reducer=20000
--number of mappers: 3; number of reducers: 1009

set hive.exec.reducers.max=<number>
set hive.exec.reducers.max=3;
-- number of mappers: 3; number of reducers: 3
set mapreduce.job.reduces=<number>
set mapreduce.job.reduces=5;
--number of mappers: 3; number of reducers: 5

--颠倒顺序
set mapreduce.job.reduces=5;
set hive.exec.reducers.max=3;
--number of mappers: 3; number of reducers: 5
--表明优先级是全局的，和顺序无关，只在我当前cli生效
--关闭窗口，回复默认

-- 作业：
-- 1. 将orders和order_products_prior建表入hive   head -1000|order_prods.txt
-- 2. 每个用户有多少个订单
-- 3. 每个用户平均每个订单是多少商品
-- 4. 每个用户在一周中的购买订单的分布 --列转行
-- 1 dow=2,1, dow=3,2 ,dow=4,5
--
-- user_id, dow_0,dow_1,dow_2,dow_3,dow_4,dow_5,dow_6
-- 1            0    0    1     2      5    0    0
--
-- 5. 平均每个购买天中，购买的商品数量 【把days_since_prior_order当做一个月中购买的天】
-- day1 : 2个订单： 1.订单：10个product，2. 15个product 共25个
-- day2：一个订单，12个product
-- day3：没有购买
--
-- （25+12） / 2
--
-- 6. 每个用户最喜爱购买的三个product是什么，最最终表结构可以是3个列，或者一个字符串
-- user_id "prod_top1,prod_top2,prod_top3"
----------------------------------------------------------------
----------------------------------------------------------------
--day04
----------------------------------------------------------------
----------------------------------------------------------------
--1. 将orders和order_products_prior建表入hive
--sed '1d' orders.csv 能把列名删除
create table orders
(
order_id string,
user_id string,
eval_set string,
order_number string,
order_dow string,
order_hour_of_day string,
days_since_prior_order string
)
row format delimited fields terminated by ',' --'\t'
lines terminated by '\n';
--location '/data/orders' --(orders是文件夹)

--导入数据
load local data inpath '/home/badou/Documents/data/order_data/orders.csv'
overwrite into table orders;

order_id                string
product_id              string
add_to_cart_order       string
reordered               string

--2.每个用户有多少个订单[orders]  pv浏览量 > uv用户量
--order_id,user_id
select user_id,count(order_id) as order_cnt
from orders
group by user_id
order by order_cnt desc
limit 10

--3.每个用户【2.平均【1.每个订单是多少商品】】 avg
-- orders[用户，订单]   order_products_prior【订单，商品】order_id product_id
--3.1.每个订单是多少商品
select order_id,count(product_id) as prod_cnt
from order_products_prior
group by order_id

--3.2
set hive.cli.print.header=true;
select orders.user_id,prod.prod_cnt
from orders
join(
select order_id,count(product_id) as prod_cnt
from order_products_prior
group by order_id)prod
on orders.order_id=prod.order_id
limit 10;


select user_id, avg(prod_cnt) as avg_prod_cnt
from
(
select orders.user_id,prod.prod_cnt
from orders
join(
select order_id,count(product_id) as prod_cnt
from order_products_prior
group by order_id)prod
on orders.order_id=prod.order_id
)t
group by user_id
order by avg_prod_cnt desc
limit 100


select orders.user_id,avg(prod.prod_cnt) as avg_prod_cnt
from orders
join(
select order_id,count(product_id) as prod_cnt
from order_products_prior
group by order_id)prod
on orders.order_id=prod.order_id
group by orders.user_id
limit 10;

--4. 每个用户在一周中的购买订单的分布 --列转行
set hive.cli.print.header=true;
select
user_id,
sum(case order_dow when '0' then 1 else 0 end) as dow_0,
sum(case order_dow when '1' then 1 else 0 end) as dow_1,
sum(case order_dow when '2' then 1 else 0 end) as dow_2,
sum(case order_dow when '3' then 1 else 0 end) as dow_3,
sum(case order_dow when '4' then 1 else 0 end) as dow_4,
sum(case order_dow when '5' then 1 else 0 end) as dow_5,
sum(case order_dow when '6' then 1 else 0 end) as dow_6
from orders
group by user_id
limit 20;

--5. 每个用户平均每个购买天中，购买的商品数量
--【把days_since_prior_order当做一个日期20181014】
--1.orders user_id days_since_prior_order 天数
select if(days_since_prior_order='','0',days_since_prior_order) as days_since_prior_order

select user_id,count(distinct days_since_prior_order) as day_cnt
from orders
group by user_id

--2.每个订单是多少商品
select order_id,count(product_id) as prod_cnt
from order_products_prior
group by order_id

1.user_id prod_cnt day_cnt  join orders order_products_prior
2.user_id prod_cnt dt(days_since_prior_order) join

--1.
select orders.user_id,sum(prod.prod_cnt)/count(distinct days_since_prior_order) as day_cnt
from orders
join
(select order_id,count(product_id) as prod_cnt
from order_products_prior
group by order_id)prod
on orders.order_id=prod.order_id
group by orders.user_id
limit 100

--2.
select user_id,avg(prod_cnt) as avg_day_prod_cnt
from(
select orders.user_id,orders.days_since_prior_order,sum(prod.prod_cnt) as prod_cnt
from orders
join
(select order_id,count(product_id) as prod_cnt
from order_products_prior
group by order_id)prod
on orders.order_id=prod.order_id
group by orders.user_id,orders.days_since_prior_order
)t
group by user_id
limit 100


-- 6. 每个用户最喜爱购买的三个product是什么，最终表结构可以是3个列，或者一个字符串
-- user_id,product_id,prod_buy_cnt 一个用户对同一个商品购买多少次

select user_id,collect_list(concat_ws('_',product_id,cast(row_num as string))) as top_3_prods
from(
select user_id,product_id,
row_number() over(partition by user_id order by prod_buy_cnt desc) as row_num,
prod_buy_cnt
from(
select orders.user_id,pri.product_id,count(1) as prod_buy_cnt
from orders
join order_products_prior pri
on orders.order_id=pri.order_id
group by orders.user_id,pri.product_id
)t
)tt
where row_num<=3
group by user_id
limit 10;


--concat_ws
select user_id,collect_list(order_id) as order_ids
from orders
group by user_id
limit 10;


1.combiner
2.set hive.groupby.skewindata=true;
一个map reduce拆成两个MR


--一个 MR job
select * from orders
join order_products_prior pri on orders.order_id=pri.order_id
join tmp_order tord on orders.order_id=tord.order_id

--一. top10%
select user_id,ceil(cast(count(distinct pri.product_id) as double)*0.1) as total_prod_cnt
from orders join order_products_prior pri
on orders.order_id=pri.order_id
group by user_id
limit 10;


select user_id,collect_list(concat_ws('_',product_id,
cast(row_num as string),cast(total_prod_cnt as string))) as top_10_perc_prod
from
(select user_id,product_id,
row_number() over(distribute by user_id sort by usr_prod_cnt desc) as row_num,
ceil(cast((count(1) over(partition by user_id)) as double)*0.1) as total_prod_cnt
from(
select user_id,product_id,
count(1) as usr_prod_cnt
from orders join (
select * from order_products_prior limit 3000
) pri
on orders.order_id=pri.order_id
group by user_id,product_id
)t
)t1
where row_num<=total_prod_cnt
group by user_id
limit 10;


--分区表
--1.表的元数据（表结构）
create table order_part(
order_id string,
user_id string,
eval_set string,
order_number string,
order_hour_of_day string,
days_since_prior_order string
)partitioned by(order_dow string)
row format delimited fields terminated by '\t';

--2.动态插入分区表
set hive.exec.dynamic.partition=true; --使用动态分区
set hive.exec.dynamic.partition.mode=nonstrict;--无限制模式

insert overwrite table order_part partition(order_dow) --(dt='20181028')
select order_id,user_id,eval_set,order_number,order_hour_of_day,days_since_prior_order,order_dow
from orders --where order_dow='2'


--统计product_id,pv，uv，reordered数
--1.pv
select product_id,count(1) as pv
from order_products_prior
group by product_id

--2.uv
select product_id,count(distinct user_id) as uv
from orders join order_products_prior pri
on orders.order_id=pri.order_id
group by pri.product_id

--3.reordered数
select product_id,sum(cast(reordered as int)) as reordered_cnt
from order_products_prior
group by product_id


--1和3合并
select product_id,
count(1) as pv,
sum(cast(reordered as int)) as reordered_cnt
from order_products_prior
group by product_id

--1,3直接放入2中
select product_id,
count(1) as pv,
count(distinct user_id) as uv,
sum(reordered) as reordered_cnt
from orders join order_products_prior pri
on orders.order_id=pri.order_id
group by pri.product_id


select t1.product_id,t1.pv,t1.reordered_cnt,t2.uv
from
(select product_id,
count(1) as pv,
sum(cast(reordered as int)) as reordered_cnt
from order_products_prior
group by product_id)t1
join
(
select product_id,count(distinct user_id) as uv
from orders join order_products_prior pri
on orders.order_id=pri.order_id
group by pri.product_id
)t2
on t1.product_id=t2.product_id



--jieba udf
add file /home/badou/Documents/code/python/udf/jieba_udf.py;
select transform(sentence) using 'python jieba_udf.py' as seg
from news_noseg
limit 10;

--去重product集合
select user_id,concat_ws(',',collect_set(product_id))
from orders join
order_products_prior pri
on orders.order_id=pri.order_id
group by user_id
limit 10;



















