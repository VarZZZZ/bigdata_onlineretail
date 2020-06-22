create external table ods_start_log  // 无用；
(`line` string)
partitioned by (`dt` string)
stored as 
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/gmall/ods/ods_start_logs';




create external table ods_start_log
(`line` string)
partitioned by (`dt` string)
stored as 
sequenceFile
LOCATION '/warehouse/gmall/ods/ods_start_logs';

drop table if exists ods_event_log;
create external table ods_event_log
(`line` string)
partitioned by (`dt` string)
stored as 
sequenceFile
LOCATION '/warehouse/gmall/ods/ods_event_logs';


drop table if exists ods_order_info;
create external table ods_order_info (
`id` string COMMENT '订单号',
`final_total_amount` decimal(10,2) COMMENT '订单金额',
`order_status` string COMMENT '订单状态',
`user_id` string COMMENT '用户id',
`out_trade_no` string COMMENT '支付流水号',
`create_time` string COMMENT '创建时间',
`operate_time` string COMMENT '操作时间',
`province_id` string COMMENT '省份ID',
`benefit_reduce_amount` decimal(10,2) COMMENT '优惠金额',
`original_total_amount` decimal(10,2) COMMENT '原价金额',
`feight_fee` decimal(10,2) COMMENT '运费'
) COMMENT '订单表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
stored as 
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_order_info/';

drop table if exists ods_order_detail;
create external table ods_order_detail(
`id` string COMMENT '订单编号',
`order_id` string COMMENT '订单号',
`user_id` string COMMENT '用户id',
`sku_id` string COMMENT '商品id',
`sku_name` string COMMENT '商品名称',
`order_price` decimal(10,2) COMMENT '商品价格',
`sku_num` bigint COMMENT '商品数量',
`create_time` string COMMENT '创建时间'
) COMMENT '订单详情表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_order_detail/';


drop table if exists ods_sku_info;
create external table ods_sku_info(
`id` string COMMENT 'skuId',
`spu_id` string COMMENT 'spuid',
`price` decimal(10,2) COMMENT '价格',
`sku_name` string COMMENT '商品名称',
`sku_desc` string COMMENT '商品描述',
`weight` string COMMENT '重量',
`tm_id` string COMMENT '品牌id',
`category3_id` string COMMENT '品类id',
`create_time` string COMMENT '创建时间'
) COMMENT 'SKU 商品表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_sku_info/';

drop table if exists ods_user_info;
create external table ods_user_info(
`id` string COMMENT '用户id',
`name` string COMMENT '姓名',
`birthday` string COMMENT '生日',
`gender` string COMMENT '性别',
`email` string COMMENT '邮箱',
`user_level` string COMMENT '用户等级',
`create_time` string COMMENT '创建时间',
`operate_time` string COMMENT '操作时间'
) COMMENT '用户表-增量及更新'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_user_info/';

drop table if exists ods_base_category1;
create external table ods_base_category1(
`id` string COMMENT 'id',
`name` string COMMENT '名称'
) COMMENT '商品一级分类表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_category1/';

drop table if exists ods_base_category2;
create external table ods_base_category2(
`id` string COMMENT ' id',
`name` string COMMENT '名称',
category1_id string COMMENT '一级品类id'
) COMMENT '商品二级分类表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_category2/';

drop table if exists ods_base_category3;
create external table ods_base_category3(
`id` string COMMENT ' id',
`name` string COMMENT '名称',
category2_id string COMMENT '二级品类id'
) COMMENT '商品三级分类表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_category3/';


drop table if exists ods_payment_info;
create external table ods_payment_info(
`id` bigint COMMENT '编号',
`out_trade_no` string COMMENT '对外业务编号',
`order_id` string COMMENT '订单编号',
`user_id` string COMMENT '用户编号',
`alipay_trade_no` string COMMENT '支付宝交易流水编号',
`total_amount` decimal(16,2) COMMENT '支付金额',
`subject` string COMMENT '交易内容',
`payment_type` string COMMENT '支付类型',
`payment_time` string COMMENT '支付时间'
) COMMENT '支付流水表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_payment_info/';

drop table if exists ods_base_province;
create external table ods_base_province (
`id` bigint COMMENT '编号',
`name` string COMMENT '省份名称',
`region_id` string COMMENT '地区ID',
`area_code` string COMMENT '地区编码',
`iso_code` string COMMENT 'iso 编码'
) COMMENT '省份表'
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_province/';

drop table if exists ods_base_region;
create external table ods_base_region (
`id` bigint COMMENT '编号',
`region_name` string COMMENT '地区名称'
) COMMENT '地区表'
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_region/';

drop table if exists ods_base_trademark;
create external table ods_base_trademark (
`tm_id` bigint COMMENT '编号',
`tm_name` string COMMENT '品牌名称'
) COMMENT '品牌表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_trademark/';

drop table if exists ods_order_status_log;
create external table ods_order_status_log (
`id` bigint COMMENT '编号',
`order_id` string COMMENT '订单ID',
`order_status` string COMMENT '订单状态',
`operate_time` string COMMENT '修改时间'
) COMMENT '订单状态表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_order_status_log/';

drop table if exists ods_spu_info;
create external table ods_spu_info(
`id` string COMMENT 'spuid',
`spu_name` string COMMENT 'spu 名称',
`category3_id` string COMMENT '品类id',
`tm_id` string COMMENT '品牌id'
) COMMENT 'SPU 商品表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_spu_info/';

drop table if exists ods_comment_info;
create external table ods_comment_info(
`id` string COMMENT '编号',
`user_id` string COMMENT '用户ID',
`sku_id` string COMMENT '商品sku',
`spu_id` string COMMENT '商品spu',
`order_id` string COMMENT '订单ID',
`appraise` string COMMENT '评价',
`create_time` string COMMENT '评价时间'
) COMMENT '商品评论表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_comment_info/';

drop table if exists ods_order_refund_info;
create external table ods_order_refund_info(
`id` string COMMENT '编号',
`user_id` string COMMENT '用户ID',
`order_id` string COMMENT '订单ID',
`sku_id` string COMMENT '商品ID',
`refund_type` string COMMENT '退款类型',
`refund_num` bigint COMMENT '退款件数',
`refund_amount` decimal(16,2) COMMENT '退款金额',
`refund_reason_type` string COMMENT '退款原因类型',
`create_time` string COMMENT '退款时间'
) COMMENT '退单表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_order_refund_info/';

drop table if exists ods_cart_info;
create external table ods_cart_info(
`id` string COMMENT '编号',
`user_id` string COMMENT '用户id',
`sku_id` string COMMENT 'skuid',
`cart_price` string COMMENT '放入购物车时价格',
`sku_num` string COMMENT '数量',
`sku_name` string COMMENT 'sku 名称(冗余)',
`create_time` string COMMENT '创建时间',
`operate_time` string COMMENT '修改时间',
`is_ordered` string COMMENT '是否已经下单',
`order_time` string COMMENT '下单时间'
) COMMENT '加购表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_cart_info/';

drop table if exists ods_favor_info;
create external table ods_favor_info(
`id` string COMMENT '编号',
`user_id` string COMMENT '用户id',
`sku_id` string COMMENT 'skuid',
`spu_id` string COMMENT 'spuid',
`is_cancel` string COMMENT '是否取消',
`create_time` string COMMENT '收藏时间',
`cancel_time` string COMMENT '取消时间'
) COMMENT '商品收藏表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_favor_info/';

drop table if exists ods_coupon_use;
create external table ods_coupon_use(
`id` string COMMENT '编号',
`coupon_id` string COMMENT '优惠券ID',
`user_id` string COMMENT 'skuid',
`order_id` string COMMENT 'spuid',
`coupon_status` string COMMENT '优惠券状态',
`get_time` string COMMENT '领取时间',
`using_time` string COMMENT '使用时间(下单)',
`used_time` string COMMENT '使用时间(支付)'
) COMMENT '优惠券领用表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_coupon_use/';

drop table if exists ods_coupon_info;
create external table ods_coupon_info(
`id` string COMMENT '购物券编号',
`coupon_name` string COMMENT '购物券名称',
`coupon_type` string COMMENT '购物券类型1 现金券2 折扣券3 满减券4 满件打折券',
`condition_amount` string COMMENT '满额数',
`condition_num` string COMMENT '满件数',
`activity_id` string COMMENT '活动编号',
`benefit_amount` string COMMENT '减金额',
`benefit_discount` string COMMENT '折扣',
`create_time` string COMMENT '创建时间',
`range_type` string COMMENT '范围类型1、商品2、品类3、品牌',
`spu_id` string COMMENT '商品id',
`tm_id` string COMMENT '品牌id',
`category3_id` string COMMENT '品类id',
`limit_num` string COMMENT '最多领用次数',
`operate_time` string COMMENT '修改时间',
`expire_time` string COMMENT '过期时间'
) COMMENT '优惠券表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_coupon_info/';

drop table if exists ods_activity_info;
create external table ods_activity_info(
`id` string COMMENT '编号',
`activity_name` string COMMENT '活动名称',
`activity_type` string COMMENT '活动类型',
`start_time` string COMMENT '开始时间',
`end_time` string COMMENT '结束时间',
`create_time` string COMMENT '创建时间'
) COMMENT '活动表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_activity_info/';


drop table if exists ods_activity_order;
create external table ods_activity_order(
`id` string COMMENT '编号',
`activity_id` string COMMENT '优惠券ID',
`order_id` string COMMENT 'skuid',
`create_time` string COMMENT '领取时间'
) COMMENT '活动订单关联表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_activity_order/';

drop table if exists ods_activity_rule;
create external table ods_activity_rule(
`id` string COMMENT '编号',
`activity_id` string COMMENT '活动ID',
`condition_amount` string COMMENT '满减金额',
`condition_num` string COMMENT '满减件数',
`benefit_amount` string COMMENT '优惠金额',
`benefit_discount` string COMMENT '优惠折扣',
`benefit_level` string COMMENT '优惠级别'
) COMMENT '优惠规则表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_activity_rule/';

drop table if exists ods_base_dic;
create external table ods_base_dic(
`dic_code` string COMMENT '编号',
`dic_name` string COMMENT '编码名称',
`parent_code` string COMMENT '父编码',
`create_time` string COMMENT '创建日期',
`operate_time` string COMMENT '操作日期'
) COMMENT '编码字典表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_dic/';


load data inpath '/origin_data/gmall/log/topic_start/2020-06-20'
into table ods_start_log partition(dt='2020-06-20');
load data inpath '/origin_data/gmall/log/topic_event/2020-06-20'
into table ods_event_log partition(dt='2020-06-20');





drop table if exists dwd_start_log;
CREATE EXTERNAL TABLE dwd_start_log(
`mid_id` string,
`user_id` string,
`version_code` string,
`version_name` string,
`lang` string,
`source` string,
`os` string,
`area` string,
`model` string,
`brand` string,
`sdk_version` string,
`gmail` string,
`height_width` string,
`app_time` string,
`network` string,
`lng` string,
`lat` string,
`entry` string,
`open_ad_type` string,
`action` string,
`loading_time` string,
`detail` string,
`extend1` string
) 
PARTITIONED BY (dt string)
row format 
serde
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
stored as 
parquet
location '/warehouse/gmall/dwd/dwd_start_log/';

insert overwrite table dwd_start_log
partition(dt='2020-06-20')
select 
    get_json_object(line,'$.mid') mid_id,
    get_json_object(line,'$.uid') user_id,
    get_json_object(line,'$.vc') version_code,
    get_json_object(line,'$.vn') version_name,
    get_json_object(line,'$.l') lang,
    get_json_object(line,'$.sr') source,
    get_json_object(line,'$.os') os,
    get_json_object(line,'$.ar') area,
    get_json_object(line,'$.md') model,
    get_json_object(line,'$.ba') brand,
    get_json_object(line,'$.sv') sdk_version,
    get_json_object(line,'$.g') gmail,
    get_json_object(line,'$.hw') height_width,
    get_json_object(line,'$.t') app_time,
    get_json_object(line,'$.nw') network,
    get_json_object(line,'$.ln') lng,
    get_json_object(line,'$.la') lat,
    get_json_object(line,'$.entry') entry,
    get_json_object(line,'$.open_ad_type') open_ad_type,
    get_json_object(line,'$.action') action,
    get_json_object(line,'$.loading_time') loading_time,
    get_json_object(line,'$.detail') detail,
    get_json_object(line,'$.extend1') extend1
from ods_start_log
where dt='2020-06-20';


insert overwrite table dwd_base_event_log
partition(dt='2020-06-21')
select
    base_analizer(line,"mid") as  mid_id,
    base_analizer(line,'uid') as user_id,
    base_analizer(line,'vc') as version_code,
    base_analizer(line,'vn') as version_name,
    base_analizer(line,'l') as lang,
    base_analizer(line,'sr') as source,
    base_analizer(line,'os') as os,
    base_analizer(line,'ar') as area,
    base_analizer(line,'md') as model,
    base_analizer(line,'ba') as brand,
    base_analizer(line,'sv') as sdk_version,
    base_analizer(line,'g') as gmail,
    base_analizer(line,'hw') as height_width,
    base_analizer(line,'t') as app_time,
    base_analizer(line,'nw') as network,
    base_analizer(line,'ln') as lng,
    base_analizer(line,'la') as lat,
    event_name,
    event_json,
    base_analizer(line,"st") as server_time
from ods_event_log lateral view flat_analizer(base_analizer(line,"et")) tmp_flat
as event_name,event_json
where dt='2020-06-21' and base_analizer(line,"et")<>"";


create function base_analizer as 'com.onlineretail.udf.BaseFieldUDF' using jar 'hdfs://master:9000/user/hive/jars/hivefunction-1.0-SNAPSHOT.jar';

create function flat_analizer as 'com.onlineretail.udtf.EventJsonUDTF' using jar 'hdfs://master:9000/user/hive/jars/hivefunction-1.0-SNAPSHOT.jar';



drop table if exists dwd_display_log;
CREATE EXTERNAL TABLE dwd_display_log(
`mid_id` string,
`user_id` string,
`version_code` string,
`version_name` string,
`lang` string,
`source` string,
`os` string,
`area` string,
`model` string,
`brand` string,
`sdk_version` string,
`gmail` string,
`height_width` string,
`app_time` string,
`network` string,
`lng` string,
`lat` string,
`action` string,
`goodsid` string,
`place` string,
`extend1` string,
`category` string,
`server_time` string
) PARTITIONED BY (
dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_display_log/'


insert overwrite table dwd_display_log
partition(dt='2020-06-20')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.action') action,
    get_json_object(event_json,'$.kv.goodsid') goodsid,
    get_json_object(event_json,'$.kv.place') place,
    get_json_object(event_json,'$.kv.extend1') extend1,
    get_json_object(event_json,'$.kv.category') category,
    server_time
from dwd_base_event_log
where dt='2020-06-20' and event_name='display';


drop table if exists dwd_newsdetail_log;
CREATE EXTERNAL TABLE dwd_newsdetail_log(
`mid_id` string,
`user_id` string,
`version_code` string,
`version_name` string,
`lang` string,
`source` string,
`os` string,
`area` string,
`model` string,
`brand` string,
`sdk_version` string,
`gmail` string,
`height_width` string,
`app_time` string,
`network` string,
`lng` string,
`lat` string,
`entry` string,
`action` string,
`goodsid` string,
`showtype` string,
`news_staytime` string,
`loading_time` string,
`type1` string,
`category` string,
`server_time` string)
PARTITIONED BY (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_newsdetail_log/';

insert overwrite table dwd_newsdetail_log PARTITION (dt='2020-06-21')
select
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.entry') entry,
    get_json_object(event_json,'$.kv.action') action,
    get_json_object(event_json,'$.kv.goodsid') goodsid,
    get_json_object(event_json,'$.kv.showtype') showtype,
    get_json_object(event_json,'$.kv.news_staytime') news_staytime,
    get_json_object(event_json,'$.kv.loading_time') loading_time,
    get_json_object(event_json,'$.kv.type1') type1,
    get_json_object(event_json,'$.kv.category') category,
    server_time
from dwd_base_event_log
where dt='2020-06-21' and event_name='newsdetail';


insert overwrite table dwd_notification_log PARTITION (dt='2020-06-21')
select
mid_id,
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
app_time,
network,
lng,
lat,
get_json_object(event_json,'$.kv.action') action,
get_json_object(event_json,'$.kv.noti_type') noti_type,
get_json_object(event_json,'$.kv.ap_time') ap_time,
get_json_object(event_json,'$.kv.content') content,
server_time
from dwd_base_event_log
where dt='2020-06-21' and event_name='notification';

insert overwrite table dwd_active_background_log PARTITION
(dt='2020-06-21')
select
mid_id,
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
app_time,
network,
lng,
lat,
get_json_object(event_json,'$.kv.active_source') active_source,
server_time
from dwd_base_event_log
where dt='2020-06-21' and event_name='active_background';


insert overwrite table dwd_error_log PARTITION (dt='2020-06-21')
select
mid_id,
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
app_time,
network,
lng,
lat,
get_json_object(event_json,'$.kv.errorBrief') errorBrief,
get_json_object(event_json,'$.kv.errorDetail') errorDetail,
server_time
from dwd_base_event_log
where dt='2020-06-21' and event_name='error';


DROP TABLE IF EXISTS `dwd_dim_sku_info`;
CREATE EXTERNAL TABLE `dwd_dim_sku_info` (
`id` string COMMENT '商品id',
`spu_id` string COMMENT 'spuid',
`price` double COMMENT '商品价格',
`sku_name` string COMMENT '商品名称',
`sku_desc` string COMMENT '商品描述',
`weight` double COMMENT '重量',
`tm_id` string COMMENT '品牌id',
`tm_name` string COMMENT '品牌名称',
`category3_id` string COMMENT '三级分类id',
`category2_id` string COMMENT '二级分类id',
`category1_id` string COMMENT '一级分类id',
`category3_name` string COMMENT '三级分类名称',
`category2_name` string COMMENT '二级分类名称',
`category1_name` string COMMENT '一级分类名称',
`spu_name` string COMMENT 'spu 名称',
`create_time` string COMMENT '创建时间'
) COMMENT '商品维度表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_sku_info/'

insert overwrite table dwd_dim_sku_info partition(dt='2020-06-18')
select
sku.id,
sku.spu_id,
sku.price,
sku.sku_name,
sku.sku_desc,
sku.weight,
sku.tm_id,
ob.tm_name,
sku.category3_id,
c2.id category2_id,
c1.id category1_id,
c3.name category3_name,
c2.name category2_name,
c1.name category1_name,
spu.spu_name,
sku.create_time
from
(
select * from ods_sku_info where dt='2020-06-18'
)sku
join
(
select * from ods_base_trademark where dt='2020-06-18'
)ob on sku.tm_id=ob.tm_id
join
(
select * from ods_spu_info where dt='2020-06-18'
)spu on spu.id = sku.spu_id
join
(
select * from ods_base_category3 where dt='2020-06-18'
)c3 on sku.category3_id=c3.id
join
(
select * from ods_base_category2 where dt='2020-06-18'
)c2 on c3.category2_id=c2.id
join
(
select * from ods_base_category1 where dt='2020-06-18'
)c1 on c2.category1_id=c1.id;


insert overwrite table dwd_dim_activity_info partition(dt='2020-06-18')
select
info.id,
info.activity_name,
info.activity_type,
rule.condition_amount,
rule.condition_num,
rule.benefit_amount,
rule.benefit_discount,
rule.benefit_level,
info.start_time,
info.end_time,
info.create_time
from
(
select * from ods_activity_info where dt='2020-06-18'
)info
left join
(
select * from ods_activity_rule where dt='2020-06-18'
)rule on info.id = rule.activity_id;


insert overwrite table dwd_fact_order_detail partition(dt='2020-06-18')
select
od.id,
od.order_id,
od.user_id,
od.sku_id,
od.sku_name,
od.order_price,
od.sku_num,
od.create_time,
oi.province_id,
od.order_price*od.sku_num
from
(
select * from ods_order_detail where dt='2020-06-18'
) od
join
(
    select
     id,
     province_id
    from ods_order_info where dt='2020-06-18'
) oi
on od.order_id=oi.id;


insert overwrite table dwd_fact_payment_info partition(dt='2020-06-18')
select
pi.id,
pi.out_trade_no,
pi.order_id,
pi.user_id,
pi.alipay_trade_no,
pi.total_amount,
pi.subject,
pi.payment_type,
pi.payment_time,
oi.province_id
from
(
select * from ods_payment_info where dt='2020-06-18'
)pi
join
(
select id, province_id from ods_order_info where dt='2020-06-18'
)oi
on pi.order_id = oi.id;


set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_fact_coupon_use partition(dt)
select
if(new.id is null,old.id,new.id),
if(new.coupon_id is null,old.coupon_id,new.coupon_id),
if(new.user_id is null,old.user_id,new.user_id),
if(new.order_id is null,old.order_id,new.order_id),
if(new.coupon_status is null,old.coupon_status,new.coupon_status),
if(new.get_time is null,old.get_time,new.get_time),
if(new.using_time is null,old.using_time,new.using_time),
if(new.used_time is null,old.used_time,new.used_time),
date_format(if(new.get_time is null,old.get_time,new.get_time),'yyyy-MM-dd')
from
(
select
id,
coupon_id,
user_id,
order_id,
coupon_status,
get_time,
using_time,
used_time
from dwd_fact_coupon_use
where dt in
(
select
date_format(get_time,'yyyy-MM-dd')
from ods_coupon_use
where dt='2020-03-10'
)
)old
full outer join
(
select
id,
coupon_id,
user_id,
order_id,
coupon_status,
get_time,
using_time,
used_time
from ods_coupon_use
where dt='2020-03-10'
)new
on old.id=new.id;


