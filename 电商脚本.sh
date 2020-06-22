#----------------hdfs_to_ods_db.sh---------
#!/bin/bash
APP=gmall
hive=/opt/module/hive/bin/hive
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
do_date=$2
else
do_date=`date -d "-1 day" +%F`
fi
sql1="
load data inpath '/origin_data/$APP/db/order_info/$do_date' OVERWRITE into table
${APP}.ods_order_info partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/order_detail/$do_date' OVERWRITE into table
${APP}.ods_order_detail partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/sku_info/$do_date' OVERWRITE into table
${APP}.ods_sku_info partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/user_info/$do_date' OVERWRITE into table
${APP}.ods_user_info partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/payment_info/$do_date' OVERWRITE into table
${APP}.ods_payment_info partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/base_category1/$do_date' OVERWRITE into table
${APP}.ods_base_category1 partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/base_category2/$do_date' OVERWRITE into table
${APP}.ods_base_category2 partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/base_category3/$do_date' OVERWRITE into table
${APP}.ods_base_category3 partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/base_trademark/$do_date' OVERWRITE into table
${APP}.ods_base_trademark partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/activity_info/$do_date' OVERWRITE into table
${APP}.ods_activity_info partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/activity_order/$do_date' OVERWRITE into table
${APP}.ods_activity_order partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/cart_info/$do_date' OVERWRITE into table
${APP}.ods_cart_info partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/comment_info/$do_date' OVERWRITE into table
${APP}.ods_comment_info partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/coupon_info/$do_date' OVERWRITE into table
${APP}.ods_coupon_info partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/coupon_use/$do_date' OVERWRITE into table
${APP}.ods_coupon_use partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/favor_info/$do_date' OVERWRITE into table
${APP}.ods_favor_info partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/order_refund_info/$do_date' OVERWRITE into table
${APP}.ods_order_refund_info partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/order_status_log/$do_date' OVERWRITE into table
${APP}.ods_order_status_log partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/spu_info/$do_date' OVERWRITE into table
${APP}.ods_spu_info partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/activity_rule/$do_date' OVERWRITE into table
${APP}.ods_activity_rule partition(dt='$do_date');
load data inpath '/origin_data/$APP/db/base_dic/$do_date' OVERWRITE into table
${APP}.ods_base_dic partition(dt='$do_date');
"
sql2="
load data inpath '/origin_data/$APP/db/base_province/$do_date' OVERWRITE into table
${APP}.ods_base_province;
load data inpath '/origin_data/$APP/db/base_region/$do_date' OVERWRITE into table
${APP}.ods_base_region;
" case $
1
in
"first"){
$hive -e "$sql1"
$hive -e "$sql2"
};;
"all"){
$hive -e "$sql1"
};;
esac

#----------------------------------------------------------------------------------------------
# ods_to_dwd_start_log.sh
#!/bin/bash
APP=gmall
hive = /home/hadoop/tool/hive/bin/hive
if [ -n "$1" ]; then
    do_date=$1
else
    do_date=`date -d '-1 day' +%F`
fi

sql="
insert overwrite table ${APP}.dwd_start_log
partition(dt='$do_date')
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
where dt='${do_date}';
"

$hive -e "$sql"



--------------------------------------------------------
ods_to_dwd_base_log.sh

#!/bin/bash
APP=gmall
hive = /home/hadoop/tool/hive/bin/hive
if [ -n "$1" ]; then
    do_date=$1
else
    do_date=`date -d '-1 day' +%F`
fi

sql="
use ${APP};
insert overwrite table ${APP}.dwd_base_event_log
partition(dt='$do_date')
select
    base_analizer(line,'mid') as  mid_id,
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
    base_analizer(line,'st') as server_time
from ${APP}.ods_event_log lateral view flat_analizer(base_analizer(line,'et')) tmp_flat
as event_name,event_json
where dt='${do_date}' and base_analizer(line,'et')<>"";
"
$hive -e "$sql"

------------------------------------------------------------


# 定义变量方便修改
APP=gmall
hive=/opt/module/hive/bin/hive
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
do_date=$1
else
do_date=`date -d "-1 day" +%F`
fi
sql="
insert overwrite table "$APP".dwd_display_log
PARTITION (dt='$do_date')
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
from "$APP".dwd_base_event_log
where dt='$do_date' and event_name='display';
insert overwrite table "$APP".dwd_newsdetail_log
PARTITION (dt='$do_date')
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
get_json_object(event_json,'$.kv.news_staytime')
news_staytime,
get_json_object(event_json,'$.kv.loading_time')
loading_time,
get_json_object(event_json,'$.kv.type1') type1,
get_json_object(event_json,'$.kv.category') category,
server_time
from "$APP".dwd_base_event_log
where dt='$do_date' and event_name='newsdetail';
insert overwrite table "$APP".dwd_loading_log
PARTITION (dt='$do_date')
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
get_json_object(event_json,'$.kv.loading_time')
loading_time,
get_json_object(event_json,'$.kv.loading_way') loading_way,
get_json_object(event_json,'$.kv.extend1') extend1,
get_json_object(event_json,'$.kv.extend2') extend2,
get_json_object(event_json,'$.kv.type') type,
get_json_object(event_json,'$.kv.type1') type1,
server_time
from "$APP".dwd_base_event_log
where dt='$do_date' and event_name='loading';
insert overwrite table "$APP".dwd_ad_log
PARTITION (dt='$do_date')
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
get_json_object(event_json,'$.kv.contentType') contentType,
get_json_object(event_json,'$.kv.displayMills')
displayMills,
get_json_object(event_json,'$.kv.itemId') itemId,
get_json_object(event_json,'$.kv.activityId') activityId,
server_time
from "$APP".dwd_base_event_log
where dt='$do_date' and event_name='ad';
insert overwrite table "$APP".dwd_notification_log
PARTITION (dt='$do_date')
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
from "$APP".dwd_base_event_log
where dt='$do_date' and event_name='notification';
insert overwrite table "$APP".dwd_active_background_log
PARTITION (dt='$do_date')
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
get_json_object(event_json,'$.kv.active_source')
active_source,
server_time
from "$APP".dwd_base_event_log
where dt='$do_date' and event_name='active_background';
insert overwrite table "$APP".dwd_comment_log
PARTITION (dt='$do_date')
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
get_json_object(event_json,'$.kv.comment_id') comment_id,
get_json_object(event_json,'$.kv.userid') userid,
get_json_object(event_json,'$.kv.p_comment_id')
p_comment_id,
get_json_object(event_json,'$.kv.content') content,
get_json_object(event_json,'$.kv.addtime') addtime,
get_json_object(event_json,'$.kv.other_id') other_id,
get_json_object(event_json,'$.kv.praise_count')
praise_count,
get_json_object(event_json,'$.kv.reply_count') reply_count,
server_time
from "$APP".dwd_base_event_log
where dt='$do_date' and event_name='comment';
insert overwrite table "$APP".dwd_favorites_log
PARTITION (dt='$do_date')
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
get_json_object(event_json,'$.kv.id') id,
get_json_object(event_json,'$.kv.course_id') course_id,
get_json_object(event_json,'$.kv.userid') userid,
get_json_object(event_json,'$.kv.add_time') add_time,
server_time
from "$APP".dwd_base_event_log
where dt='$do_date' and event_name='favorites';
insert overwrite table "$APP".dwd_praise_log
PARTITION (dt='$do_date')
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
get_json_object(event_json,'$.kv.id') id,
get_json_object(event_json,'$.kv.userid') userid,
get_json_object(event_json,'$.kv.target_id') target_id,
get_json_object(event_json,'$.kv.type') type,
get_json_object(event_json,'$.kv.add_time') add_time,
server_time
from "$APP".dwd_base_event_log
where dt='$do_date' and event_name='praise';

insert overwrite table "$APP".dwd_error_log
PARTITION (dt='$do_date')
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
from "$APP".dwd_base_event_log
where dt='$do_date' and event_name='error';
"
$hive -e "$sql"