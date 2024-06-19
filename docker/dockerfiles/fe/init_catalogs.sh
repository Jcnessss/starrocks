#!/bin/bash

FE_SERVICE=${FE_SERVICE:-127.0.0.1}
FE_QUERY_PORT=${FE_QUERY_PORT:-9030}
FE_HTTP_PORT=${FE_HTTP_PORT:-8030}
PROBE_TIMEOUT=30
PROBE_INTERVAL=3
STARROCKS_ROOT=${STARROCKS_ROOT:-"/opt/starrocks"}
STARROCKS_HOME=${STARROCKS_ROOT}/fe
CONFIGMAP_MOUNT_PATH=${STARROCKS_HOME}/catalogs

log_std()
{
    echo "[`date`] $@"
}

show_catalogs()
{
    local svc=$1
    timeout 15 mysql --connect-timeout 2 -h $svc -P $FE_QUERY_PORT -u root --skip-column-names --batch -e 'SHOW CATALOGS;'
}

create_catalog_by_name()
{
    local svc=$1
    local catalog=$2
    local initcatalog=$3
    start=`date +%s`
    local timeout=$PROBE_TIMEOUT

    while true
    do
        #log_std "Create external catalog ..."
        timeout 15 mysql --connect-timeout 2 -h "${svc}" -P "${FE_QUERY_PORT}" -u root --skip-column-names  < \
              "${initcatalog}"

        cataloglist=`show_catalogs $svc`
        if echo "$cataloglist" | grep -q -w "${catalog}" &>/dev/null ; then
            break;
        fi

        let "expire=start+timeout"
        now=`date +%s`
        if [[ $expire -le $now ]] ; then
            log_std "Time out, abort!"
            exit 1
        fi

        sleep $PROBE_INTERVAL

    done
}

check_catalog_if_exist()
{
    local svc=$1
    local catalog=$2
    cataloglist=`show_catalogs $svc`
    #log_std "catalogs list $cataloglist"
    if echo "$cataloglist" | grep -q -w "${catalog}" &>/dev/null ; then
      	#log_std "$catalog exist"
        echo 1
    else
      	#log_std "$catalog not exist"
        echo 0
    fi
}

create_catalog_if_not_exist()
{
    local svc=$1
    local catalog=$2
    local initcatalog=$3
    local exist=`check_catalog_if_exist ${svc} ${catalog}`
    if [[ "x$exist" != "x1" ]] ; then
        log_std "$catalog not exist, create it"
        create_catalog_by_name ${svc} ${catalog} ${initcatalog}
    else
        log_std "$catalog exist, skip"
    fi
}

create_catalogs_from_configmap()
{
    local svc=$1
    if [[ "x$CONFIGMAP_MOUNT_PATH" == "x" ]] ; then
        log_std 'Empty $CONFIGMAP_MOUNT_PATH env var, skip it!'
        return 0
    fi
    if ! test -d $CONFIGMAP_MOUNT_PATH ; then
        log_std "$CONFIGMAP_MOUNT_PATH not exist or not a directory, ignore ..."
        return 0
    fi

    for catalog in `ls $CONFIGMAP_MOUNT_PATH`
    do
        log_std "Process catalog $catalog ..."
        local initcatalog=$CONFIGMAP_MOUNT_PATH/$catalog
        if test -e $initcatalog ; then
            #log_std "init catalog in $initcatalog"
            create_catalog_if_not_exist $svc $catalog $initcatalog
        fi
    done
}

fe_health_check() {
    local svc=$1
    local url=http://$svc:$FE_HTTP_PORT/api/health
    local timeout=180  # 超时时间（秒）
    local interval=5  # 每次请求之间的间隔（秒）

    sleep 5
    start_time=$(date +%s)

    while true; do
      # 发送 HTTP 请求
      response=$(curl -s -o /dev/null -w "%{http_code}" "$url")
      echo "fe health check $response"

      # 检查响应状态码
      if [ "$response" -eq 200 ]; then
        echo "FE health check success"
        return 0
      fi

      # 检查是否超时
      current_time=$(date +%s)
      elapsed_time=$((current_time - start_time))
      if [ "$elapsed_time" -ge "$timeout" ]; then
        echo "FE health check timeout"
        exit 1
      fi

      sleep "$interval"
    done
}

fe_service=$FE_SERVICE
fe_health_check $fe_service
create_catalogs_from_configmap $fe_service

