#!/bin/bash
#set -x
#!!!!!!!not realized yet!!!!!!!

function putMeta {
    HTTP_CODE=$(curl -o -s -w "%{http_code}" -XPUT "http://localhost:8080/custodian/meta" --data "@$1.json" 2>/dev/null)
    if [ "$HTTP_CODE" != "204" ]
    then
        echo "Putting meta '$1' is failed. Http response code: $HTTP_CODE"
        exit
    fi
    echo "Meta '$1' is processed"
}

putMeta country 
