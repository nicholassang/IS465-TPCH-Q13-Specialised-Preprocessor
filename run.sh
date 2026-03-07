#!/bin/bash

DATA=$1
OUT=$2

./tpch_q3 \
    $DATA/customer_q3.parquet \
    $DATA/orders_q3.parquet \
    $DATA/lineitem_q3.parquet \
    > $OUT