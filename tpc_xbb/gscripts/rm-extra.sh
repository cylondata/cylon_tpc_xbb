#!/bin/bash

#dd=../../datum/bb_sf10/16
dd=/scratch_hdd/auyar/bb_sf100/1

upperdirs="data data_refresh"
lowerdirs="household_demographics item_marketprices ship_mode time_dim web_returns customer_address income_band product_reviews store warehouse customer_demographics  inventory promotion store_returns web_clickstreams web_site item reason web_page"

for upperdir in $upperdirs; do
   for lowerdir in $lowerdirs; do
      dn=$dd/$upperdir/$lowerdir
      echo "deleting: $dn"
      rm -rf $dn
   done
done
