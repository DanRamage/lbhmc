#!/bin/bash

if [ ! -f /tmp/lock_populate_xenia_lbhmc ]; then
  touch /tmp/lock_populate_xenia_lbhmc
  /usr/local/bin/python /home/xeniaprod/scripts/postgresql/feeds/lbhmc/populate_lbhmc.py --ConfigFile=/home/xeniaprod/scripts/postgresql/feeds/lbhmc/lbhmc.ini
  rm -f /tmp/lock_populate_xenia_lbhmc
fi
