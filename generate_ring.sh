#!/bin/bash

if [ "$#" = 1 ] 
then 
  SERVER=$1
else 
  SERVER='mfmn1@127.0.0.1'
fi

rm -r ./dev/*
make devrel
for d in dev/dev*; do $d/bin/mfmn start; done
for d in dev/dev{2,3}; do $d/bin/mfmn-admin join $SERVER; done
./dev/dev1/bin/mfmn-admin member_status
./dev/dev2/bin/mfmn attach
