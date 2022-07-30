[![Build Status](https://travis-ci.org/isaiah-perumalla/kdb4j.svg?branch=main)](https://travis-ci.org/isaiah-perumalla/kdb4j)
# kdb4j efficient Java library for KDB+ Time Series Database
## experimental not fully functional yet !
## goals 
* Event driven java lib for kdb+ publishing and subcribing to [kdb+ ticker plant](http://code.kx.com/q/) 
* Non blocking IO using a single thread event loop
* Allocation free in steady state


* 
## Building kdb ticker plant docker image
1. build docker image, `cd kdb` 
2. `sudo docker build -t kdb4j-test:latest .`
3. tickerplant listens on port 5010, RDB on 5011 and HDB on port 5012
4. `docker run `docker run -t -p 5010-5012:5010-5012 kdb4j-test:latest -v <dir-to-store-hdb>:/data`
5. run `SystemTests` to verify can connect to ticker plant inside docker container

`
