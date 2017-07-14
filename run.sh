#!/bin/sh
spark-submit \
    --class Algorithm.dataProcess \
    --master yarn-client \
    --executor-memory 20G \
    --total-executor-cores 200 \
    --conf "spark.executor.extraJavaOptions=-Dfile.encoding=UTF-8"\
    --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8" \
    target/ProcessVideoInfo-1.0.jar \
    /data/stage/outface/omg/tdw/export_video_t_video_info_extend_hour/ds=2017070715/attempt_*.gz  \
    baronfeng_video/output 

