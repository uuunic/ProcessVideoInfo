#!/bin/sh
spark-submit \
    --class Algorithm.VidCidWash \
    --master yarn \
    --deploy-mode client\
    --num-executors 100 \
    --executor-memory 4G \
    --executor-cores 4 \
    --driver-cores 5 --driver-memory 20G \
    --conf "spark.driver.maxResultSize=5G" \
    --conf "spark.kryoserializer.buffer.max.mb=512" \
    --conf "spark.ui.port=4039" \
    --conf "spark.executor.extraJavaOptions=-Dfile.encoding=UTF-8"\
    --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8" \
    ProcessVideoInfo-1.0-jar-with-dependencies.jar \
    /data/stage/outface/omg/tdw/export_video_t_video_info_extend_hour/ds=2017080212/attempt_*.gz /
    useless_arg
