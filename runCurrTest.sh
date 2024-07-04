redis-cli XADD stream_key 0-1 temperature 96
redis-cli XREAD block 1000 streams stream_key 0-1

# sleep for 0.5 second
sleep 0.5

redis-cli XADD stream_key 0-2 temperature 97
redis-cli XREAD block 1000 streams stream_key 0-2