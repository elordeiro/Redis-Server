redis-cli xadd blueberry 0-1 temperature 25
redis-cli xadd blueberry 0-2 temperature 26
redis-cli xread streams blueberry 0-1
redis-cli xread streams blueberry 0-1 0-2