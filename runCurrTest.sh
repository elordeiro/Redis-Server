echo "redis-cli ping"; redis-cli ping
echo "redis-cli echo hello"; redis-cli echo hello
echo "redis-cli set foo bar"; redis-cli set foo bar
echo "redis-cli set bar bax px 5000"; redis-cli set bar baz px 5000
echo "redis-cli get foo"; redis-cli get foo
echo "redis-cli get bar"; redis-cli get bar
echo "sleep 5"; sleep 5
echo "redis-cli get foo"; redis-cli get foo
echo "redis-cli get bar"; redis-cli get bar
echo "redis-cli -p 6380 get foo"; redis-cli -p 6380 get foo
echo "redis-cli -p 6380 get bar"; redis-cli -p 6380 get bar
echo "redis-cli wait 2 5000"; redis-cli wait 2 5000

echo "redis-cli xadd mystream 0-1 temperature 19.8"; redis-cli xadd mystream * temperature 19.8
echo "redis-cli xadd mystream 0-2 temperature 21.6"; redis-cli xadd mystream * temperature 21.6
echo "redis-cli xadd mystream 0-3 temperature 22.4"; redis-cli xadd mystream * temperature 22.4

echo "redis-cli xread streams mystream 0-1 +"; redis-cli xread streams mystream 0-1 +
