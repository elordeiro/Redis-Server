redis-cli xadd testStream 0-1 foo1 bar1
redis-cli xadd testStream 0-2 foo2 bar2
redis-cli xadd testStream 0-3 foo3 bar3
redis-cli xadd testStream 0-4 foo4 bar4
redis-cli xrange testStream - 0-3