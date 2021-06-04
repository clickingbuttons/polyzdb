#!/bin/bash
./target/release/polyzdb \
  --trades \
	--ratelimit 99 \
	--data-dir /mnt/ssd1 \
	--data-dir /mnt/ssd2 \
	--data-dir /mnt/ssd3 \
	--data-dir /mnt/ssd4 \
	--data-dir /mnt/ssd5 \
	--data-dir /mnt/ssd6 \
	--data-dir /mnt/ssd7 \
	--data-dir /mnt/ssd8 \
  2> >(tee stderr-trades.log)
