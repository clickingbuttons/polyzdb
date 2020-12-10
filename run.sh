./target/release/polyzdb \
	--ratelimit 99 \
	-d /mnt/ssd1 \
	-d /mnt/ssd2 \
	-d /mnt/ssd3 \
	-d /mnt/ssd4 \
	-d /mnt/ssd5 \
	-d /mnt/ssd6 \
	-d /mnt/ssd7 \
	-d /mnt/ssd8 \
  2> >(tee 01.log)
