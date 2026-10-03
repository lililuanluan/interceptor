set -eu
umask 000

count="$1"

# 完整 testnet 只生成在初始化容器内部。
cometbft testnet \
	--v "$count" \
	--o /tmp/generated \
	--populate-persistent-peers=true \
	--hostname-prefix node

# 只导出配置和密钥，不导出 data。
i=0
while [ "$i" -lt "$count" ]; do
	dest="/out/node$i"

	mkdir "$dest"
	cp -R "/tmp/generated/node$i/config/." "$dest/"

	# 导出的目录和文件对所有用户开放读写权限。
	chmod 777 "$dest"
	chmod 666 \
		"$dest/config.toml" \
		"$dest/genesis.json" \
		"$dest/node_key.json" \
		"$dest/priv_validator_key.json"

	i=$((i + 1))
done
