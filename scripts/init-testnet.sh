set -eu
umask 000

count="$1"

# Generate the full testnet only inside the initialization container.
cometbft testnet \
	--v "$count" \
	--o /tmp/generated \
	--populate-persistent-peers=true \
	--hostname-prefix node

# Export only configuration and keys, not data.
i=0
while [ "$i" -lt "$count" ]; do
	dest="/out/node$i"

	mkdir "$dest"
	cp -R "/tmp/generated/node$i/config/." "$dest/"

	# Make exported directories and files readable and writable by everyone.
	chmod 777 "$dest"
	chmod 666 \
		"$dest/config.toml" \
		"$dest/genesis.json" \
		"$dest/node_key.json" \
		"$dest/priv_validator_key.json"

	i=$((i + 1))
done
