set -eu
umask 000

export CMTHOME=/tmp/validator

if [ ! -d "$CMTHOME" ]; then
	mkdir "$CMTHOME"
	mkdir "$CMTHOME/data"

	ln -s /input-config "$CMTHOME/config"

	printf '%s\n' '{"height":"0","round":0,"step":0}' \
		>"$CMTHOME/data/priv_validator_state.json"
else
	test -f "$CMTHOME/data/priv_validator_state.json"
	test -L "$CMTHOME/config"
fi

# exec lets Docker's stop signal reach CometBFT directly.
# Disable outbound dialing and PEX. The interceptor establishes all connections.
exec cometbft start \
	--home "$CMTHOME" \
	--proxy_app=persistent_kvstore \
	--rpc.laddr=tcp://0.0.0.0:26657 \
	--p2p.laddr=tcp://0.0.0.0:26656 \
	--p2p.persistent_peers= \
	--p2p.seeds= \
	--p2p.pex=false
