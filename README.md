# rpcbridge

A minimal Linux daemon written in Rust that redirects `/run/user/1000/discord-ipc-N` to your Discord Application's Social SDK. This helps show your presence on your profile without needing to run a full Discord client.

```
app with Discord RPC
  -> discord-ipc-0
  -> rpcbridge (SET_ACTIVITY)
  -> OAuth2 Gateway
  -> your profile
```

## what it handles

- opcode 0 handshake, `READY`, `SET_ACTIVITY`, clear, ping/pong
- details, state, timestamps, buttons, party, `status_display_type`
- named assets and external HTTPS artwork
- up to five activities at once, newest first, one per application; clearing or disconnecting drops only that app
- Gateway heartbeat, reconnect backoff, token refresh, five updates per 20 seconds

It is activity-only. Voice, join, invites and the rest of the local RPC surface are rejected.

## requirements

- Linux (Unix sockets; no Windows/Mac named pipes yet)
- Rust 1.85 or newer to build
- a Discord application with:
  - **Discord Social SDK** enabled
  - the `sdk.social_layer_presence` scope available
  - **OAuth2 → Public Client** on, or a `client_secret` in the config
  - this redirect in **OAuth2 → Redirects**: `http://127.0.0.1:8765/callback`

There is no built-in application ID, you'll have to provide your own. Run `rpcbridge login` and enter your application ID when prompted, or pass it directly:

```bash
rpcbridge login --client-id 123456789012345678
```

The id is written to `client_id` in the config, which is later on stored in the config & reused.

## build and install

```bash
cargo build --release
install -Dm755 target/release/rpcbridge ~/.local/bin/rpcbridge
install -Dm644 packaging/rpcbridge.service ~/.config/systemd/user/rpcbridge.service
```

no packaging implemented.

## sign in

```bash
rpcbridge login
```

This opens the Discord consent page, catches the loopback callback, and stores the access and refresh tokens in `~/.local/share/rpcbridge/auth.json` with mode `0600`.

Tokens renew automatically: the daemon checks every 30 minutes and refreshes a day before expiry, and the Gateway refreshes after a `4004`. This needs **Public Client** on, or a `client_secret` in the config.

## run

```bash
rpcbridge run
```

or:

```bash
systemctl --user daemon-reload
systemctl --user enable --now rpcbridge
```

Check it:

```bash
rpcbridge status
rpcbridge probe --seconds 15
```

`probe` is a tiny RPC client. It asks the daemon for its `READY`, sends a test activity, holds it, then clears it. It uses `client_id` unless `--app-id` is passed.

## config

`~/.config/rpcbridge/config.toml`:

```toml
client_id = "YOUR_APPLICATION_ID"
# client_secret = "..." # only for a confidential client
redirect_uri = "http://127.0.0.1:8765/callback"
# socket_index = 0
status = "online" # online, idle, dnd, invisible
gateway_url = "wss://gateway.discord.gg/?v=10&encoding=json"
```

The daemon takes the first free `discord-ipc-N`. Most clients try slot 0 first, so if another Discord client is listening there, stop it or set `socket_index`.

The OAuth session has its own `status`. Setting `online` can make you appear online even when another client is idle. Use `dnd` or `idle` in the config if that matters.

## license

GPL-3.0-or-later.