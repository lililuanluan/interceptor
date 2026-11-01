import asyncio
import json
import struct
from contextlib import suppress
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAX_FRAME_BYTES = 64 * 1024 * 1024


async def send_json(writer, message):
    data = json.dumps(message).encode("utf-8")
    if len(data) > MAX_FRAME_BYTES:
        raise ValueError("IPC frame too large")

    writer.write(struct.pack("!I", len(data)) + data)
    await writer.drain()


async def receive_json(reader):
    prefix = await reader.readexactly(4)
    length = struct.unpack("!I", prefix)[0]

    if length > MAX_FRAME_BYTES:
        raise ValueError("IPC frame too large")

    data = await reader.readexactly(length)
    return json.loads(data)


async def handle_connection(reader, writer):
    try:
        hello = await receive_json(reader)
        if hello != {"type": "hello", "version": 1}:
            raise ValueError(f"Unexpected hello: {hello}")

        print("[python] Rust connected", flush=True)
        await send_json(writer, {"type": "ready", "version": 1})

        # 目前只有握手，等待 Rust 关闭连接。
        await reader.read()
        print("[python] Rust disconnected", flush=True)
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    cluster_id = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    socket_path = Path(f"/tmp/interceptor-{cluster_id}.sock")

    server = await asyncio.start_unix_server(
        handle_connection,
        path=str(socket_path),
    )

    print(f"[python] listening: {socket_path}", flush=True)
    process = None

    try:
        async with server:
            process = await asyncio.create_subprocess_exec(
                str(ROOT / "target/release/interceptor-dev"),
                "--cluster-id",
                cluster_id,
                "--max-block",
                "15",
                "--log-root",
                str(ROOT / "logs"),
                "--tx-config-yaml",
                str(ROOT / "tx-example.yaml"),
                cwd=str(ROOT),
            )
            try:
                return_code = await asyncio.wait_for(
                    process.wait(),
                    timeout=100,
                )
            except asyncio.TimeoutError:
                print("[python] experiment timed out", flush=True)
                return_code = 124
    finally:
        if process is not None and process.returncode is None:
            with suppress(ProcessLookupError):
                process.terminate()

            try:
                await asyncio.wait_for(process.wait(), timeout=30)
            except asyncio.TimeoutError:
                with suppress(ProcessLookupError):
                    process.kill()
                await process.wait()

        socket_path.unlink(missing_ok=True)

    raise SystemExit(return_code)


if __name__ == "__main__":
    asyncio.run(main())
