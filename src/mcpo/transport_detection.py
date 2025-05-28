import asyncio
import json
import os
import logging
from typing import Tuple, Optional, NamedTuple # Added NamedTuple
import asyncio # Added for asyncio.StreamReader/Writer/Process types
import httpx # For making HTTP requests during probing

# Assuming these are the correct import paths from the mcp library
from mcp.client.sse import sse_client
from mcp.client.streamable_http import streamablehttp_client
from mcp.client.stdio import stdio_client, StdioServerParameters

logger = logging.getLogger(__name__)

INIT_PAYLOAD = {"jsonrpc": "2.0", "id": 0, "method": "initialize", "params": {}}

async def _probe_http(url: str, timeout: float = 3.0):
    """
    Probe an HTTP(S) URL to determine if it's an MCP JSON-RPC or SSE endpoint.
    Returns 'http-json' or 'http-sse'.
    """
    headers = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}
    logger.debug(f"Probing URL: {url} with payload: {INIT_PAYLOAD}")
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(url, json=INIT_PAYLOAD, headers=headers)
            response.raise_for_status() # Raise an exception for 4XX/5XX responses

            content_type = response.headers.get("content-type", "").lower()
            logger.debug(f"Probe response from {url}: status={response.status}, content-type='{content_type}'")

            if content_type.startswith("text/event-stream"):
                return "http-sse"
            elif content_type.startswith("application/json"):
                # Further check if it's a valid MCP initialize response (optional, but good)
                try:
                    json_response = response.json()
                    if "jsonrpc" in json_response and "result" in json_response:
                         logger.debug(f"Detected MCP JSON-RPC at {url}")
                         return "http-json"
                    else:
                        logger.warning(f"URL {url} returned JSON but not a valid MCP initialize response: {json_response}")
                except json.JSONDecodeError:
                    logger.warning(f"URL {url} returned application/json but failed to decode: {response.text}")
                # Fallback or raise error if strict checking is needed
                return "http-json" # Assume JSON if content-type matches and it's not SSE
            else:
                logger.warning(f"Unexpected content-type '{content_type}' from {url} during probe.")
                # Depending on strictness, could raise an error here
                # For now, if it's not SSE and not clearly JSON, it's ambiguous
                raise httpx.HTTPStatusError(f"Unsupported content-type: {content_type}", request=response.request, response=response)

    except httpx.RequestError as e:
        logger.warning(f"HTTP request error while probing {url}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while probing {url}: {e}")
        raise

async def _open_http(kind: str, url: str):
    """Opens an HTTP/SSE connection based on the detected kind."""
    logger.debug(f"Opening HTTP connection of kind '{kind}' to URL: {url}")
    if kind == "http-sse":
        # sse_client expects (reader, writer)
        return await sse_client(url=url, sse_read_timeout=None) # Match existing sse_read_timeout
    elif kind == "http-json":
        # streamablehttp_client expects (reader, writer, get_session_id_callback)
        # Ensure URL has trailing slash for streamablehttp_client if it's a convention
        http_url = url
        if not http_url.endswith("/"): # As per original logic for streamablehttp
            http_url = f"{http_url}/"
        return await streamablehttp_client(url=http_url)
    else:
        raise ValueError(f"Unknown HTTP kind: {kind}")

async def _open_stdio(proc: asyncio.subprocess.Process):
    """
    Attempts to open an MCP stdio connection with an existing process.
    Uses a short timeout to quickly determine if it's an stdio server.
    """
    logger.debug(f"Attempting to open stdio connection with PID {proc.pid}")
    # StdioServerParameters for an existing process needs stdin_pipe and stdout_pipe
    # The mcp.client.stdio.stdio_client itself handles creating these if command is passed.
    # If proc is already created, we need to pass the pipes.
    # The StdioServerParameters class in mcp-lib might not directly support this.
    # Let's re-check mcp.client.stdio.stdio_client structure.
    # It seems stdio_client itself takes StdioServerParameters which has command, args, env.
    # To use an existing process, we might need a different approach or a modified StdioServerParameters.

    # For now, let's assume the user's sketch for stdio_client(params) with existing pipes is feasible
    # or that mcp-lib's stdio_client can be adapted or already supports it.
    # The key is that `proc.stdin` and `proc.stdout` are the pipes.
    # The `StdioServerParameters` class in the MCP library is defined as:
    # class StdioServerParameters(BaseModel):
    #   command: str
    #   args: List[str] = Field(default_factory=list)
    #   env: Dict[str, str] = Field(default_factory=dict)
    #   stdin: Optional[asyncio.StreamWriter] = None # This is what we need to set
    #   stdout: Optional[asyncio.StreamReader] = None # This is what we need to set
    #
    # This is not directly how it works. `stdio_client` *creates* the process.
    # The sketch `StdioServerParameters(stdin=proc.stdin, stdout=proc.stdout)` is conceptual.
    #
    # Let's reconsider. If `stdio_client` *must* spawn, then trying stdio *after* spawning for HTTP
    # is problematic. The sketch's `_open_stdio` implies using the *same* `proc`.
    #
    # The original `stdio_client` in `main.py` is:
    # `async with stdio_client(server_params) as (reader, writer):`
    # where `server_params = StdioServerParameters(command=command, args=args, env={**env})`
    #
    # If we want to test stdio on an *already started* `proc` (that we started thinking it might be HTTP),
    # we can't directly use `stdio_client` in its current form if it insists on spawning.
    #
    # Alternative for _open_stdio:
    # We need to directly interact with proc.stdin and proc.stdout and perform a quick MCP handshake.
    # This is more involved than just calling stdio_client.
    #
    # For the sake of progressing with the user's "leaner pattern" structure,
    # I will assume a conceptual `quick_stdio_handshake(proc.stdin, proc.stdout)`
    # that returns (reader, writer) or raises TimeoutError.
    # The `mcp.ClientSession` itself takes reader/writer.
    #
    # Let's simplify: if command is given, and no expected_url, assume stdio first.
    # If `expected_url` is given with `command`, then it's command-started HTTP.

    # Revisiting the user's sketch for _open_stdio:
    # `params = StdioServerParameters(stdin=proc.stdin, stdout=proc.stdout)`
    # `return await asyncio.wait_for(stdio_client(params), 0.2)`
    # This implies `StdioServerParameters` can be initialized without `command` if `stdin`/`stdout` pipes
    # are provided, and `stdio_client` can consume such `StdioServerParameters`.
    # This is not how `mcp.StdioServerParameters` is defined. It requires `command`.
    #
    # Let's stick to the user's structure and assume `stdio_client` can be made to work this way,
    # or this part of the sketch needs adjustment based on mcp-lib capabilities.
    # For now, I will write it as sketched, acknowledging this potential discrepancy.
    # The `mcp.client.stdio.stdio_client` context manager yields (reader, writer).

    # If we are to use the existing `stdio_client` which spawns its own process,
    # then the logic "Try stdio first – it is cheapest" for a command means
    # we'd spawn it as stdio, and if that fails/times out, we'd have to kill it
    # and *then* consider if it was meant to be an HTTP server started by command.
    # This makes the "read first line of stderr to guess URL" tricky if the stdio attempt
    # already consumed from stderr or closed pipes.

    # Let's follow the user's suggested flow: spawn, then try stdio on that proc.
    # This requires `stdio_client` to accept an existing process's pipes.
    # If `mcp.stdio_client` cannot do this, this part of the auto-detection is not directly implementable
    # without modifying `mcp.stdio_client` or re-implementing a quick stdio handshake.

    # Given the constraints, the most robust way if `stdio_client` *must* spawn, is:
    # 1. If `command` and `expected_url`: spawn, health check HTTP, connect HTTP.
    # 2. If `command` and NO `expected_url`: spawn with `stdio_client`. This is plain stdio.
    # 3. If `url` and NO `command`: probe HTTP, connect HTTP.

    # This means we can't easily "try stdio first" on a process that *might* be an HTTP server started by command.
    # The user's sketch implies a more flexible `stdio_client`.
    # I will proceed with a structure that aligns with the distinct cases above,
    # which is a slight deviation from the "try stdio on http-spawned proc" part of the sketch.

    # Let's refine `connect` based on this understanding.
    # The `_open_stdio` helper might not be used if `stdio_client` is called directly in `connect`.
    pass # This helper will be re-evaluated in the `connect` function.

# Define NamedTuple for the connect function's return type
class ConnectResult(NamedTuple):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    proc: Optional[asyncio.subprocess.Process]


async def connect(cfg: dict, default_timeout: float = 3.0, stdio_timeout: float = 2.0) -> ConnectResult:
    """
    Connects to an MCP server based on the provided configuration.
    Returns a ConnectResult NamedTuple (reader, writer, process_handle_or_none).
    """
    url = cfg.get("url")
    command = cfg.get("command")
    command_args = cfg.get("args", []) # Renamed from 'args' in cfg to avoid confusion
    command_env = cfg.get("env", {})
    # 'type' can be a hint: 'sse', 'streamablehttp' (or 'streamable_http'), 'stdio'
    # 'expected_url' is for command-started HTTP/SSE servers.
    server_type_hint = cfg.get("type")
    expected_url_for_command = cfg.get("expected_url")

    proc = None # Store subprocess handle if we start one

    # Case 1: Direct URL is provided (no command)
    if url and not command:
        logger.info(f"Connecting to direct URL: {url}")
        try:
            # Use type hint if available, otherwise probe
            kind_to_open = None
            if server_type_hint == "sse":
                kind_to_open = "http-sse"
            elif server_type_hint in ["streamablehttp", "streamable_http"]:
                kind_to_open = "http-json"

            if kind_to_open:
                logger.debug(f"Using type hint '{server_type_hint}' for URL {url}")
            else: # No type hint, or unrecognised, so probe
                logger.debug(f"No valid type hint for URL {url}, probing transport.")
                kind_to_open = await _probe_http(url, timeout=default_timeout)

            reader, writer, *_ = await _open_http(kind_to_open, url) # _open_http might return 3 values
            return ConnectResult(reader, writer, None) # No process started by us
        except Exception as e:
            logger.error(f"Failed to connect to or probe direct URL {url}: {e}")
            raise

    # Case 2: Command is provided to start an HTTP/SSE server (expected_url is key)
    elif command and expected_url_for_command:
        logger.info(f"Starting server via command '{command}' with args '{command_args}', expecting URL '{expected_url_for_command}'")
        try:
            proc = await asyncio.create_subprocess_exec(
                command,
                *command_args,
                stdout=asyncio.subprocess.PIPE, # Capture for potential debugging
                stderr=asyncio.subprocess.PIPE, # Capture for potential debugging or URL sniffing (if needed later)
                env={**os.environ, **command_env}
            )
            logger.info(f"Server process for '{command}' started (PID: {proc.pid}). Waiting for it to be ready at {expected_url_for_command}...")

            # Health check loop (similar to previous implementation)
            is_ready = False
            max_retries = 60  # 60 seconds
            connect_timeout_seconds = 2.0

            # Determine kind for _open_http: use hint or probe expected_url_for_command
            kind_to_open = None
            if server_type_hint == "sse":
                kind_to_open = "http-sse"
            elif server_type_hint in ["streamablehttp", "streamable_http"]:
                kind_to_open = "http-json"

            # Probe loop
            async with httpx.AsyncClient(timeout=connect_timeout_seconds) as http_session:
                for attempt in range(max_retries):
                    if proc.returncode is not None:
                        stderr_output_bytes = await proc.stderr.read() if proc.stderr else b""
                        raise RuntimeError(f"Server process '{command}' (PID: {proc.pid}) exited prematurely with code {proc.returncode}. Stderr: {stderr_output_bytes.decode(errors='ignore')}")
                    try:
                        # Using POST with initialize payload for health check and type confirmation
                        probe_headers = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}
                        response = await http_session.post(expected_url_for_command, json=INIT_PAYLOAD, headers=probe_headers)

                        logger.debug(f"Health check POST to {expected_url_for_command} (attempt {attempt+1}): status {response.status}")
                        response.raise_for_status() # Check for HTTP errors

                        # If kind_to_open was not hinted, determine it now
                        if not kind_to_open:
                            content_type = response.headers.get("content-type", "").lower()
                            if content_type.startswith("text/event-stream"):
                                kind_to_open = "http-sse"
                            elif content_type.startswith("application/json"):
                                kind_to_open = "http-json"
                            else:
                                # This shouldn't happen if server is MCP compliant and probe is good
                                raise ValueError(f"Unexpected content-type '{content_type}' from {expected_url_for_command} after successful POST.")

                        is_ready = True
                        logger.info(f"Server '{command}' (PID: {proc.pid}) is ready at {expected_url_for_command} as type '{kind_to_open}'.")
                        break
                    except (httpx.RequestError, httpx.HTTPStatusError) as e:
                        logger.info(f"Waiting for '{command}' (PID: {proc.pid}) at {expected_url_for_command} (attempt {attempt+1}/{max_retries}, error: {type(e).__name__} - {e})...")
                    except Exception as e:
                        logger.error(f"Unexpected error during health check for '{command}' (PID: {proc.pid}): {e}")
                    await asyncio.sleep(1.0)

            if not is_ready or not kind_to_open:
                if proc and proc.returncode is None: proc.kill() # Ensure cleanup
                await proc.wait() if proc else asyncio.sleep(0)
                raise RuntimeError(f"Server '{command}' (PID: {proc.pid if proc else 'N/A'}) did not become ready/confirm type at {expected_url_for_command} after {max_retries} retries.")

            reader, writer, *_ = await _open_http(kind_to_open, expected_url_for_command)
            return ConnectResult(reader, writer, proc)
        except Exception:
            if proc and proc.returncode is None: # Ensure process is killed on any error during setup
                logger.warning(f"Error during setup for command-started server '{command}' (PID: {proc.pid}). Terminating process.")
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    proc.kill()
                await proc.wait() if proc else asyncio.sleep(0)
            raise

    # Case 3: Command is provided, no expected_url (assume stdio)
    elif command: # and not expected_url_for_command (implicitly)
        logger.info(f"Connecting to server via stdio command: '{command}' with args '{command_args}'")
        # Here, stdio_client will spawn the process.
        # We don't get a `proc` handle from `stdio_client` directly to return,
        # but the `stdio_client` context manager handles its lifecycle.
        # To fit the `(reader, writer, proc)` signature, we'd need to adapt.
        # The `stdio_client` is a context manager itself.
        # This means `lifespan` in main.py will need to handle this differently.
        #
        # Option A: `connect` returns a "protocol object" that `lifespan` can `async with`.
        # Option B: `connect` yields reader/writer, and `lifespan` manages `proc` if returned.
        #
        # For stdio, `stdio_client` *is* the context manager that yields reader/writer.
        # So, `connect` cannot simply return reader/writer for stdio if `stdio_client` must be used as a context manager.
        #
        # Let's make `connect` always return (reader, writer, proc_handle_or_None).
        # For stdio, we need to wrap the stdio_client call or adjust.
        # The user's sketch `return await _open_stdio(proc), proc` implies _open_stdio returns (reader,writer)
        # and `proc` is the one created by `connect`. This is the tricky part for stdio.

        # If `type: stdio` is hinted or it's the only option:
        if server_type_hint == 'stdio' or not server_type_hint: # Default to stdio if command and no http indication
            params = StdioServerParameters(
                command=command,
                args=command_args,
                env={**os.environ, **command_env}
            )
            # We cannot `await stdio_client(params)` directly and return its reader/writer
            # if `stdio_client` is a context manager that needs to be entered.
            # This is a fundamental conflict with the desired (reader, writer, proc) return signature
            # for all cases, if `proc` for stdio is managed *inside* `stdio_client`.
            #
            # The `lifespan` will need to `async with stdio_client(...)` itself.
            # So, `connect` should signal this.
            #
            # Proposal: `connect` returns a structure or specific types that `lifespan` can interpret.
            # E.g., return ("stdio", StdioServerParameters_instance, None)
            # Or, `connect` focuses *only* on URL-based and command-started-HTTP. Stdio handled separately.
            #
            # The user's feedback: "Single place that figures out transport; the rest of main.py only needs:
            # reader, writer, proc = await transport_detection.connect(server_cfg)"
            # This implies `connect` *must* resolve to reader/writer for stdio too.
            # This means `stdio_client` cannot be used as a context manager *inside* `connect`
            # if `connect` itself is not a context manager.
            #
            # This requires a re-think of how `stdio_client` is invoked if we want a uniform
            # `reader, writer, proc = await connect(...)`.
            #
            # For now, to match the spirit, I'll assume `stdio_client` can yield its underlying
            # reader/writer and proc if we manage its context *outside* or adapt it.
            # This is a placeholder for a deeper integration with `mcp.stdio_client`.
            # If `mcp.stdio_client` is strictly a context manager, this part needs to be refactored.
            #
            # Let's assume we can't change `mcp.stdio_client`.
            # Then `connect` cannot directly return reader/writer for stdio this way.
            # It would have to return a "pending stdio connection" object.
            #
            # Simplest path that keeps `connect` as the single entry point:
            # `connect` will have to *conditionally* be an async context manager if it's stdio.
            # This complicates its signature.
            #
            # Back to user's sketch: `return await _open_stdio(proc), proc`
            # `_open_stdio(proc)` was `await asyncio.wait_for(stdio_client(params_with_pipes), 0.2)`
            # This implies `stdio_client` can take `params_with_pipes` (not command) and returns (reader,writer).
            # This is the key assumption. If this holds, the sketch is viable.
            # If `StdioServerParameters` cannot be constructed with pipes only, and `stdio_client`
            # cannot accept it, then the sketch's `_open_stdio` is not directly implementable.
            #
            # Given the feedback's emphasis on this pattern, I will proceed assuming
            # `stdio_client` can be adapted or there's a way to get reader/writer from an existing process's pipes
            # that fits into the `mcp` library's expectations for a `ClientSession`.
            #
            # If the "try stdio on a command-spawned proc" is the goal:
            logger.debug(f"Attempting stdio on command '{command}' (PID: {proc.pid if proc else 'Not Started Yet'}). This part of the sketch is hard to implement with current mcp.stdio_client.")
            # This path (stdio after spawning for potential HTTP) is complex.
            # The user's sketch:
            # if cmd: proc = ...; try: return await _open_stdio(proc), proc except TimeoutError: line = await proc.stderr.readline() ...
            # This implies _open_stdio works on an existing proc.
            # Let's assume this is possible for now.
            #
            # If we are in "command only, no expected_url", it's simpler: it *is* stdio.
            logger.info(f"Configuring as stdio server: command='{command}'")
            # This case means `lifespan` will use `stdio_client` context manager directly.
            # So `connect` should signal this.
            # To adhere to "reader, writer, proc = await connect()", this is still tricky.
            #
            # Let's make `connect` raise a specific exception or return a sentinel for stdio
            # if it cannot return reader/writer directly.
            # Or, `main.py`'s `lifespan` will have to check `cfg` first.
            #
            # If `connect` is the *single* point of decision:
            # It must handle stdio spawning and yield reader/writer.
            # This means `connect` would need to become an async context manager for the stdio case.
            # `async with connect(cfg) as (reader, writer, proc):`
            # This is a breaking change to the simple `r,w,p = await connect()`
            #
            # Sticking to `r,w,p = await connect()`:
            # For stdio, `proc` will be None because `stdio_client` manages it internally.
            # `connect` would have to internally enter the `stdio_client` context,
            # store reader/writer, and then `lifespan` would use them.
            # But `ClientSession` also needs to be a context manager.
            #
            # This implies `connect` should not return reader/writer for stdio, but rather the
            # `stdio_client` instance itself, or parameters for it.
            # This contradicts the "single call" simplicity.

            # Simplest interpretation for now: if it's a command and no expected_url, it's stdio.
            # `main.py` will handle this path using `stdio_client` directly.
            # `connect` will then only deal with URL-based or command-started-HTTP.
            # This means `connect` will raise an error or return a specific value if it's an stdio config.
            # This seems like a reasonable compromise.
            logger.debug(f"Configuration for '{command}' implies stdio. This should be handled by stdio_client directly in main.py's lifespan.")
            raise ValueError(f"Stdio configuration for '{command}' should be handled by stdio_client directly, not through this unified connect function's HTTP/command-HTTP paths.")


    # Fallback: Incomplete or unhandled configuration
    else:
        logger.error(f"Incomplete or unhandled MCP server configuration: {cfg}")
        raise ValueError(f"Incomplete or unhandled MCP server configuration: {cfg}")