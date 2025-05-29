import json
import os
import logging
import socket
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Optional, Dict, Any, AsyncGenerator

import uvicorn
import asyncio
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from .transport_detection import connect as transport_connect
from starlette.routing import Mount

logger = logging.getLogger(__name__)


from mcpo.utils.main import get_model_fields, get_tool_handler
from mcpo.utils.auth import get_verify_api_key, APIKeyMiddleware


async def create_dynamic_endpoints(app: FastAPI, api_dependency=None):
    session: ClientSession = app.state.session
    if not session:
        raise ValueError("Session is not initialized in the app state.")

    result = await session.initialize()
    server_info = getattr(result, "serverInfo", None)
    if server_info:
        app.title = server_info.name or app.title
        app.description = (
            f"{server_info.name} MCP Server" if server_info.name else app.description
        )
        app.version = server_info.version or app.version

    tools_result = await session.list_tools()
    tools = tools_result.tools

    for tool in tools:
        endpoint_name = tool.name
        endpoint_description = tool.description

        inputSchema = tool.inputSchema
        outputSchema = getattr(tool, "outputSchema", None)

        form_model_fields = get_model_fields(
            f"{endpoint_name}_form_model",
            inputSchema.get("properties", {}),
            inputSchema.get("required", []),
            inputSchema.get("$defs", {}),
        )

        response_model_fields = None
        if outputSchema:
            response_model_fields = get_model_fields(
                f"{endpoint_name}_response_model",
                outputSchema.get("properties", {}),
                outputSchema.get("required", []),
                outputSchema.get("$defs", {}),
            )

        tool_handler = get_tool_handler(
            session,
            endpoint_name,
            form_model_fields,
            response_model_fields,
        )

        app.post(
            f"/{endpoint_name}",
            summary=endpoint_name.replace("_", " ").title(),
            description=endpoint_description,
            response_model_exclude_none=True,
            dependencies=[Depends(api_dependency)] if api_dependency else [],
        )(tool_handler)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    mcp_config: Optional[Dict[str, Any]] = getattr(app.state, "mcp_config", None)
    api_dependency = getattr(app.state, "api_dependency", None)

    # This is for the main app that mounts sub-apps, or if mcp_config is missing for a sub-app
    if not mcp_config:
        logger.debug("Main app lifespan or missing mcp_config, managing sub-app lifespans.")
        async with AsyncExitStack() as stack:
            for route in app.routes:
                if isinstance(route, Mount) and isinstance(route.app, FastAPI):
                    await stack.enter_async_context(
                        route.app.router.lifespan_context(route.app),
                    )
            yield
        return

    # Pure stdio: has 'command', does not have 'url', does not have 'expected_url'
    is_pure_stdio = bool(
        mcp_config.get("command") and
        not mcp_config.get("url") and
        not mcp_config.get("expected_url")
    )

    server_proc: Optional[asyncio.subprocess.Process] = None

    if is_pure_stdio:
        logger.info(f"Connecting to stdio server with config: {mcp_config}")
        stdio_command = mcp_config["command"]
        stdio_args = mcp_config.get("args", [])
        stdio_env = {**os.environ, **mcp_config.get("env", {})}

        server_params = StdioServerParameters(
            command=stdio_command,
            args=stdio_args,
            env=stdio_env,
        )
        # stdio_client is a context manager that handles process lifecycle
        async with stdio_client(server_params) as (reader, writer):
            async with ClientSession(reader, writer) as session:
                app.state.session = session
                await create_dynamic_endpoints(app, api_dependency=api_dependency)
                yield
        # Process is managed by stdio_client's context
    else:
        # HTTP/SSE (direct URL or command-started)
        logger.info(f"Attempting to connect/start server with config: {mcp_config}")
        reader = None
        writer = None
        try:
            # transport_connect will handle probing, starting process if needed, and returning reader/writer/proc
            connect_result = await transport_connect(mcp_config)
            context_manager = connect_result.context_manager
            server_proc = connect_result.proc  # transport_connect returns proc if it started one

            if context_manager:
                async with context_manager as (reader, writer, *_):
                    async with ClientSession(reader, writer) as session:
                        app.state.session = session
                        await create_dynamic_endpoints(app, api_dependency=api_dependency)
                        yield
            else:
                # Should not be reached if transport_connect behaves correctly
                logger.error(f"Failed to obtain context manager from transport_connect for config: {mcp_config}")
                raise RuntimeError(f"Failed to connect using transport_detection for config: {mcp_config}")

        finally:
            if server_proc and server_proc.returncode is None:
                logger.info(f"Terminating server process (PID: {server_proc.pid}) started by transport_connect...")
                server_proc.terminate()
                try:
                    await asyncio.wait_for(server_proc.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning(f"Server process (PID: {server_proc.pid}) did not terminate gracefully, killing.")
                    server_proc.kill()
                await server_proc.wait()  # Ensure it's reaped
                logger.info(f"Server process (PID: {server_proc.pid}) terminated.")
            elif server_proc:  # Process already exited
                logger.info(f"Server process (PID: {server_proc.pid}) had already exited with code {server_proc.returncode}.")


async def run(
    host: str = "127.0.0.1",
    port: int = 8000,
    api_key: Optional[str] = "",
    cors_allow_origins=["*"],
    **kwargs,
):
    # Server API Key
    api_dependency = get_verify_api_key(api_key) if api_key else None
    strict_auth = kwargs.get("strict_auth", False)

    server_type = kwargs.get("server_type")
    server_command = kwargs.get("server_command")
    config_path = kwargs.get("config_path")

    # mcpo server
    name = kwargs.get("name") or "MCP OpenAPI Proxy"
    description = (
        kwargs.get("description") or "Automatically generated API from MCP Tool Schemas"
    )
    version = kwargs.get("version") or "1.0"

    ssl_certfile = kwargs.get("ssl_certfile")
    ssl_keyfile = kwargs.get("ssl_keyfile")
    path_prefix = kwargs.get("path_prefix") or "/"

    # Configure basic logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger.info(
        f"Starting MCPO Server:\n"
        f"  Name: {name}\n"
        f"  Version: {version}\n"
        f"  Description: {description}\n"
        f"  Hostname: {socket.gethostname()}\n"
        f"  Port: {port}\n"
        f"  API Key: {'Provided' if api_key else 'Not Provided'}\n"
        f"  CORS Allowed Origins: {cors_allow_origins}\n"
        f"  SSL Certificate: {ssl_certfile or 'None'}\n"
        f"  SSL Key: {ssl_keyfile or 'None'}\n"
        f"  Path Prefix: {path_prefix}"
    )

    main_app = FastAPI(
        title=name,
        description=description,
        version=version,
        ssl_certfile=ssl_certfile,
        ssl_keyfile=ssl_keyfile,
        lifespan=lifespan,
    )

    main_app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_allow_origins or ["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Add middleware to protect documentation and spec
    if api_key and strict_auth:
        main_app.add_middleware(APIKeyMiddleware, api_key=api_key)

    if server_type == "sse":
        logger.info(f"Configuring for a single SSE MCP Server with URL {server_command[0]}")
        main_app.state.server_type = "sse"
        main_app.state.args = server_command[0]  # Expects URL as the first element
        main_app.state.api_dependency = api_dependency
    elif server_type in ["streamablehttp", "streamable_http", "streamable-http"]:
        logger.info(f"Configuring for a single StreamableHTTP MCP Server with URL {server_command[0]}")
        main_app.state.server_type = "streamablehttp"
        main_app.state.args = server_command[0]  # Expects URL as the first element
        main_app.state.api_dependency = api_dependency
    elif server_command:  # This handles stdio
        logger.info(f"Configuring for a single Stdio MCP Server with command: {' '.join(server_command)}")
        main_app.state.server_type = "stdio"
        main_app.state.command = server_command[0]
        main_app.state.args = server_command[1:]
        main_app.state.env = os.environ.copy()
        main_app.state.api_dependency = api_dependency
    elif config_path:
        logger.info(f"Loading MCP server configurations from: {config_path}")
        with open(config_path, "r") as f:
            config_data = json.load(f)

        mcp_servers = config_data.get("mcpServers", {})
        if not mcp_servers:
            logger.error(f"No 'mcpServers' found in config file: {config_path}")
            raise ValueError("No 'mcpServers' found in config file.")

        logger.info("Configured MCP Servers:")

        for server_name_cfg, server_cfg_details in mcp_servers.items():
            log_parts = [f"  Preparing MCP Server '{server_name_cfg}'"]
            if server_cfg_details.get("type"):
                log_parts.append(f"with type hint '{server_cfg_details['type']}'")
            if server_cfg_details.get("command"):
                log_parts.append(f"via command '{server_cfg_details['command']}'")
            if server_cfg_details.get("url"):
                log_parts.append(f"at URL '{server_cfg_details['url']}'")
            if server_cfg_details.get("expected_url"):
                log_parts.append(f"expected at '{server_cfg_details['expected_url']}' after command start")
            logger.info(" ".join(log_parts))

        main_app.description += "\n\n- **available tools**："
        for server_name, server_cfg in mcp_servers.items():
            current_mcp_config = server_cfg.copy()

            # Handle nested options structure - flatten it
            if "options" in current_mcp_config:
                options = current_mcp_config.pop("options")
                current_mcp_config.update(options)

            # Basic validation: must have command or url
            if not current_mcp_config.get("command") and not current_mcp_config.get("url"):
                logger.warning(f"Skipping server '{server_name}': configuration must include 'command' or 'url'.")
                continue

            # Command-based HTTP/SSE requires 'expected_url'
            if (current_mcp_config.get("command") and
                current_mcp_config.get("type") in ["sse", "streamablehttp", "streamable_http", "streamable-http"] and
                not current_mcp_config.get("expected_url")):
                logger.warning(f"Skipping server '{server_name}': command-based HTTP/SSE server requires 'expected_url'.")
                continue

            sub_app = FastAPI(
                title=f"{server_name}",
                description=f"{server_name} MCP Server\n\n- [back to tool list](/docs)",
                version="1.0",
                lifespan=lifespan,  # This lifespan will now use mcp_config
            )

            sub_app.add_middleware(
                CORSMiddleware,
                allow_origins=cors_allow_origins or ["*"],
                allow_credentials=True,
                allow_methods=["*"],
                allow_headers=["*"],
            )

            sub_app.state.mcp_config = current_mcp_config
            sub_app.state.api_dependency = api_dependency

            # Add middleware to protect documentation and spec
            if api_key and strict_auth:
                sub_app.add_middleware(APIKeyMiddleware, api_key=api_key)

            main_app.mount(f"{path_prefix}{server_name}", sub_app)
            main_app.description += f"\n    - [{server_name}](/{server_name}/docs)"
    else:
        if server_command:  # server_command is from CLI args
            logger.info(f"Configuring for a single MCP Server from CLI arguments.")
            cli_mcp_config: Dict[str, Any] = {"name": "cli_server"}

            cli_mcp_config["type"] = server_type  # From CLI --type, defaults to "stdio"

            if server_type == "stdio":
                cli_mcp_config["command"] = server_command[0]
                cli_mcp_config["args"] = server_command[1:]
                cli_mcp_config["env"] = os.environ.copy()  # Ensure current env is passed for stdio
            elif server_type in ["sse", "streamablehttp", "streamable_http", "streamable-http"]:
                if not server_command or not isinstance(server_command[0], str):
                    raise ValueError(f"URL must be provided for {server_type} server via CLI.")
                cli_mcp_config["url"] = server_command[0]

            main_app.state.mcp_config = cli_mcp_config
            main_app.state.api_dependency = api_dependency
        else:
            logger.error("MCPO server_command or config_path must be provided.")
            raise ValueError("You must provide either server_command or config.")

    logger.info("Uvicorn server starting...")
    config = uvicorn.Config(
        app=main_app,
        host=host,
        port=port,
        ssl_certfile=ssl_certfile,
        ssl_keyfile=ssl_keyfile,
        log_level="info",
    )
    server = uvicorn.Server(config)

    await server.serve()
