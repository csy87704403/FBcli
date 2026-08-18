import { createInterface } from 'node:readline'

import { getFreebuffBase3RootAgentIdForModel } from '@codebuff/common/constants/free-agents'
import { DEFAULT_FREEBUFF_MODEL_ID } from '@codebuff/common/constants/freebuff-models'
import { publishedTools, toolNames } from '@codebuff/common/tools/constants'
import {
  CodebuffClient,
  type CustomToolDefinition,
  type MessageContent,
  type RunState,
} from '@codebuff/sdk'
import z from 'zod/v4'

import { getAuthTokenDetails } from '../../cli/src/utils/auth'
import { bundledAgents } from '../../cli/src/agents/bundled-agents.generated'
import { serializeForPersistence } from '../../cli/src/utils/safe-json'
import {
  callFreebuffSession,
  type FreebuffSessionMethod,
} from '../../cli/src/utils/freebuff-session-api'

type JsonObject = Record<string, unknown>

type Request = {
  id?: string
  type?: 'ping' | 'chat' | 'tool_result' | 'reset' | 'shutdown'
  session_id?: string
  model?: string
  prompt?: string
  message_content?: MessageContent[]
  cwd?: string
  tools?: ToolSpec[]
  tool_call_id?: string
  content?: unknown
  is_error?: boolean
}

type ToolSpec = {
  name: string
  description?: string
  parameters?: unknown
}

type PendingTool = {
  resolve: (value: { content: unknown; isError: boolean }) => void
  reject: (error: Error) => void
}

const runs = new Map<string, RunState>()
const pendingTools = new Map<string, PendingTool>()
let activeSession:
  | { instanceId: string; model: string; expiresAt: string }
  | undefined
let requestChain = Promise.resolve()

for (const method of ['log', 'warn', 'error', 'info', 'debug'] as const) {
  console[method] = (...args: unknown[]) => {
    process.stderr.write(`${args.map(String).join(' ')}\n`)
  }
}

function emit(value: JsonObject): void {
  process.stdout.write(`${JSON.stringify(value)}\n`)
}

function requestError(id: string, error: unknown): void {
  emit({
    id,
    type: 'error',
    message: error instanceof Error ? error.message : String(error),
  })
}

function outputText(output: RunState['output']): string {
  if (output.type !== 'lastMessage' && output.type !== 'allMessages') return ''
  for (let i = output.value.length - 1; i >= 0; i--) {
    const message = output.value[i] as { role?: unknown; content?: unknown }
    if (message?.role !== 'assistant') continue
    if (typeof message.content === 'string') return message.content
    if (!Array.isArray(message.content)) continue
    const parts: string[] = []
    for (const part of message.content) {
      if (
        typeof part === 'object' &&
        part !== null &&
        'type' in part &&
        part.type === 'text' &&
        'text' in part
      ) {
        parts.push(String(part.text))
      }
    }
    if (parts.length > 0) return parts.join('\n')
  }
  return ''
}

function parseToolResultContent(content: unknown): unknown {
  if (typeof content !== 'string') return content
  try {
    return JSON.parse(content)
  } catch {
    return content
  }
}

function relayTool(
  requestId: string,
  tool: ToolSpec,
  input: unknown,
): Promise<{ content: unknown; isError: boolean }> {
  const toolCallId = `call_cli_${crypto.randomUUID()}`
  return new Promise((resolve, reject) => {
    pendingTools.set(toolCallId, { resolve, reject })
    emit({
      id: requestId,
      type: 'tool_call',
      tool_call_id: toolCallId,
      name: tool.name,
      arguments: input,
    })
  })
}

function buildToolBridge(requestId: string, tools: ToolSpec[]) {
  const byName = new Map(
    tools.filter((tool) => tool.name).map((tool) => [tool.name, tool]),
  )
  const unavailable = (name: string) => [
    {
      type: 'json' as const,
      value: {
        errorMessage: `Tool ${name} is not exposed by the external caller. Use only the explicitly supplied external tools.`,
      },
    },
  ]
  const overrides: Record<string, (input: any) => Promise<any>> = {}
  for (const name of publishedTools) {
    if (name === 'read_files') {
      overrides[name] = async (input: {
        paths?: Array<string | { path?: string; offset?: number; limit?: number }>
      }) => {
        const entries = input.paths ?? []
        const paths = entries
          .map((entry) =>
            typeof entry === 'string' ? entry : entry?.path?.trim() || '',
          )
          .filter(Boolean)
        const exact = byName.get('read_files')
        if (exact) {
          const result = await relayTool(requestId, exact, input)
          const value = parseToolResultContent(result.content)
          if (Array.isArray(value)) {
            return [{ type: 'json' as const, value }]
          }
          if (value && typeof value === 'object') {
            const files = Object.entries(value).map(([path, content]) => ({
              path,
              content: String(content ?? ''),
            }))
            return [{ type: 'json' as const, value: files }]
          }
          return [
            {
              type: 'json' as const,
              value: paths.map((path) => ({
                path,
                content: String(value ?? ''),
              })),
            },
          ]
        }

        const single = byName.get('read_file')
        if (!single) {
          return [
            {
              type: 'json' as const,
              value: paths.map((path) => ({
                path,
                content:
                  'Error: the external caller exposes neither read_files nor read_file.',
              })),
            },
          ]
        }
        const files = []
        for (const entry of entries) {
          const path = typeof entry === 'string' ? entry : entry?.path?.trim() || ''
          if (!path) continue
          const result = await relayTool(requestId, single, { path })
          const value = parseToolResultContent(result.content)
          files.push({
            path,
            content: result.isError
              ? `Error: ${String(value ?? 'read_file failed')}`
              : String(value ?? ''),
          })
        }
        return [{ type: 'json' as const, value: files }]
      }
      continue
    }
    if (name === 'str_replace') {
      overrides[name] = async (input: {
        path?: string
        replacements?: Array<{
          oldString?: string
          newString?: string
          allowMultiple?: boolean
        }>
      }) => {
        const path = input.path?.trim() || ''
        const exact = byName.get('str_replace')
        if (exact) {
          const result = await relayTool(requestId, exact, input)
          const value = parseToolResultContent(result.content)
          return [
            {
              type: 'json' as const,
              value: result.isError
                ? { file: path, errorMessage: String(value) }
                : { file: path, message: String(value ?? 'updated') },
            },
          ]
        }

        const edit = byName.get('edit_file')
        if (!edit) return unavailable(name)
        const messages: string[] = []
        for (const replacement of input.replacements ?? []) {
          const result = await relayTool(requestId, edit, {
            path,
            old_string: replacement.oldString ?? '',
            new_string: replacement.newString ?? '',
          })
          const value = parseToolResultContent(result.content)
          if (result.isError) {
            return [
              {
                type: 'json' as const,
                value: { file: path, errorMessage: String(value) },
              },
            ]
          }
          messages.push(String(value ?? 'updated'))
        }
        return [
          {
            type: 'json' as const,
            value: { file: path, message: messages.join('\n') || 'updated' },
          },
        ]
      }
      continue
    }
    overrides[name] = async (input: unknown) => {
      const spec = byName.get(name)
      if (!spec) return unavailable(name)
      const result = await relayTool(requestId, spec, input)
      const value = parseToolResultContent(result.content)
      if (result.isError) {
        return [{ type: 'json', value: { errorMessage: String(value) } }]
      }
      return [{ type: 'json', value }]
    }
  }

  const customToolDefinitions = tools
    .filter((tool) => !toolNames.includes(tool.name as never))
    .map(
      (tool): CustomToolDefinition => ({
        toolName: tool.name,
        inputSchema: z.record(z.string(), z.any()),
        description:
          (tool.description || `External caller tool ${tool.name}`) +
          `\nInput JSON schema: ${JSON.stringify(tool.parameters ?? {})}`,
        endsAgentStep: true,
        exampleInputs: [],
        execute: async (input) => {
          const result = await relayTool(requestId, tool, input)
          const value = parseToolResultContent(result.content)
          if (result.isError) {
            return [{ type: 'json', value: { errorMessage: String(value) } }]
          }
          return [{ type: 'json', value: value as any }]
        },
      }),
    )
  return { overrides, customToolDefinitions }
}

function requireAuthToken(): string {
  const { token } = getAuthTokenDetails()
  if (!token) {
    throw new Error('Freebuff CLI is not logged in; run `freebuff login` first')
  }
  return token
}

async function callSession(
  method: FreebuffSessionMethod,
  token: string,
  model?: string,
) {
  return callFreebuffSession(method, token, {
    model,
    instanceId: activeSession?.instanceId,
  })
}

async function ensureFreebuffSession(requestedModel: string) {
  const token = requireAuthToken()

  if (activeSession) {
    const current = await callSession('GET', token)
    if (
      current.status === 'active' &&
      current.instanceId === activeSession.instanceId
    ) {
      activeSession = current
      if (current.model !== requestedModel) {
        throw new Error(
          `active Freebuff session is locked to ${current.model}; requested ${requestedModel}`,
        )
      }
      return { token, session: current }
    }
    activeSession = undefined
  }

  const admitted = await callSession('POST', token, requestedModel)
  if (admitted.status !== 'active') {
    throw new Error(`Freebuff session admission returned ${admitted.status}`)
  }
  activeSession = admitted
  return { token, session: admitted }
}

async function handleChat(id: string, request: Request): Promise<void> {
  const prompt = request.prompt?.trim()
  if (!prompt) throw new Error('prompt is required')

  const sessionId = request.session_id?.trim() || 'default'
  const model = request.model?.trim() || DEFAULT_FREEBUFF_MODEL_ID
  const { token, session } = await ensureFreebuffSession(model)
  const toolBridge = buildToolBridge(id, request.tools ?? [])
  const rootAgentId = getFreebuffBase3RootAgentIdForModel(session.model)
  const bundledRootAgent = bundledAgents[rootAgentId]
  if (!bundledRootAgent) {
    throw new Error(`missing bundled Freebuff root agent: ${rootAgentId}`)
  }
  // Base3 Freebuff roots are single-loop agents with no spawnable subagents.
  // The gateway does not use the CLI's local-agent or MCP registries, so avoid
  // loading and validating every bundled and user-defined agent.
  const rootAgent = { ...bundledRootAgent }
  const agentDefinitions = [rootAgent]
  if (rootAgent && request.tools?.length) {
    rootAgent.toolNames = Array.from(
      new Set([
        ...(rootAgent.toolNames ?? []),
        ...request.tools.map((tool) => tool.name),
      ]),
    )
  }
  const client = new CodebuffClient({
    apiKey: token,
    cwd: request.cwd || process.cwd(),
    agentDefinitions,
    customToolDefinitions: toolBridge.customToolDefinitions,
    overrideTools: toolBridge.overrides,
  })

  emit({
    id,
    type: 'start',
    session_id: sessionId,
    model: session.model,
  })

  const run = await client.run({
    agent: rootAgentId,
    prompt,
    content: request.message_content,
    previousRun: runs.get(sessionId),
    costMode: 'free',
    extraCodebuffMetadata: { freebuff_instance_id: session.instanceId },
    handleStreamChunk: async (chunk) => {
      if (typeof chunk === 'string') {
        emit({ id, type: 'delta', text: chunk })
      }
    },
    handleEvent: async (event) => {
      if (event.type === 'error') {
        emit({ id, type: 'upstream_error', message: event.message })
      }
    },
  })

  const persistedRun = JSON.parse(
    serializeForPersistence(run).json,
  ) as RunState
  runs.set(sessionId, persistedRun)
  if (run.output.type === 'error') {
    throw new Error(run.output.message)
  }
  emit({
    id,
    type: 'result',
    session_id: sessionId,
    text: outputText(run.output),
  })
}

async function releaseSession(): Promise<void> {
  if (!activeSession) return
  const token = requireAuthToken()
  try {
    await callSession('DELETE', token)
  } finally {
    activeSession = undefined
  }
}

async function handleRequest(request: Request): Promise<void> {
  const id = request.id?.trim() || crypto.randomUUID()
  switch (request.type) {
    case 'ping':
      emit({ id, type: 'pong', authenticated: Boolean(getAuthTokenDetails().token) })
      return
    case 'reset':
      runs.delete(request.session_id?.trim() || 'default')
      emit({ id, type: 'reset_ok' })
      return
    case 'tool_result': {
      const toolCallId = request.tool_call_id?.trim() || ''
      const pending = pendingTools.get(toolCallId)
      if (!pending) throw new Error(`unknown tool_call_id: ${toolCallId}`)
      pendingTools.delete(toolCallId)
      pending.resolve({ content: request.content, isError: request.is_error === true })
      return
    }
    case 'shutdown':
      await releaseSession()
      emit({ id, type: 'shutdown_ok' })
      process.exitCode = 0
      return
    case 'chat':
      await handleChat(id, request)
      return
    default:
      throw new Error('type must be ping, chat, tool_result, reset, or shutdown')
  }
}

const lines = createInterface({ input: process.stdin, crlfDelay: Infinity })
emit({ type: 'ready', protocol: 'freebuff-headless-jsonl-v1' })

for await (const line of lines) {
  const trimmed = line.trim()
  if (!trimmed) continue
  let request: Request
  try {
    request = JSON.parse(trimmed) as Request
  } catch {
    requestError('', new Error('invalid JSON line'))
    continue
  }

  if (request.type === 'tool_result') {
    try {
      await handleRequest(request)
    } catch (error) {
      requestError(request.id?.trim() || '', error)
    }
    continue
  }
  requestChain = requestChain.then(async () => {
    try {
      await handleRequest(request)
    } catch (error) {
      requestError(request.id?.trim() || '', error)
    } finally {
      if (request.type === 'chat') Bun.gc(true)
    }
  })
  if (process.exitCode !== undefined) break
}

if (activeSession) {
  await releaseSession().catch(() => {})
}
