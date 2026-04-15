local M = {}

local ns = vim.api.nvim_create_namespace("cmax")
vim.api.nvim_set_hl(0, "CmaxBoxActive", { fg = "#e0a020", bold = true })
vim.api.nvim_set_hl(0, "CmaxStatusActive", { fg = "#61afef", italic = true })
vim.api.nvim_set_hl(0, "CmaxStatusReady", { fg = "#98c379", italic = true })
vim.api.nvim_set_hl(0, "CmaxStatusAttention", { fg = "#e5c07b", italic = true })
vim.api.nvim_set_hl(0, "CmaxStatusError", { fg = "#e06c75", italic = true })
vim.api.nvim_set_hl(0, "CmaxStatusDead", { fg = "#5c6370", italic = true })

local state = {
   terminals = {},
   menu_buf = nil,
   win = nil,
   counter = 0,
   selected = 1,
   poll_timer = nil,
   status_lines = {},
   view = "menu",
   saved_sessions = {},
   filtered_terminals = {},
   tab_filter_query = nil,
   session_picker_return_to_menu = false,
   codex_claimed_sessions = {},
   codex_history_offset = 0,
   codex_latest_prompts = {},
   codex_prompt_history = {},
   codex_session_cache = {},
   claude_history_offset = 0,
   claude_latest_prompts = {},
   claude_prompt_history = {},
   launch_profiles = {},
   multiplexer_session_id = nil,
   multiplexer_created_at = nil,
   auto_heading_opts = nil,
   active_heading_job = nil,
   heading_backoff_until = 0,
   heading_failure_message = nil,
   tab_filter_opts = nil,
   active_tab_filter_job = nil,
   tab_filter_loading = false,
   tab_filter_error = nil,
   tab_filter_return_selected = nil,
   tab_filter_progress_done = 0,
   tab_filter_progress_total = 0,
   tab_filter_failure_count = 0,
}

local status_dir = (vim.env.TMPDIR or "/tmp") .. "/cmax-status"
local claude_hook_script_path = vim.fn.expand("~/.claude/hooks/cmax-status.py")
local claude_old_hook_script_path = vim.fn.expand("~/.claude/hooks/cmax-status.sh")
local claude_settings_path = vim.fn.expand("~/.claude/settings.json")
local claude_history_path = vim.fn.expand("~/.claude/history.jsonl")
local claude_sessions_path = vim.fn.expand("~/.claude/sessions")
local claude_projects_path = vim.fn.expand("~/.claude/projects")
local codex_hook_script_path = vim.fn.expand("~/.codex/hooks/cmax-status.py")
local codex_hooks_path = vim.fn.expand("~/.codex/hooks.json")
local codex_config_path = vim.fn.expand("~/.codex/config.toml")
local codex_history_path = vim.fn.expand("~/.codex/history.jsonl")
local codex_sessions_path = vim.fn.expand("~/.codex/sessions")
local cmax_state_dir = vim.fn.stdpath("state") .. "/cmax"
local cmax_sessions_dir = cmax_state_dir .. "/sessions"

local item_height = 5
local inner_height = item_height - 2

local HOOK_SCRIPT = [[#!/usr/bin/env python3
import json
import os
import sys


def atomic_write(path, text):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write(text)
    os.replace(tmp, path)


try:
    payload = json.load(sys.stdin)
except Exception:
    sys.exit(0)

cmax_id = os.environ.get("CMAX_ID")
if not cmax_id:
    sys.exit(0)

provider = os.environ.get("CMAX_PROVIDER", "")
status_dir = os.path.join(os.environ.get("TMPDIR", "/tmp"), "cmax-status")
os.makedirs(status_dir, exist_ok=True)

event = payload.get("hook_event_name", "")
notification_type = payload.get("notification_type", "")
status = None
prompt = None

if provider == "codex":
    if event == "SessionStart":
        status = "Starting..."
    elif event in {"UserPromptSubmit", "PreToolUse", "PostToolUse"}:
        status = "Working..."
    elif event == "Stop":
        status = "Waiting for input"
elif provider == "claude":
    status_map = {
        "Stop": "Waiting for input",
        "UserPromptSubmit": "Working...",
        "PermissionRequest": "Needs permission",
        "SessionStart": "Starting...",
        "SessionEnd": "Session ended",
        "PostToolUseFailure": "Tool error",
        "PreCompact": "Compacting...",
        "SubagentStart": "Working (subagent)...",
    }
    status = status_map.get(event)
    if event == "Notification":
        status = {
            "permission_prompt": "Needs permission",
            "idle_prompt": "Idle",
        }.get(notification_type)

if event == "UserPromptSubmit":
    prompt = payload.get("prompt")

if status:
    atomic_write(os.path.join(status_dir, cmax_id), status)

if isinstance(prompt, str) and prompt.strip():
    prompt = " ".join(prompt.split())[:80]
    atomic_write(os.path.join(status_dir, f"{cmax_id}.prompt"), prompt)

session_payload = {}
session_id = payload.get("session_id") or payload.get("sessionId")
if isinstance(session_id, str) and session_id:
    session_payload["session_id"] = session_id
transcript_path = payload.get("transcript_path")
if isinstance(transcript_path, str) and transcript_path:
    session_payload["transcript_path"] = transcript_path
source = payload.get("source")
if isinstance(source, str) and source:
    session_payload["source"] = source

if session_payload:
    atomic_write(
        os.path.join(status_dir, f"{cmax_id}.session"),
        json.dumps(session_payload),
    )
]]

local HOOK_EVENTS = {
   "SessionStart", "SessionEnd", "Stop", "UserPromptSubmit",
   "PermissionRequest", "SubagentStart", "Notification",
   "PostToolUseFailure", "PreCompact",
}

local STATUS_HL = {
   ["Working..."] = "CmaxStatusActive",
   ["Working (subagent)..."] = "CmaxStatusActive",
   ["Starting..."] = "CmaxStatusActive",
   ["Compacting..."] = "CmaxStatusActive",
   ["Waiting for input"] = "CmaxStatusReady",
   ["Idle"] = "CmaxStatusReady",
   ["Needs permission"] = "CmaxStatusAttention",
   ["Tool error"] = "CmaxStatusError",
   ["Session ended"] = "CmaxStatusDead",
}

local show_menu
local show_saved_sessions
local show_chat_search
local show_tab_filter_results
local prompt_tab_filter
local get_codex_session_meta
local normalize_search_text
local truncate_text
local provider_title
local provider_session_id
local pump_term_heading_jobs
local find_terminal
local rerender_active_view

local function read_file(path)
   local f = io.open(path, "r")
   if not f then return nil end
   local content = f:read("*a")
   f:close()
   return content
end

local function write_file(path, content)
   local dir = vim.fn.fnamemodify(path, ":h")
   local ok = pcall(vim.fn.mkdir, dir, "p")
   if not ok then return false end
   local f = io.open(path, "w")
   if not f then return false end
   f:write(content)
   f:close()
   return true
end

local function decode_json(text)
   if type(text) ~= "string" or text == "" then return nil end
   local ok, decoded = pcall(vim.json.decode, text)
   if ok then return decoded end
end

local function decode_json_line(line)
   return decode_json(line)
end

local function read_json_file(path)
   return decode_json(read_file(path))
end

local function clear_table(t)
   for key in pairs(t) do
      t[key] = nil
   end
end

local function sanitize_prompt(prompt)
   if type(prompt) ~= "string" then return nil end
   prompt = prompt:gsub("%s*\n%s*", " "):gsub("%s+", " ")
   prompt = vim.trim(prompt)
   if prompt == "" then return nil end
   if vim.fn.strdisplaywidth(prompt) > 80 then
      prompt = prompt:sub(1, 79) .. "..."
   end
   return prompt
end

local function sanitize_heading(heading)
   if type(heading) ~= "string" then return nil end
   heading = heading:gsub("\r", "")
   heading = heading:match("([^\n]+)") or heading
   heading = heading:gsub("^%s*heading%s*:%s*", "")
   heading = heading:gsub("^%s*[\"'`]", "")
   heading = heading:gsub("[\"'`]%s*$", "")
   heading = normalize_search_text(heading)
   if not heading then return nil end
   if vim.fn.strdisplaywidth(heading) > 80 then
      heading = truncate_text(heading, 80)
   end
   return heading
end

local function normalize_auto_heading_opts(opts)
   local defaults = {
      enabled = true,
      command = { "ollama" },
      host = "http://127.0.0.1:11434",
      model = "qwen2.5:1.5b",
      debounce_ms = 1500,
      keepalive = "2m",
      timeout_ms = 30000,
      max_context_chars = nil,
      max_output_tokens = 24,
   }

   opts = vim.tbl_deep_extend("force", defaults, opts or {})
   if type(opts.command) == "string" then
      opts.command = { opts.command }
   end
   if not vim.islist(opts.command) or #opts.command == 0 then
      opts.command = { "ollama" }
   end
   opts.enabled = not not opts.enabled
   opts.debounce_ms = math.max(0, tonumber(opts.debounce_ms) or defaults.debounce_ms)
   opts.timeout_ms = math.max(1000, tonumber(opts.timeout_ms) or defaults.timeout_ms)
   opts.host = tostring(opts.host or defaults.host):gsub("/+$", "")
   if opts.max_context_chars ~= nil then
      local max_context_chars = tonumber(opts.max_context_chars)
      if max_context_chars then
         opts.max_context_chars = math.max(1000, max_context_chars)
      else
         opts.max_context_chars = defaults.max_context_chars
      end
   end
   opts.max_output_tokens = math.max(8, tonumber(opts.max_output_tokens) or defaults.max_output_tokens)
   if opts.keepalive == false or opts.keepalive == "" then
      opts.keepalive = nil
   elseif opts.keepalive ~= nil then
      opts.keepalive = tostring(opts.keepalive)
   end
   return opts
end

local function normalize_tab_filter_opts(opts)
   local defaults = {
      enabled = true,
      command = { "ollama" },
      host = "http://127.0.0.1:11434",
      model = "qwen2.5:3b",
      debounce_ms = 0,
      keepalive = "2m",
      timeout_ms = 45000,
      max_context_chars_per_tab = nil,
      max_reason_chars = 90,
      parallel_jobs = 4,
      max_output_tokens = 32,
   }

   opts = vim.tbl_deep_extend("force", defaults, opts or {})
   if type(opts.command) == "string" then
      opts.command = { opts.command }
   end
   if not vim.islist(opts.command) or #opts.command == 0 then
      opts.command = { "ollama" }
   end
   opts.enabled = not not opts.enabled
   opts.debounce_ms = math.max(0, tonumber(opts.debounce_ms) or defaults.debounce_ms)
   opts.timeout_ms = math.max(1000, tonumber(opts.timeout_ms) or defaults.timeout_ms)
   opts.host = tostring(opts.host or defaults.host):gsub("/+$", "")
   if opts.max_context_chars_per_tab ~= nil then
      local max_context_chars_per_tab = tonumber(opts.max_context_chars_per_tab)
      if max_context_chars_per_tab then
         opts.max_context_chars_per_tab = math.max(1000, max_context_chars_per_tab)
      else
         opts.max_context_chars_per_tab = defaults.max_context_chars_per_tab
      end
   end
   opts.max_reason_chars = math.max(20, tonumber(opts.max_reason_chars) or defaults.max_reason_chars)
   opts.parallel_jobs = math.max(1, tonumber(opts.parallel_jobs) or defaults.parallel_jobs)
   opts.max_output_tokens = math.max(8, tonumber(opts.max_output_tokens) or defaults.max_output_tokens)
   if opts.keepalive == false or opts.keepalive == "" then
      opts.keepalive = nil
   elseif opts.keepalive ~= nil then
      opts.keepalive = tostring(opts.keepalive)
   end
   return opts
end

local function default_term_name(term)
   return term.default_name or term.name or provider_title(term.provider)
end

local function refresh_term_name(term)
   local name = term.generated_heading or term.last_prompt or default_term_name(term)
   if not name or name == "" then
      name = provider_title(term.provider)
   end
   if name == term.name then return false end
   term.name = name
   return true
end

local function prompt_history_store(provider)
   if provider == "codex" then
      return state.codex_prompt_history
   end
   if provider == "claude" then
      return state.claude_prompt_history
   end
end

normalize_search_text = function(text)
   if type(text) ~= "string" then return nil end
   text = text:gsub("[%z\1-\31]", " ")
   text = text:gsub("%s+", " ")
   text = vim.trim(text)
   if text == "" then return nil end
   return text
end

truncate_text = function(text, max_len)
   if type(text) ~= "string" then return "" end
   if #text <= max_len then return text end
   return text:sub(1, max_len - 3) .. "..."
end

local QUERY_SYNONYMS = {
   settings = { "setting", "settings", "config", "configs", "configuration", "configurations", "preference", "preferences", "option", "options" },
   config = { "config", "configs", "configuration", "configurations", "settings", "setting", "option", "options", "preferences" },
   configuration = { "configuration", "configurations", "config", "configs", "settings", "preferences", "options" },
   preference = { "preference", "preferences", "settings", "options", "config", "configuration" },
   option = { "option", "options", "settings", "preferences", "config", "configuration" },
}

local function tokenize_search_terms(text)
   text = normalize_search_text(text)
   if not text then return {} end

   local tokens = {}
   local seen = {}
   for token in text:lower():gmatch("[a-z0-9][a-z0-9_%-]*") do
      if token:sub(-1) == "s" and #token > 4 then
         local singular = token:sub(1, -2)
         if not seen[singular] then
            tokens[#tokens + 1] = singular
            seen[singular] = true
         end
      end
      if not seen[token] then
         tokens[#tokens + 1] = token
         seen[token] = true
      end
   end

   return tokens
end

local function expand_query_terms(query)
   local terms = {}
   for _, token in ipairs(tokenize_search_terms(query)) do
      local variants = QUERY_SYNONYMS[token] or { token }
      terms[#terms + 1] = variants
   end
   return terms
end

local function lexical_tab_filter_match(query, candidate)
   local query_norm = normalize_search_text(query)
   if not query_norm then return nil end
   query_norm = query_norm:lower()

   local sections = {
      { reason = "heading hit", text = normalize_search_text(candidate.heading) },
      { reason = "prompt hit", text = normalize_search_text(candidate.last_prompt) },
      { reason = "history hit", text = normalize_search_text(candidate.user_prompts) },
      { reason = "path hit", text = normalize_search_text(candidate.cwd) },
   }

   for _, section in ipairs(sections) do
      local haystack = section.text and section.text:lower() or nil
      if haystack and haystack:find(query_norm, 1, true) then
         return {
            match = true,
            reason = section.reason,
         }
      end
   end

   local term_groups = expand_query_terms(query_norm)
   if #term_groups == 0 then return nil end

   local matched = 0
   local best_reason = nil
   for _, variants in ipairs(term_groups) do
      local found = false
      for _, section in ipairs(sections) do
         local haystack = section.text and section.text:lower() or nil
         if haystack then
            for _, variant in ipairs(variants) do
               if haystack:find(variant, 1, true) then
                  found = true
                  best_reason = best_reason or section.reason
                  break
               end
            end
         end
         if found then break end
      end
      if found then
         matched = matched + 1
      end
   end

   if matched == 0 then return nil end

   local ratio = matched / #term_groups
   if #term_groups == 1 or ratio >= 0.6 then
      return {
         match = true,
         reason = best_reason or "term hit",
      }
   end

   return nil
end

local function shorten_path(path)
   if type(path) ~= "string" or path == "" then return "" end
   local home = vim.env.HOME or ""
   if home ~= "" and path:sub(1, #home) == home then
      path = "~" .. path:sub(#home + 1)
   end
   return vim.fn.pathshorten(path)
end

local function timestamp_to_unix(value)
   if type(value) == "number" then
      if value > 1000000000000 then
         return math.floor(value / 1000)
      end
      return value
   end
   if type(value) ~= "string" then return 0 end

   local year, month, day, hour, min, sec = value:match("^(%d%d%d%d)%-(%d%d)%-(%d%d)T(%d%d):(%d%d):(%d%d)")
   if not year then return 0 end

   return os.time({
      year = tonumber(year),
      month = tonumber(month),
      day = tonumber(day),
      hour = tonumber(hour),
      min = tonumber(min),
      sec = tonumber(sec),
   })
end

local function format_search_timestamp(value)
   local unix = timestamp_to_unix(value)
   if unix <= 0 then return "" end
   return os.date("%Y-%m-%d %H:%M", unix)
end

local function build_chat_search_entry(provider, session_id, cwd, transcript_path, role, text, timestamp)
   text = normalize_search_text(text)
   if not text then return nil end

   return {
      provider = provider,
      session_id = session_id,
      cwd = cwd or "",
      transcript_path = transcript_path,
      role = role or "",
      preview = truncate_text(text, 180),
      text = truncate_text(text, 4000),
      sort_time = timestamp_to_unix(timestamp),
      display_time = format_search_timestamp(timestamp),
      display_cwd = shorten_path(cwd),
   }
end

local function extract_codex_search_text(entry)
   if type(entry) ~= "table" or entry.type ~= "response_item" then return nil end

   local payload = entry.payload
   if type(payload) ~= "table" or payload.type ~= "message" then return nil end

   local role = payload.role
   if role ~= "user" and role ~= "assistant" then return nil end

   local content = payload.content
   local chunks = {}

   if type(content) == "string" then
      chunks[1] = content
   elseif vim.islist(content) then
      for _, item in ipairs(content) do
         if type(item) == "table" then
            local item_type = item.type
            if (item_type == "input_text" or item_type == "output_text" or item_type == "text")
               and type(item.text) == "string" then
               chunks[#chunks + 1] = item.text
            end
         end
      end
   end

   if #chunks == 0 then return nil end
   return role, table.concat(chunks, "\n"), entry.timestamp
end

local function extract_claude_search_text(entry)
   if type(entry) ~= "table" then return nil end

   local role = entry.type
   local message = entry.message
   if type(message) ~= "table" then return nil end
   role = message.role or role
   if role ~= "user" and role ~= "assistant" then return nil end

   local content = message.content
   local chunks = {}

   if type(content) == "string" then
      chunks[1] = content
   elseif vim.islist(content) then
      for _, item in ipairs(content) do
         if type(item) == "table" and item.type == "text" and type(item.text) == "string" then
            chunks[#chunks + 1] = item.text
         end
      end
   end

   if #chunks == 0 then return nil end
   return role, table.concat(chunks, "\n"), entry.timestamp
end

local function collect_codex_chat_search_entries(entries)
   local paths = vim.fn.glob(codex_sessions_path .. "/**/*.jsonl", false, true)

   for _, path in ipairs(paths) do
      local meta = get_codex_session_meta(path)
      local session_id = meta.session_id or path:match("([0-9a-f%-]+)%.jsonl$")
      local cwd = meta.cwd or ""
      local f = io.open(path, "r")
      if f then
         for line in f:lines() do
            local role, text, timestamp = extract_codex_search_text(decode_json_line(line))
            local item = build_chat_search_entry("codex", session_id, cwd, path, role, text, timestamp)
            if item then entries[#entries + 1] = item end
         end
         f:close()
      end
   end
end

local function collect_claude_chat_search_entries(entries)
   local paths = vim.fn.glob(claude_projects_path .. "/**/*.jsonl", false, true)

   for _, path in ipairs(paths) do
      if not path:find("/subagents/", 1, true) then
         local session_id = vim.fn.fnamemodify(path, ":t:r")
         local f = io.open(path, "r")
         if f then
            local cwd = ""
            for line in f:lines() do
               local entry = decode_json_line(line)
               if type(entry) == "table" and cwd == "" and type(entry.cwd) == "string" then
                  cwd = entry.cwd
               end
               local role, text, timestamp = extract_claude_search_text(entry)
               local item = build_chat_search_entry("claude", session_id, cwd, path, role, text, timestamp)
               if item then entries[#entries + 1] = item end
            end
            f:close()
         end
      end
   end
end

local function collect_chat_search_entries()
   local entries = {}
   collect_codex_chat_search_entries(entries)
   collect_claude_chat_search_entries(entries)

   table.sort(entries, function(a, b)
      if a.sort_time == b.sort_time then
         return (a.session_id or "") > (b.session_id or "")
      end
      return a.sort_time > b.sort_time
   end)

   return entries
end

local function encode_chat_search_line(entry)
   local fields = {
      entry.provider or "",
      entry.session_id or "",
      entry.cwd or "",
      entry.transcript_path or "",
      provider_title(entry.provider or ""),
      entry.display_time or "",
      entry.role or "",
      entry.display_cwd or "",
      entry.preview or "",
      entry.text or "",
   }

   for i, value in ipairs(fields) do
      fields[i] = normalize_search_text(value) or ""
   end

   return table.concat(fields, "\t")
end

local function set_term_heading(term, heading)
   heading = sanitize_heading(heading)
   if heading == term.generated_heading then return false end
   term.generated_heading = heading
   return refresh_term_name(term)
end

local function set_term_prompt(term, prompt)
   prompt = sanitize_prompt(prompt)
   if not prompt or prompt == term.last_prompt then return false end
   term.last_prompt = prompt
   return refresh_term_name(term)
end

local function get_session_prompt_history(provider, session_id)
   local store = prompt_history_store(provider)
   if not store or not session_id then return nil end
   return store[session_id]
end

local function get_term_prompt_history(term)
   return get_session_prompt_history(term.provider, provider_session_id(term))
end

local function build_heading_context(prompts, max_context_chars)
   local lines = {}

   if max_context_chars == nil then
      for i, prompt in ipairs(prompts) do
         lines[#lines + 1] = string.format("%d. %s", i, prompt)
      end
      return table.concat(lines, "\n"), false
   end

   local total = 0
   for i = #prompts, 1, -1 do
      local line = string.format("%d. %s", i, prompts[i])
      local line_len = #line + 1
      if #lines > 0 and total + line_len > max_context_chars then
         break
      end
      table.insert(lines, 1, line)
      total = total + line_len
   end

   return table.concat(lines, "\n"), #lines < #prompts
end

local function build_term_heading_request(term)
   local prompts = get_term_prompt_history(term)
   local session_id = provider_session_id(term)
   if not session_id or not prompts or #prompts == 0 then return nil end

   local context, truncated = build_heading_context(prompts, state.auto_heading_opts.max_context_chars)
   local key = table.concat({
      term.provider,
      session_id,
      tostring(#prompts),
      prompts[#prompts] or "",
   }, "\31")

   local prompt = table.concat({
      "You generate concise Neovim tab headings for long-running AI chat sessions.",
      'Return JSON only in the exact shape {"heading":"..."} and nothing else.',
      "Rules:",
      "- The heading must be 3 to 8 words.",
      "- Keep it concrete, specific, and scannable.",
      "- Prefer the actual task over generic words like chat, conversation, or help.",
      "- If the work narrowed over time, bias toward the latest stable direction.",
      "- Do not include quotes around the heading value besides valid JSON.",
      "- Do not end the heading with a period.",
      "",
      "Provider: " .. provider_title(term.provider),
      "Current heading: " .. (term.generated_heading or "none"),
      truncated and "Note: only the most recent prompts fit inside max_context_chars." or "Note: all recorded prompts are included.",
      "",
      "User prompts:",
      context,
   }, "\n")

   return {
      key = key,
      prompt = prompt,
   }
end

local function strip_terminal_control_sequences(text)
   if type(text) ~= "string" or text == "" then return "" end
   text = text:gsub("\r", "")
   text = text:gsub("\27%[[0-?]*[ -/]*[@-~]", "")
   text = text:gsub("\27%][^\7]*\7", "")
   text = text:gsub("\27%].-\27\\", "")
   text = text:gsub("\27P.-\27\\", "")
   text = text:gsub("\226\160[\128-\191]", "")
   text = text:gsub("\226\161[\128-\191]", "")
   text = text:gsub("[%z\1-\8\11-\12\14-\31\127]", " ")
   return text
end

local function clean_model_output(text)
   return vim.trim(strip_terminal_control_sequences(text))
end

local function parse_term_heading_output(output)
   output = clean_model_output(output)
   if output == "" then return nil end

   local decoded = decode_json(output)
   if type(decoded) == "table" and type(decoded.heading) == "string" then
      return sanitize_heading(decoded.heading)
   end

   local json_blob = output:match("%b{}")
   decoded = decode_json(json_blob)
   if type(decoded) == "table" and type(decoded.heading) == "string" then
      return sanitize_heading(decoded.heading)
   end

   return sanitize_heading(output)
end

local function shorten_text_middle(text, max_len)
   if type(text) ~= "string" then return "" end
   if #text <= max_len then return text end
   local head = math.floor((max_len - 3) / 2)
   local tail = max_len - 3 - head
   return text:sub(1, head) .. "..." .. text:sub(#text - tail + 1)
end

local function clean_model_error(text)
   text = normalize_search_text(clean_model_output(text))
   if not text then return nil end
   return shorten_text_middle(text, 240)
end

local function build_ollama_generate_command(model, prompt, opts)
   local body = {
      model = model,
      prompt = prompt,
      stream = false,
      format = "json",
      think = false,
   }

   if opts.keepalive then
      body.keep_alive = opts.keepalive
   end

   body.options = {
      temperature = 0,
      num_predict = opts.max_output_tokens,
   }

   return {
      "curl",
      "-sS",
      "--fail-with-body",
      "-X",
      "POST",
      (opts.host or "http://127.0.0.1:11434") .. "/api/generate",
      "-H",
      "Content-Type: application/json",
      "-d",
      vim.json.encode(body),
   }
end

local function parse_ollama_generate_output(output)
   output = clean_model_output(output)
   if output == "" then
      return nil, "empty response from ollama"
   end

   local decoded = decode_json(output)
   if type(decoded) ~= "table" then
      return nil, "invalid response from ollama"
   end

   if type(decoded.error) == "string" and decoded.error ~= "" then
      return nil, decoded.error
   end

   if type(decoded.response) ~= "string" then
      return nil, "missing response from ollama"
   end

   return decoded.response
end

local function push_limited_lines(lines, line, max_chars)
   if not line or line == "" then return 0 end
   lines[#lines + 1] = line

   if not max_chars then return #line + 1 end

   local total = 0
   for _, value in ipairs(lines) do
      total = total + #value + 1
   end

   while total > max_chars and #lines > 1 do
      local first = lines[1]
      total = total - (#first + 1)
      table.remove(lines, 1)
   end

   return total
end

local function find_claude_project_path(session_id)
   if not session_id or session_id == "" then return nil end
   local paths = vim.fn.glob(claude_projects_path .. "/**/" .. session_id .. ".jsonl", false, true)
   return paths[1]
end

local function collect_term_transcript_lines(term, max_chars)
   local lines = {}

   local function append(text)
      text = normalize_search_text(text)
      if not text then return end
      push_limited_lines(lines, text, max_chars)
   end

   local prompts = get_term_prompt_history(term) or {}
   if #prompts > 0 then
      for _, prompt in ipairs(prompts) do
         append(prompt)
      end
      return lines
   end

   if term.provider == "codex" then
      local path = term.codex_session_path
      if path and path ~= "" then
         local f = io.open(path, "r")
         if f then
            for line in f:lines() do
               local role, text = extract_codex_search_text(decode_json_line(line))
               if role == "user" and text then
                  append(text)
               end
            end
            f:close()
         end
      end
   elseif term.provider == "claude" then
      local session_id = term.claude_session_id
      local path = find_claude_project_path(session_id)
      if path then
         local f = io.open(path, "r")
         if f then
            for line in f:lines() do
               local role, text = extract_claude_search_text(decode_json_line(line))
               if role == "user" and text then
                  append(text)
               end
            end
            f:close()
         end
      end
   end

   return lines
end

local function build_tab_filter_candidates()
   local opts = state.tab_filter_opts or {}
   local candidates = {}

   for index, term in ipairs(state.terminals) do
      local prompt_lines = collect_term_transcript_lines(term, opts.max_context_chars_per_tab)
      local user_prompts = table.concat(prompt_lines, "\n")
      candidates[#candidates + 1] = {
         tab_index = index,
         cmax_id = term.cmax_id,
         provider = term.provider,
         heading = term.name or default_term_name(term),
         last_prompt = term.last_prompt or "",
         status = term.status or "",
         cwd = term.cwd or "",
         session_id = provider_session_id(term),
         user_prompts = user_prompts,
      }
   end

   return candidates
end

local function build_tab_filter_prompt(query, candidate)
   local payload = {
      query = query,
      tab = {
         provider = candidate.provider,
         heading = candidate.heading,
         last_prompt = candidate.last_prompt,
         status = candidate.status,
         cwd = candidate.cwd,
         session_id = candidate.session_id,
         user_prompts = candidate.user_prompts,
      },
   }

   return table.concat({
      "You evaluate whether a single open Neovim chat tab matches a semantic query.",
      'Return JSON only in the exact shape {"match":true,"reason":"..."} and nothing else.',
      "Rules:",
      "- Consider the tab heading, last prompt, provider, status, cwd, and the full user prompt history.",
      "- Be liberal about relatedness. If the tab plausibly relates to the query, prefer match=true.",
      "- Treat common related terms as relevant, for example settings/config/preferences/options.",
      "- match must be true or false.",
      "- If the tab is not relevant, set match=false.",
      "- If the tab is plausibly related, set match=true.",
      "- reason must be a very short phrase, not a sentence.",
      "",
      vim.json.encode(payload),
   }, "\n")
end

local function parse_tab_filter_output(output)
   output = clean_model_output(output)
   if output == "" then return nil, false end

   local decoded = decode_json(output)
   if type(decoded) ~= "table" then
      decoded = decode_json(output:match("%b{}"))
   end
   if type(decoded) ~= "table" then
      return nil, false
   end

   local is_match = decoded.match
   if type(is_match) == "string" then
      local normalized = is_match:lower()
      if normalized == "true" or normalized == "yes" then
         is_match = true
      elseif normalized == "false" or normalized == "no" then
         is_match = false
      end
   end
   if type(is_match) ~= "boolean" then
      return nil, false
   end

   return {
      match = is_match,
      reason = normalize_search_text(decoded.reason) or (is_match and "semantic match" or "not relevant"),
   }, true
end

local function new_state_id()
   return string.format("%x-%x-%x", os.time(), vim.uv.hrtime() % 0xffffffff, vim.fn.getpid())
end

local function ensure_multiplexer_session_id()
   if not state.multiplexer_session_id then
      state.multiplexer_session_id = new_state_id()
      state.multiplexer_created_at = os.time()
   end
   return state.multiplexer_session_id
end

local function current_session_path()
   return cmax_sessions_dir .. "/" .. ensure_multiplexer_session_id() .. ".json"
end

provider_title = function(provider)
   if provider == "codex" then
      return "Codex"
   end
   if provider == "claude" then
      return "Claude"
   end
   return provider:gsub("^%l", string.upper)
end

provider_session_id = function(term)
   if term.provider == "codex" then
      return term.codex_session_id
   end
   if term.provider == "claude" then
      return term.claude_session_id
   end
end

local function cleanup_term_files(term)
   os.remove(status_dir .. "/" .. term.cmax_id)
   os.remove(status_dir .. "/" .. term.cmax_id .. ".prompt")
   os.remove(status_dir .. "/" .. term.cmax_id .. ".session")
end

local function build_terminal_snapshot(term)
   return {
      provider = term.provider,
      cwd = term.cwd,
      name = term.name,
      default_name = term.default_name,
      last_prompt = term.last_prompt,
      generated_heading = term.generated_heading,
      status = term.status,
      session_id = provider_session_id(term),
      transcript_path = term.codex_session_path,
      command = term.launch and vim.deepcopy(term.launch.command) or nil,
      dangerously = term.launch and term.launch.dangerously or nil,
      profile = term.launch and term.launch.profile or nil,
   }
end

local function build_session_snapshot()
   if #state.terminals == 0 then return nil end

   ensure_multiplexer_session_id()

   local terminals = {}
   for _, term in ipairs(state.terminals) do
      table.insert(terminals, build_terminal_snapshot(term))
   end

   return {
      version = 1,
      id = state.multiplexer_session_id,
      created_at = state.multiplexer_created_at or os.time(),
      updated_at = os.time(),
      cwd = vim.fn.getcwd(),
      selected = state.selected,
      terminals = terminals,
   }
end

local function save_current_session()
   local snapshot = build_session_snapshot()
   if not snapshot then return end
   state.multiplexer_created_at = snapshot.created_at
   write_file(current_session_path(), vim.json.encode(snapshot))
end

local function load_saved_sessions()
   local sessions = {}
   local paths = vim.fn.glob(cmax_sessions_dir .. "/*.json", false, true)

   for _, path in ipairs(paths) do
      local session = read_json_file(path)
      if type(session) == "table" and vim.islist(session.terminals) and #session.terminals > 0 then
         session.path = path
         session.id = session.id or vim.fn.fnamemodify(path, ":t:r")
         session.updated_at = tonumber(session.updated_at) or tonumber(session.created_at) or 0
         session.created_at = tonumber(session.created_at) or session.updated_at
         table.insert(sessions, session)
      end
   end

   table.sort(sessions, function(a, b)
      if a.updated_at == b.updated_at then
         return (a.id or "") > (b.id or "")
      end
      return a.updated_at > b.updated_at
   end)

   state.saved_sessions = sessions
   return sessions
end

local function poll_history(path, offset_key, prompt_store, history_store, extract)
   local stat = vim.uv.fs_stat(path)
   if not stat or stat.type ~= "file" then return false end

   local offset = state[offset_key] or 0
   if stat.size < offset then
      offset = 0
      state[offset_key] = 0
      clear_table(prompt_store)
      if history_store then
         clear_table(history_store)
      end
   end
   if stat.size == offset then return false end

   local f = io.open(path, "r")
   if not f then return false end
   f:seek("set", offset)

   local changed = false
   for line in f:lines() do
      local entry = decode_json_line(line)
      local session_id, raw_prompt = extract(entry)
      local latest_prompt = sanitize_prompt(raw_prompt)
      local history_prompt = normalize_search_text(raw_prompt)
      if session_id and (latest_prompt or history_prompt) then
         if latest_prompt and prompt_store[session_id] ~= latest_prompt then
            prompt_store[session_id] = latest_prompt
            changed = true
         end
         if history_store and history_prompt then
            history_store[session_id] = history_store[session_id] or {}
            table.insert(history_store[session_id], history_prompt)
            changed = true
         end
      end
   end

   state[offset_key] = f:seek() or stat.size
   f:close()
   return changed
end

local function poll_codex_history()
   return poll_history(
      codex_history_path,
      "codex_history_offset",
      state.codex_latest_prompts,
      state.codex_prompt_history,
      function(entry)
      if type(entry) ~= "table" then return nil end
      return entry.session_id, entry.text
   end
   )
end

local function poll_claude_history()
   return poll_history(
      claude_history_path,
      "claude_history_offset",
      state.claude_latest_prompts,
      state.claude_prompt_history,
      function(entry)
      if type(entry) ~= "table" then return nil end
      return entry.sessionId or entry.session_id, entry.display
   end
   )
end

local function parse_codex_session_started_at(path)
   local filename = vim.fs.basename(path)
   local year, month, day, hour, min, sec = filename:match(
      "^rollout%-(%d%d%d%d)%-(%d%d)%-(%d%d)T(%d%d)%-(%d%d)%-(%d%d)%-.+%.jsonl$"
   )
   if not year then return nil end
   return os.time({
      year = tonumber(year),
      month = tonumber(month),
      day = tonumber(day),
      hour = tonumber(hour),
      min = tonumber(min),
      sec = tonumber(sec),
   })
end

get_codex_session_meta = function(path)
   local cached = state.codex_session_cache[path]
   if cached then return cached end

   local meta = {
      path = path,
      started_at = parse_codex_session_started_at(path),
      session_id = path:match("([0-9a-f%-]+)%.jsonl$"),
   }

   local f = io.open(path, "r")
   if f then
      local first_line = f:read("*l")
      f:close()
      local entry = decode_json_line(first_line)
      if entry and entry.type == "session_meta" and type(entry.payload) == "table" then
         meta.cwd = entry.payload.cwd
      end
   end

   state.codex_session_cache[path] = meta
   return meta
end

local function claim_codex_session(term, meta)
   if not meta or not meta.path or not meta.session_id then return false end
   state.codex_claimed_sessions[meta.path] = term.cmax_id
   term.codex_session_path = meta.path
   term.codex_session_id = meta.session_id
   return true
end

local function resolve_codex_session(term)
   if term.provider ~= "codex" or term.codex_session_id then return false end

   local now = vim.uv.now()
   if term.next_codex_lookup_at and now < term.next_codex_lookup_at then
      return false
   end
   term.next_codex_lookup_at = now + 2000

   local paths = vim.fn.glob(codex_sessions_path .. "/**/*.jsonl", false, true)
   local best_meta
   local best_delta

   for _, path in ipairs(paths) do
      local claimed_by = state.codex_claimed_sessions[path]
      if claimed_by == nil or claimed_by == term.cmax_id then
         local meta = get_codex_session_meta(path)
         local delta = meta.started_at and (meta.started_at - term.started_at) or nil
         if meta.cwd == term.cwd and delta and delta >= -5 and delta <= 120 then
            local distance = math.abs(delta)
            if not best_delta or distance < best_delta then
               best_meta = meta
               best_delta = distance
            end
         end
      end
   end

   return claim_codex_session(term, best_meta)
end

local function resolve_claude_session(term)
   if term.provider ~= "claude" or term.claude_session_id or not term.pid then return false end

   local path = string.format("%s/%d.json", claude_sessions_path, term.pid)
   local meta = read_json_file(path)
   if type(meta) ~= "table" then return false end

   local session_id = meta.sessionId or meta.session_id
   if not session_id or (meta.cwd and meta.cwd ~= term.cwd) then return false end

   term.claude_session_id = session_id
   return true
end

local function update_term_session_from_file(term)
   local session = read_json_file(status_dir .. "/" .. term.cmax_id .. ".session")
   if type(session) ~= "table" then return false end

   local changed = false
   local session_id = session.session_id

   if term.provider == "codex" then
      if session_id and session_id ~= term.codex_session_id then
         term.codex_session_id = session_id
         changed = true
      end
      if session.transcript_path and session.transcript_path ~= term.codex_session_path then
         if term.codex_session_path and state.codex_claimed_sessions[term.codex_session_path] == term.cmax_id then
            state.codex_claimed_sessions[term.codex_session_path] = nil
         end
         term.codex_session_path = session.transcript_path
         state.codex_claimed_sessions[term.codex_session_path] = term.cmax_id
         changed = true
      end
   elseif term.provider == "claude" then
      if session_id and session_id ~= term.claude_session_id then
         term.claude_session_id = session_id
         changed = true
      end
   end

   return changed
end

local function write_hook_script(path)
   if write_file(path, HOOK_SCRIPT) then
      pcall(vim.fn.system, { "chmod", "+x", path })
      return true
   end
   return false
end

local function install_claude_hooks()
   if not write_hook_script(claude_hook_script_path) then return end
   os.remove(claude_old_hook_script_path)

   local settings = read_json_file(claude_settings_path)
   if type(settings) ~= "table" then return end

   settings.hooks = settings.hooks or {}
   local cmax_hook = {
      type = "command",
      command = claude_hook_script_path,
      timeout = 5,
      async = true,
   }

   local changed = false

   for _, event in ipairs(HOOK_EVENTS) do
      settings.hooks[event] = settings.hooks[event] or {}

      for _, entry in ipairs(settings.hooks[event]) do
         local hooks_list = entry.hooks or {}
         for j = #hooks_list, 1, -1 do
            if hooks_list[j].command == claude_old_hook_script_path then
               table.remove(hooks_list, j)
               changed = true
            end
         end
      end

      local found = false
      for _, entry in ipairs(settings.hooks[event]) do
         local hooks_list = entry.hooks or {}
         for _, hook in ipairs(hooks_list) do
            if hook.command == claude_hook_script_path then
               found = true
               break
            end
         end
         if found then break end
      end

      if not found then
         if #settings.hooks[event] > 0 then
            local entry = settings.hooks[event][1]
            entry.hooks = entry.hooks or {}
            table.insert(entry.hooks, cmax_hook)
         else
            table.insert(settings.hooks[event], {
               matcher = "",
               hooks = { cmax_hook },
            })
         end
         changed = true
      end
   end

   if changed then
      write_file(claude_settings_path, vim.json.encode(settings))
   end
end

local function ensure_codex_hooks_enabled()
   local content = read_file(codex_config_path)
   if not content then
      write_file(codex_config_path, "[features]\ncodex_hooks = true\n")
      return
   end

   local lines = vim.split(content, "\n", { plain = true })
   local changed = false
   local found_features = false
   local found_flag = false
   local in_features = false

   for i, line in ipairs(lines) do
      local trimmed = vim.trim(line)
      if trimmed:match("^%[") then
         if in_features and not found_flag then
            table.insert(lines, i, "codex_hooks = true")
            found_flag = true
            changed = true
            break
         end
         in_features = trimmed == "[features]"
         if in_features then
            found_features = true
         end
      elseif in_features and trimmed:match("^codex_hooks%s*=") then
         found_flag = true
         if trimmed ~= "codex_hooks = true" then
            lines[i] = line:gsub("=.*$", "= true")
            changed = true
         end
      end
   end

   if not found_features then
      if #lines > 0 and lines[#lines] ~= "" then
         table.insert(lines, "")
      end
      table.insert(lines, "[features]")
      table.insert(lines, "codex_hooks = true")
      changed = true
   elseif in_features and not found_flag then
      table.insert(lines, "codex_hooks = true")
      changed = true
   end

   if changed then
      write_file(codex_config_path, table.concat(lines, "\n"))
   end
end

local function ensure_codex_hook_group(groups, matcher, hook)
   local wanted_matcher = matcher or ""

   for _, group in ipairs(groups) do
      local group_matcher = group.matcher or ""
      if group_matcher == wanted_matcher then
         group.hooks = group.hooks or {}
         for _, existing in ipairs(group.hooks) do
            if existing.command == hook.command then
               return false
            end
         end
         table.insert(group.hooks, hook)
         return true
      end
   end

   table.insert(groups, {
      matcher = matcher,
      hooks = { hook },
   })
   return true
end

local function install_codex_hooks()
   if not write_hook_script(codex_hook_script_path) then return end
   ensure_codex_hooks_enabled()

   local hooks_config = read_json_file(codex_hooks_path)
   if type(hooks_config) ~= "table" then
      hooks_config = { hooks = {} }
   end
   hooks_config.hooks = hooks_config.hooks or {}

   local hook = {
      type = "command",
      command = codex_hook_script_path,
      timeout = 5,
   }

   local specs = {
      { event = "SessionStart", matcher = "startup|resume" },
      { event = "PreToolUse", matcher = "Bash" },
      { event = "PostToolUse", matcher = "Bash" },
      { event = "UserPromptSubmit", matcher = "" },
      { event = "Stop", matcher = "" },
   }

   local changed = false
   for _, spec in ipairs(specs) do
      hooks_config.hooks[spec.event] = hooks_config.hooks[spec.event] or {}
      if ensure_codex_hook_group(hooks_config.hooks[spec.event], spec.matcher, hook) then
         changed = true
      end
   end

   if changed or not read_file(codex_hooks_path) then
      write_file(codex_hooks_path, vim.json.encode(hooks_config))
   end
end

local function install_hooks()
   install_claude_hooks()
   install_codex_hooks()
end

local function menu_item_count()
   return #state.terminals + 1
end

local function saved_session_count()
   if #state.saved_sessions == 0 then
      return 1
   end
   return #state.saved_sessions
end

local function filtered_terminal_count()
   if #state.filtered_terminals == 0 then
      return 1
   end
   return #state.filtered_terminals
end

local function current_item_count()
   if state.view == "sessions" then
      return saved_session_count()
   end
   if state.view == "filter" then
      return filtered_terminal_count()
   end
   return menu_item_count()
end

local function apply_highlights(buf)
   vim.api.nvim_buf_clear_namespace(buf, ns, 0, -1)
   local line_count = vim.api.nvim_buf_line_count(buf)

   local start_line = (state.selected - 1) * item_height
   for i = 0, item_height - 1 do
      local line = start_line + i
      if line < line_count then
         vim.api.nvim_buf_add_highlight(buf, ns, "CmaxBoxActive", line, 0, -1)
      end
   end

   local sel_end = start_line + item_height
   for _, entry in ipairs(state.status_lines) do
      if entry.line < start_line or entry.line >= sel_end then
         vim.api.nvim_buf_add_highlight(buf, ns, entry.hl, entry.line, 0, -1)
      end
   end
end

local function configure_window(win)
   vim.wo[win].wrap = false
   vim.wo[win].number = false
   vim.wo[win].relativenumber = false
   vim.wo[win].signcolumn = "no"
   vim.wo[win].cursorline = false
end

local function ensure_window()
   if state.win and vim.api.nvim_win_is_valid(state.win) then
      return state.win
   end

   vim.cmd("vsplit")
   state.win = vim.api.nvim_get_current_win()
   configure_window(state.win)
   return state.win
end

local function pad_line(text, inner_width)
   text = text or ""
   local len = vim.fn.strdisplaywidth(text)
   if len > inner_width then
      text = text:sub(1, inner_width - 3) .. "..."
      len = vim.fn.strdisplaywidth(text)
   end
   return "| " .. text .. string.rep(" ", inner_width - len) .. " |"
end

local function render_menu(win)
   local buf = state.menu_buf
   if not buf or not vim.api.nvim_buf_is_valid(buf) then return end
   state.view = "menu"

   local box_width = vim.api.nvim_win_get_width(win)
   if box_width < 6 then box_width = 6 end
   local inner_width = box_width - 4
   local top = "+" .. string.rep("-", box_width - 2) .. "+"
   local bottom = top
   local empty = "|" .. string.rep(" ", box_width - 2) .. "|"

   local labels = {}
   for _, term in ipairs(state.terminals) do
      table.insert(labels, term.name)
   end
   table.insert(labels, "+ New")

   local lines = {}
   state.status_lines = {}
   for i, label in ipairs(labels) do
      table.insert(lines, top)
      table.insert(lines, pad_line(label, inner_width))
      local term = state.terminals[i]
      if term and term.status and term.status ~= "" then
         table.insert(lines, pad_line("  * " .. term.status, inner_width))
         local hl = STATUS_HL[term.status] or "CmaxStatusActive"
         table.insert(state.status_lines, { line = #lines - 1, hl = hl })
      else
         table.insert(lines, empty)
      end
      for _ = 3, inner_height do
         table.insert(lines, empty)
      end
      table.insert(lines, bottom)
   end

   vim.bo[buf].modifiable = true
   vim.api.nvim_buf_set_lines(buf, 0, -1, false, lines)
   vim.bo[buf].modifiable = false

   if state.selected > menu_item_count() then
      state.selected = menu_item_count()
   end
   apply_highlights(buf)

   local target = (state.selected - 1) * item_height + 1
   if vim.api.nvim_win_is_valid(win) then
      vim.api.nvim_win_set_cursor(win, { target, 0 })
   end
end

local function session_title(session)
   local when = os.date("%Y-%m-%d %H:%M", session.updated_at or session.created_at or os.time())
   local count = #(session.terminals or {})
   return string.format("%s  %d tab%s", when, count, count == 1 and "" or "s")
end

local function session_summary(session)
   local labels = {}
   for i, term in ipairs(session.terminals or {}) do
      labels[#labels + 1] = term.generated_heading or term.name or term.last_prompt or (provider_title(term.provider) .. " " .. i)
      if #labels == 3 then break end
   end
   return table.concat(labels, " | ")
end

local function render_saved_sessions(win)
   local buf = state.menu_buf
   if not buf or not vim.api.nvim_buf_is_valid(buf) then return end
   state.view = "sessions"

   local box_width = vim.api.nvim_win_get_width(win)
   if box_width < 6 then box_width = 6 end
   local inner_width = box_width - 4
   local top = "+" .. string.rep("-", box_width - 2) .. "+"
   local bottom = top

   local lines = {}
   state.status_lines = {}

   if #state.saved_sessions == 0 then
      table.insert(lines, top)
      table.insert(lines, pad_line("No saved sessions", inner_width))
      table.insert(lines, pad_line("Press q to go back", inner_width))
      table.insert(lines, pad_line("", inner_width))
      table.insert(lines, bottom)
   else
      for _, session in ipairs(state.saved_sessions) do
         table.insert(lines, top)
         table.insert(lines, pad_line(session_title(session), inner_width))
         table.insert(lines, pad_line(session_summary(session), inner_width))
         table.insert(lines, pad_line(vim.fn.pathshorten(session.cwd or ""), inner_width))
         table.insert(lines, bottom)
      end
   end

   vim.bo[buf].modifiable = true
   vim.api.nvim_buf_set_lines(buf, 0, -1, false, lines)
   vim.bo[buf].modifiable = false

   if state.selected > saved_session_count() then
      state.selected = saved_session_count()
   end
   apply_highlights(buf)

   local target = (state.selected - 1) * item_height + 1
   if vim.api.nvim_win_is_valid(win) then
      vim.api.nvim_win_set_cursor(win, { target, 0 })
   end
end

local function render_filtered_terminals(win)
   local buf = state.menu_buf
   if not buf or not vim.api.nvim_buf_is_valid(buf) then return end
   state.view = "filter"

   local box_width = vim.api.nvim_win_get_width(win)
   if box_width < 6 then box_width = 6 end
   local inner_width = box_width - 4
   local top = "+" .. string.rep("-", box_width - 2) .. "+"
   local bottom = top
   local empty = "|" .. string.rep(" ", box_width - 2) .. "|"

   local lines = {}
   state.status_lines = {}

   if state.tab_filter_loading and #state.filtered_terminals == 0 then
      local progress = string.format("%d/%d tabs checked", state.tab_filter_progress_done or 0, state.tab_filter_progress_total or 0)
      if (state.tab_filter_failure_count or 0) > 0 then
         progress = progress .. string.format("  %d failed", state.tab_filter_failure_count)
      end
      table.insert(lines, top)
      table.insert(lines, pad_line("Filtering tabs... " .. progress, inner_width))
      table.insert(lines, pad_line(state.tab_filter_query or "", inner_width))
      table.insert(lines, pad_line("Press q or <Esc> to go back", inner_width))
      table.insert(lines, bottom)
   elseif state.tab_filter_error then
      table.insert(lines, top)
      table.insert(lines, pad_line("Tab filter failed", inner_width))
      table.insert(lines, pad_line(shorten_text_middle(state.tab_filter_error, inner_width), inner_width))
      table.insert(lines, pad_line("Press s to try again, q to go back", inner_width))
      table.insert(lines, bottom)
   elseif #state.filtered_terminals == 0 then
      table.insert(lines, top)
      table.insert(lines, pad_line("No matching tabs", inner_width))
      table.insert(lines, pad_line(state.tab_filter_query or "", inner_width))
      table.insert(lines, pad_line("Press q or <Esc> to go back", inner_width))
      table.insert(lines, bottom)
   else
      for _, item in ipairs(state.filtered_terminals) do
         local live_term = item.cmax_id and find_terminal(item.cmax_id) or nil
         local heading = item.heading
         local status = item.status
         local display_cwd = item.display_cwd
         if live_term then
            heading = live_term.name or default_term_name(live_term)
            status = live_term.status or ""
            display_cwd = shorten_path(live_term.cwd or "")
         end

         table.insert(lines, top)
         table.insert(lines, pad_line(heading, inner_width))
         table.insert(lines, pad_line("  * " .. item.reason, inner_width))
         local meta = table.concat(vim.tbl_filter(function(value)
            return value and value ~= ""
         end, {
            item.display_score,
            status,
            display_cwd,
         }), "  ")
         if meta ~= "" then
            table.insert(lines, pad_line(meta, inner_width))
         else
            table.insert(lines, empty)
         end
         table.insert(lines, bottom)
      end
   end

   vim.bo[buf].modifiable = true
   vim.api.nvim_buf_set_lines(buf, 0, -1, false, lines)
   vim.bo[buf].modifiable = false

   if state.selected > filtered_terminal_count() then
      state.selected = filtered_terminal_count()
   end
   apply_highlights(buf)

   local target = (state.selected - 1) * item_height + 1
   if vim.api.nvim_win_is_valid(win) then
      vim.api.nvim_win_set_cursor(win, { target, 0 })
   end
end

local function select_item(index)
   if index < 1 then index = 1 end
   local count = current_item_count()
   if index > count then index = count end
   state.selected = index

   local buf = state.menu_buf
   local win = state.win
   if not buf or not vim.api.nvim_buf_is_valid(buf) then return end
   if not win or not vim.api.nvim_win_is_valid(win) then return end

   apply_highlights(buf)
   local target = (index - 1) * item_height + 1
   vim.api.nvim_win_set_cursor(win, { target, 0 })
end

find_terminal = function(cmax_id)
   for _, term in ipairs(state.terminals) do
      if term.cmax_id == cmax_id then
         return term
      end
   end
end

local function find_terminal_with_index(cmax_id)
   for index, term in ipairs(state.terminals) do
      if term.cmax_id == cmax_id then
         return term, index
      end
   end
end

local function find_terminal_by_session(provider, session_id, transcript_path)
   for index, term in ipairs(state.terminals) do
      if term.provider == provider then
         if provider == "codex" then
            if (session_id and term.codex_session_id == session_id)
               or (transcript_path and term.codex_session_path == transcript_path) then
               return term, index
            end
         elseif provider == "claude" and session_id and term.claude_session_id == session_id then
            return term, index
         end
      end
   end
end

local function focus_terminal_session(term, index)
   if not term or not vim.api.nvim_buf_is_valid(term.buf) then return false end
   local win = ensure_window()
   state.selected = index or state.selected
   vim.api.nvim_win_set_buf(win, term.buf)
   vim.cmd("startinsert")
   save_current_session()
   return true
end

local function close_tab_filter_view()
   if state.active_tab_filter_job and state.active_tab_filter_job.handles then
      for _, handle in pairs(state.active_tab_filter_job.handles) do
         pcall(function()
            handle:kill(15)
         end)
      end
   end
   state.active_tab_filter_job = nil
   state.tab_filter_loading = false
   state.tab_filter_error = nil
   state.filtered_terminals = {}
   state.tab_filter_query = nil
   state.tab_filter_progress_done = 0
   state.tab_filter_progress_total = 0
   state.tab_filter_failure_count = 0

   local selected = state.tab_filter_return_selected
   if selected then
      state.selected = math.max(1, math.min(selected, menu_item_count()))
   end
   state.tab_filter_return_selected = nil
   show_menu()
end

local function open_filtered_selected()
   local item = state.filtered_terminals[state.selected]
   if not item then return end

   local term, index
   if item.cmax_id then
      term, index = find_terminal_with_index(item.cmax_id)
   end
   if not term and item.tab_index and state.terminals[item.tab_index] then
      term = state.terminals[item.tab_index]
      index = item.tab_index
   end

   if not term then
      vim.notify("cmax: the filtered tab is no longer open", vim.log.levels.WARN)
      show_menu()
      return
   end

   state.tab_filter_return_selected = nil
   focus_terminal_session(term, index)
end

local function set_filter_keymaps(buf)
   vim.keymap.set("n", "j", function() select_item(state.selected + 1) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "k", function() select_item(state.selected - 1) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "G", function() select_item(filtered_terminal_count()) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "gg", function() select_item(1) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "<C-d>", function()
      local half = math.max(1, math.floor(filtered_terminal_count() / 2))
      select_item(state.selected + half)
   end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "<C-u>", function()
      local half = math.max(1, math.floor(filtered_terminal_count() / 2))
      select_item(state.selected - half)
   end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "<CR>", open_filtered_selected, { buffer = buf, nowait = true })
   vim.keymap.set("n", "s", prompt_tab_filter, { buffer = buf, nowait = true })
   vim.keymap.set("n", "q", close_tab_filter_view, { buffer = buf, nowait = true })
   vim.keymap.set("n", "<Esc>", close_tab_filter_view, { buffer = buf, nowait = true })
end

local function ensure_filter_view()
   local win = ensure_window()
   local buf = state.menu_buf
   if not (buf and vim.api.nvim_buf_is_valid(buf) and state.view == "filter") then
      buf = vim.api.nvim_create_buf(false, true)
      vim.bo[buf].bufhidden = "wipe"
      state.menu_buf = buf
      vim.api.nvim_win_set_buf(win, buf)
      set_filter_keymaps(buf)

      vim.api.nvim_create_autocmd("WinResized", {
         buffer = buf,
         callback = function()
            if vim.api.nvim_win_is_valid(win) then
               render_filtered_terminals(win)
            end
         end,
      })
   else
      vim.api.nvim_win_set_buf(win, buf)
   end

   return buf, win
end

local function build_filtered_tab_items(results)
   local opts = state.tab_filter_opts or {}
   local max_reason_chars = opts.max_reason_chars or 90
   local items = {}

   for _, result in ipairs(results or {}) do
      if result.match and result.candidate then
         local candidate = result.candidate
         items[#items + 1] = {
            cmax_id = candidate.cmax_id,
            tab_index = candidate.tab_index,
            heading = candidate.heading,
            reason = truncate_text(result.reason or "semantic match", max_reason_chars),
            display_score = "related",
            status = candidate.status or "",
            display_cwd = shorten_path(candidate.cwd or ""),
         }
      end
   end

   table.sort(items, function(a, b)
      return a.tab_index < b.tab_index
   end)

   return items
end

local function finalize_tab_filter_request(request)
   if not state.active_tab_filter_job or state.active_tab_filter_job.id ~= request.id then
      return
   end

   state.active_tab_filter_job = nil
   state.tab_filter_loading = false

   local items = build_filtered_tab_items(request.results)
   if #items > 0 then
      show_tab_filter_results(items, request.query)
      return
   end

   if request.failure_count > 0 then
      local summary = string.format("%d/%d tabs failed", request.failure_count, request.total)
      if request.last_error then
         summary = summary .. ": " .. request.last_error
      end
      show_tab_filter_results({}, request.query, { error = summary })
      return
   end

   show_tab_filter_results({}, request.query)
end

local function pump_tab_filter_jobs(request)
   if not state.active_tab_filter_job or state.active_tab_filter_job.id ~= request.id then
      return
   end

   local opts = state.tab_filter_opts or {}
   local max_running = math.max(1, math.min(opts.parallel_jobs or 1, request.total))

   while request.running < max_running and request.next_index <= request.total do
      local candidate = request.candidates[request.next_index]
      request.next_index = request.next_index + 1
      request.running = request.running + 1

      local handle = vim.system(build_ollama_generate_command(request.model, build_tab_filter_prompt(request.query, candidate), opts), {
         text = true,
         timeout = opts.timeout_ms,
      }, function(result)
         vim.schedule(function()
            if not state.active_tab_filter_job or state.active_tab_filter_job.id ~= request.id then
               return
            end

            request.running = request.running - 1
            request.completed = request.completed + 1
            state.tab_filter_progress_done = request.completed

            if result.code == 0 then
               local response_text, response_error = parse_ollama_generate_output(result.stdout)
               local parsed, ok
               if response_text then
                  parsed, ok = parse_tab_filter_output(response_text)
               else
                  parsed, ok = nil, false
               end
               if ok then
                  request.results[#request.results + 1] = vim.tbl_extend("force", parsed, {
                     candidate = candidate,
                  })
                  state.filtered_terminals = build_filtered_tab_items(request.results)
               else
                  request.failure_count = request.failure_count + 1
                  request.last_error = clean_model_error(response_error or result.stdout) or "model returned invalid JSON"
                  state.tab_filter_failure_count = request.failure_count
               end
            else
               request.failure_count = request.failure_count + 1
               request.last_error = clean_model_error(result.stderr ~= "" and result.stderr or result.stdout or "") or "model request failed"
               state.tab_filter_failure_count = request.failure_count
            end

            if request.completed >= request.total then
               finalize_tab_filter_request(request)
            else
               if state.view == "filter" then
                  rerender_active_view()
               end
               pump_tab_filter_jobs(request)
            end
         end)
      end)

      request.handles[#request.handles + 1] = handle
   end
end

show_tab_filter_results = function(items, query, opts)
   opts = opts or {}

   if state.view ~= "filter" then
      state.tab_filter_return_selected = state.selected
   end

   state.filtered_terminals = items or {}
   state.tab_filter_query = query
   state.tab_filter_loading = not not opts.loading
   state.tab_filter_error = opts.error and clean_model_error(opts.error) or nil
   state.selected = 1

   local _, win = ensure_filter_view()
   render_filtered_terminals(win)
end

prompt_tab_filter = function()
   local opts = state.tab_filter_opts or {}
   if not opts.enabled then
      vim.notify("cmax: semantic tab filter is disabled", vim.log.levels.WARN)
      return
   end

   if vim.fn.executable("curl") ~= 1 then
      vim.notify("cmax: curl is required for the tab filter", vim.log.levels.ERROR)
      return
   end

   vim.ui.input({
      prompt = "Filter tabs> ",
      default = state.tab_filter_query or "",
   }, function(input)
      local query = normalize_search_text(input)
      if not query then return end

      local candidates = build_tab_filter_candidates()
      if #candidates == 0 then
         vim.notify("cmax: no open tabs to filter", vim.log.levels.INFO)
         return
      end

      if state.active_tab_filter_job and state.active_tab_filter_job.handles then
         for _, handle in pairs(state.active_tab_filter_job.handles) do
            pcall(function()
               handle:kill(15)
            end)
         end
      end

      local request = {
         id = tostring(vim.uv.hrtime()),
         query = query,
         model = opts.model
            or (state.auto_heading_opts and state.auto_heading_opts.model)
            or "qwen2.5:1.5b",
         candidates = {},
         total = #candidates,
         next_index = 1,
         running = 0,
         completed = 0,
         failure_count = 0,
         last_error = nil,
         results = {},
         handles = {},
      }

      for _, candidate in ipairs(candidates) do
         local lexical = lexical_tab_filter_match(query, candidate)
         if lexical then
            request.results[#request.results + 1] = vim.tbl_extend("force", lexical, {
               candidate = candidate,
            })
            request.completed = request.completed + 1
         else
            request.candidates[#request.candidates + 1] = candidate
         end
      end

      state.active_tab_filter_job = request
      state.tab_filter_progress_done = request.completed
      state.tab_filter_progress_total = #candidates
      state.tab_filter_failure_count = 0
      state.filtered_terminals = build_filtered_tab_items(request.results)
      show_tab_filter_results(state.filtered_terminals, query, { loading = #request.candidates > 0 })
      if #request.candidates == 0 then
         finalize_tab_filter_request(request)
         return
      end
      pump_tab_filter_jobs(request)
   end)
end

local function open_chat_search_result(provider, session_id, cwd, transcript_path, label)
   local term, index = find_terminal_by_session(provider, session_id, transcript_path)
   if focus_terminal_session(term, index) then
      return
   end

   local opts = {
      provider = provider,
      cwd = cwd ~= "" and cwd or vim.fn.getcwd(),
      transcript_path = transcript_path,
      label = label,
      last_prompt = label,
      startinsert = true,
   }

   if session_id and session_id ~= "" then
      opts.resume = true
      opts.session_id = session_id
   end

   ensure_window()
   local cmd = build_command(opts)
   create_terminal(cmd, opts)
   state.selected = #state.terminals
   save_current_session()
end

local function open_chat_search_window()
   local buf = vim.api.nvim_create_buf(false, true)
   vim.bo[buf].bufhidden = "wipe"

   local width = math.max(80, math.floor(vim.o.columns * 0.9))
   local height = math.max(20, math.floor((vim.o.lines - vim.o.cmdheight) * 0.8))
   local row = math.max(1, math.floor((vim.o.lines - height) / 2) - 1)
   local col = math.max(0, math.floor((vim.o.columns - width) / 2))

   local win = vim.api.nvim_open_win(buf, true, {
      relative = "editor",
      row = row,
      col = col,
      width = width,
      height = height,
      style = "minimal",
      border = "rounded",
      title = " Chat Search ",
      title_pos = "center",
   })

   configure_window(win)
   return buf, win
end

rerender_active_view = function()
   if not (state.menu_buf and vim.api.nvim_buf_is_valid(state.menu_buf)) then return end
   if not (state.win and vim.api.nvim_win_is_valid(state.win)) then return end

   if state.view == "sessions" then
      render_saved_sessions(state.win)
   elseif state.view == "filter" then
      render_filtered_terminals(state.win)
   else
      render_menu(state.win)
   end
end

local function schedule_term_heading(term)
   local opts = state.auto_heading_opts
   if not opts or not opts.enabled then return end
   if vim.fn.executable("curl") ~= 1 then return end

   local request = build_term_heading_request(term)
   if not request then return end
   if term.heading_applied_key == request.key and term.generated_heading then
      term.heading_dirty = false
      term.heading_target_key = request.key
      return
   end

   if term.heading_target_key ~= request.key then
      term.heading_target_key = request.key
      term.heading_due_at = vim.uv.now() + opts.debounce_ms
      term.heading_dirty = true
      return
   end

   if not term.generated_heading and term.heading_requested_key ~= request.key then
      term.heading_due_at = term.heading_due_at or (vim.uv.now() + opts.debounce_ms)
      term.heading_dirty = true
   end
end

local function start_term_heading_job(term, request)
   local opts = state.auto_heading_opts
   term.heading_requested_key = request.key
   term.heading_dirty = false
   state.active_heading_job = {
      cmax_id = term.cmax_id,
      key = request.key,
   }

   vim.system(build_ollama_generate_command(opts.model, request.prompt, opts), { text = true, timeout = opts.timeout_ms }, function(result)
      vim.schedule(function()
         local live_term = find_terminal(term.cmax_id)
         state.active_heading_job = nil

         if live_term and live_term.heading_requested_key == request.key then
            live_term.heading_requested_key = nil
         end

         if result.code == 0 then
            local response_text, parse_error = parse_ollama_generate_output(result.stdout)
            if response_text then
               state.heading_failure_message = nil
            else
               parse_error = clean_model_error(parse_error) or "invalid ollama response"
            end

            if live_term and live_term.heading_target_key == request.key and response_text then
               local heading = parse_term_heading_output(response_text)
               if heading and set_term_heading(live_term, heading) then
                  save_current_session()
                  rerender_active_view()
               end
               live_term.heading_applied_key = request.key
            elseif live_term and response_text then
               live_term.heading_dirty = true
               live_term.heading_due_at = vim.uv.now()
            elseif live_term then
               live_term.heading_dirty = true
               live_term.heading_due_at = vim.uv.now() + opts.debounce_ms
               state.heading_backoff_until = vim.uv.now() + 15000
               if parse_error ~= "" and parse_error ~= state.heading_failure_message then
                  state.heading_failure_message = parse_error
                  vim.notify("cmax auto headings: " .. parse_error, vim.log.levels.WARN)
               end
            end
         else
            local error_text = clean_model_error(result.stderr ~= "" and result.stderr or result.stdout or "")
            if live_term then
               live_term.heading_dirty = true
               live_term.heading_due_at = vim.uv.now() + opts.debounce_ms
            end
            state.heading_backoff_until = vim.uv.now() + 15000
            if error_text ~= "" and error_text ~= state.heading_failure_message then
               state.heading_failure_message = error_text
               vim.notify("cmax auto headings: " .. error_text, vim.log.levels.WARN)
            end
         end

         if not state.active_heading_job and vim.uv.now() >= state.heading_backoff_until then
            for _, pending_term in ipairs(state.terminals) do
               schedule_term_heading(pending_term)
            end
         end

         pump_term_heading_jobs()
      end)
   end)
end

pump_term_heading_jobs = function()
   local opts = state.auto_heading_opts
   if not opts or not opts.enabled then return end
   if state.active_heading_job then return end
   if vim.uv.now() < (state.heading_backoff_until or 0) then return end
   if vim.fn.executable("curl") ~= 1 then return end

   local best_term
   local best_request
   local best_due
   local now = vim.uv.now()

   for _, term in ipairs(state.terminals) do
      if term.heading_dirty and term.heading_target_key
         and term.heading_requested_key ~= term.heading_target_key then
         local due_at = term.heading_due_at or 0
         if due_at <= now then
            local request = build_term_heading_request(term)
            if request and request.key == term.heading_target_key then
               if not best_due or due_at < best_due then
                  best_term = term
                  best_request = request
                  best_due = due_at
               end
            end
         end
      end
   end

   if best_term and best_request then
      start_term_heading_job(best_term, best_request)
   end
end

local function open_confirmation_popup(text, yes_label, no_label, on_yes, on_no)
   local buttons = string.format("[%s]    [%s]", yes_label, no_label)
   local width = math.max(#text, #buttons) + 4
   local lines = {
      "+" .. string.rep("-", width - 2) .. "+",
      "| " .. text .. string.rep(" ", width - 3 - #text) .. "|",
      "|" .. string.rep(" ", width - 2) .. "|",
      "| " .. buttons .. string.rep(" ", width - 3 - #buttons) .. "|",
      "+" .. string.rep("-", width - 2) .. "+",
   }

   local popup_buf = vim.api.nvim_create_buf(false, true)
   vim.api.nvim_buf_set_lines(popup_buf, 0, -1, false, lines)
   vim.bo[popup_buf].modifiable = false
   vim.bo[popup_buf].bufhidden = "wipe"

   local popup_win = vim.api.nvim_open_win(popup_buf, true, {
      relative = "editor",
      row = math.floor((vim.o.lines - #lines) / 2),
      col = math.floor((vim.o.columns - width) / 2),
      width = width,
      height = #lines,
      style = "minimal",
      border = "none",
   })

   local function close_popup()
      if vim.api.nvim_win_is_valid(popup_win) then
         vim.api.nvim_win_close(popup_win, true)
      end
   end

   local function confirm()
      close_popup()
      if on_yes then on_yes() end
   end

   local function cancel()
      close_popup()
      if on_no then on_no() end
   end

   vim.keymap.set("n", string.lower(yes_label:sub(1, 1)), confirm, { buffer = popup_buf, nowait = true })
   vim.keymap.set("n", string.upper(yes_label:sub(1, 1)), confirm, { buffer = popup_buf, nowait = true })
   vim.keymap.set("n", "<CR>", confirm, { buffer = popup_buf, nowait = true })
   vim.keymap.set("n", string.lower(no_label:sub(1, 1)), cancel, { buffer = popup_buf, nowait = true })
   vim.keymap.set("n", string.upper(no_label:sub(1, 1)), cancel, { buffer = popup_buf, nowait = true })
   vim.keymap.set("n", "<Esc>", cancel, { buffer = popup_buf, nowait = true })
   vim.keymap.set("n", "q", cancel, { buffer = popup_buf, nowait = true })
end

local function remove_terminal(index)
   local term = state.terminals[index]
   if not term then return end

   if term.codex_session_path and state.codex_claimed_sessions[term.codex_session_path] == term.cmax_id then
      state.codex_claimed_sessions[term.codex_session_path] = nil
   end
   cleanup_term_files(term)

   if vim.api.nvim_buf_is_valid(term.buf) then
      pcall(vim.api.nvim_buf_delete, term.buf, { force = true })
   end

   table.remove(state.terminals, index)
   if state.selected > menu_item_count() then
      state.selected = menu_item_count()
   end
end

local function clear_all_terminals()
   for index = #state.terminals, 1, -1 do
      remove_terminal(index)
   end
   state.selected = 1
end

local function delete_terminal(index, on_done)
   local term = state.terminals[index]
   if not term then return end

   open_confirmation_popup("DELETE " .. term.name, "Yes", "No", function()
      remove_terminal(index)
      save_current_session()
      if on_done then on_done() end
   end)
end

local function normalize_launch_profile(key, profile)
   if type(profile) == "string" or vim.islist(profile) then
      profile = { command = profile, provider = "codex" }
   else
      profile = vim.deepcopy(profile or {})
   end

   profile.key = key
   profile.provider = profile.provider or "claude"
   return profile
end

local function normalize_launch_profiles(profiles)
   local normalized = {}
   for key, profile in pairs(profiles or {}) do
      normalized[key] = normalize_launch_profile(key, profile)
   end
   return normalized
end

local function ensure_codex_hooks_flag_in_command(cmd, provider)
   if provider ~= "codex" then return cmd end

   if type(cmd) == "table" then
      for i = 1, #cmd - 1 do
         if cmd[i] == "--enable" and cmd[i + 1] == "codex_hooks" then
            return cmd
         end
      end
      local out = { cmd[1], "--enable", "codex_hooks" }
      for i = 2, #cmd do
         table.insert(out, cmd[i])
      end
      return out
   end

   if type(cmd) == "string" and cmd:match("^%s*codex[%s$]") and not cmd:find("%-%-enable%s+codex_hooks") then
      return cmd:gsub("^%s*codex", "codex --enable codex_hooks", 1)
   end

   return cmd
end

local function build_command(opts)
   local provider = opts.provider or "claude"

   if opts.command ~= nil then
      return ensure_codex_hooks_flag_in_command(vim.deepcopy(opts.command), provider)
   end

   if provider == "codex" then
      local cmd = { provider, "--enable", "codex_hooks" }
      if opts.profile then
         table.insert(cmd, "-p")
         table.insert(cmd, opts.profile)
      end
      if opts.dangerously then
         table.insert(cmd, "--dangerously-bypass-approvals-and-sandbox")
      end
      if opts.resume then
         table.insert(cmd, "resume")
         if opts.session_id then
            table.insert(cmd, opts.session_id)
         end
      end
      return cmd
   end

   local cmd = { provider }
   if opts.dangerously then
      table.insert(cmd, "--dangerously-skip-permissions")
   end
   if opts.resume then
      table.insert(cmd, "-r")
      if opts.session_id then
         table.insert(cmd, opts.session_id)
      end
   end
   return cmd
end

local function setup_term_keymaps(term_buf)
   vim.keymap.set("t", "<C-]>", [[<C-\><C-n>]], { buffer = term_buf, nowait = true })
   vim.keymap.set("n", "<C-]>", "<Nop>", { buffer = term_buf, nowait = true })
   vim.keymap.set("t", "<C-_>", function()
      vim.schedule(show_menu)
   end, { buffer = term_buf, nowait = true, silent = true })
   vim.keymap.set("n", "-", function()
      show_menu()
   end, { buffer = term_buf, nowait = true })
end

local function create_terminal(cmd, opts)
   opts = opts or {}
   state.counter = state.counter + 1

   local provider = opts.provider or "claude"
   local title = provider_title(provider)
   local name = opts.label or (title .. " " .. state.counter)
   local cmax_id = vim.fn.getpid() .. "_" .. state.counter
   local cwd = opts.cwd or vim.fn.getcwd()
   local term_buf = vim.api.nvim_create_buf(false, true)
   vim.bo[term_buf].bufhidden = "hide"
   vim.api.nvim_win_set_buf(state.win, term_buf)

   local env = vim.fn.environ()
   env.CMAX_ID = cmax_id
   env.CMAX_PROVIDER = provider

   local job_id = vim.fn.termopen(cmd, {
      env = env,
      cwd = cwd,
      on_exit = function(_, _, _)
         vim.schedule(function()
            local term = find_terminal(cmax_id)
            if not term then return end
            term.status = "Session ended"
            cleanup_term_files(term)
            save_current_session()
            if state.menu_buf and vim.api.nvim_buf_is_valid(state.menu_buf)
               and state.win and vim.api.nvim_win_is_valid(state.win) then
               rerender_active_view()
            end
         end)
      end,
   })

   local term = {
      buf = term_buf,
      job_id = job_id,
      pid = vim.fn.jobpid(job_id),
      name = name,
      default_name = name,
      cmax_id = cmax_id,
      provider = provider,
      cwd = cwd,
      started_at = os.time(),
      status = opts.status,
      launch = {
         command = opts.command ~= nil and vim.deepcopy(opts.command) or nil,
         dangerously = opts.dangerously,
         profile = opts.profile,
      },
      codex_session_id = provider == "codex" and opts.session_id or nil,
      codex_session_path = provider == "codex" and opts.transcript_path or nil,
      claude_session_id = provider == "claude" and opts.session_id or nil,
   }

   table.insert(state.terminals, term)
   if term.codex_session_path then
      state.codex_claimed_sessions[term.codex_session_path] = term.cmax_id
   end
   if opts.last_prompt then
      set_term_prompt(term, opts.last_prompt)
   end
   if opts.generated_heading then
      set_term_heading(term, opts.generated_heading)
   else
      refresh_term_name(term)
   end

   setup_term_keymaps(term_buf)
   if opts.startinsert ~= false then
      vim.cmd("startinsert")
   end
   save_current_session()
   return term
end

local function open_selected(opts)
   opts = opts or {}
   local dangerously = opts.dangerously
   local provider = opts.provider or "claude"

   local index = state.selected
   if index <= #state.terminals then
      local term = state.terminals[index]
      if vim.api.nvim_buf_is_valid(term.buf) then
         vim.api.nvim_win_set_buf(state.win, term.buf)
         vim.cmd("startinsert")
      end
   else
      local cmd = build_command(opts)
      local title = provider_title(provider)
      local label = dangerously and (title .. " " .. (state.counter + 1) .. " (yolo)") or nil
      create_terminal(cmd, vim.tbl_extend("force", opts, {
         label = opts.label or label,
         provider = provider,
      }))
   end
end

local function restore_snapshot(snapshot)
   if type(snapshot) ~= "table" or not vim.islist(snapshot.terminals) then return end

   clear_all_terminals()

   state.multiplexer_session_id = snapshot.id or new_state_id()
   state.multiplexer_created_at = tonumber(snapshot.created_at) or os.time()
   state.selected = tonumber(snapshot.selected) or 1

   for _, item in ipairs(snapshot.terminals) do
      local opts = {
         provider = item.provider or "claude",
         cwd = item.cwd or snapshot.cwd or vim.fn.getcwd(),
         label = item.default_name or item.last_prompt or item.name,
         last_prompt = item.last_prompt,
         generated_heading = item.generated_heading,
         status = item.status,
         dangerously = item.dangerously,
         profile = item.profile,
         startinsert = false,
         transcript_path = item.transcript_path,
      }

      if item.session_id then
         opts.resume = true
         opts.session_id = item.session_id
      elseif item.command ~= nil then
         opts.command = item.command
      end

      local cmd = build_command(opts)
      create_terminal(cmd, opts)
   end

   if state.selected > menu_item_count() then
      state.selected = menu_item_count()
   end
   save_current_session()
end

local function restore_saved_session(index)
   local snapshot = state.saved_sessions[index]
   if not snapshot then return end

   local function do_restore()
      restore_snapshot(snapshot)
      show_menu()
   end

   if #state.terminals == 0 then
      do_restore()
      return
   end

   save_current_session()
   open_confirmation_popup("REPLACE current multiplexer session", "Replace", "Cancel", do_restore)
end

show_chat_search = function()
   local fzf = vim.fn.exepath("fzf")
   if fzf == "" then
      vim.notify("cmax: fzf is not installed", vim.log.levels.ERROR)
      return
   end

   local entries = collect_chat_search_entries()
   if #entries == 0 then
      vim.notify("cmax: no chat history found", vim.log.levels.INFO)
      return
   end

   local lines = {}
   for _, entry in ipairs(entries) do
      lines[#lines + 1] = encode_chat_search_line(entry)
   end

   local input_path = vim.fn.tempname()
   local output_path = vim.fn.tempname()
   if not write_file(input_path, table.concat(lines, "\n")) then
      vim.notify("cmax: failed to build chat search index", vim.log.levels.ERROR)
      return
   end

   local buf, win = open_chat_search_window()
   local cmd = string.format(
      "cat %s | %s --layout=reverse --info=inline --tiebreak=index --prompt=%s --header=%s --delimiter='\\t' --with-nth='5,6,7,8,9' --nth='5,6,7,8,9,10' --bind='ctrl-s:toggle-sort' > %s",
      vim.fn.shellescape(input_path),
      vim.fn.shellescape(fzf),
      vim.fn.shellescape("Chats> "),
      vim.fn.shellescape("Enter to resume the thread, ESC to cancel"),
      vim.fn.shellescape(output_path)
   )

   vim.fn.termopen({ vim.o.shell, vim.o.shellcmdflag, cmd }, {
      on_exit = function()
         vim.schedule(function()
            local selection = read_file(output_path)

            os.remove(input_path)
            os.remove(output_path)

            if vim.api.nvim_win_is_valid(win) then
               vim.api.nvim_win_close(win, true)
            end

            if vim.api.nvim_buf_is_valid(buf) then
               pcall(vim.api.nvim_buf_delete, buf, { force = true })
            end

            selection = selection and vim.trim(selection) or ""
            if selection == "" then return end

            local fields = vim.split(selection, "\t", { plain = true, trimempty = false })
            local provider = fields[1]
            local session_id = fields[2]
            local cwd = fields[3] or ""
            local transcript_path = fields[4] or ""
            local label = fields[9] or ""

            if provider ~= "codex" and provider ~= "claude" then return end
            open_chat_search_result(provider, session_id, cwd, transcript_path, label)
         end)
      end,
   })

   vim.cmd("startinsert")
end

show_saved_sessions = function(opts)
   opts = opts or {}
   local win = ensure_window()
   state.session_picker_return_to_menu = not not opts.return_to_menu
   load_saved_sessions()

   local buf = vim.api.nvim_create_buf(false, true)
   vim.bo[buf].bufhidden = "wipe"
   state.menu_buf = buf
   state.selected = 1

   vim.api.nvim_win_set_buf(win, buf)
   render_saved_sessions(win)

   vim.keymap.set("n", "j", function() select_item(state.selected + 1) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "k", function() select_item(state.selected - 1) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "G", function() select_item(saved_session_count()) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "gg", function() select_item(1) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "<C-d>", function()
      local half = math.max(1, math.floor(saved_session_count() / 2))
      select_item(state.selected + half)
   end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "<C-u>", function()
      local half = math.max(1, math.floor(saved_session_count() / 2))
      select_item(state.selected - half)
   end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "<CR>", function()
      restore_saved_session(state.selected)
   end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "f", show_chat_search, { buffer = buf, nowait = true })
   vim.keymap.set("n", "<Esc>", function()
      if state.session_picker_return_to_menu and state.win and vim.api.nvim_win_is_valid(state.win) then
         show_menu()
      elseif state.win and vim.api.nvim_win_is_valid(state.win) then
         vim.api.nvim_win_close(state.win, true)
         state.win = nil
         state.menu_buf = nil
      end
   end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "q", function()
      if state.session_picker_return_to_menu and state.win and vim.api.nvim_win_is_valid(state.win) then
         show_menu()
      elseif state.win and vim.api.nvim_win_is_valid(state.win) then
         vim.api.nvim_win_close(state.win, true)
         state.win = nil
         state.menu_buf = nil
      end
   end, { buffer = buf, nowait = true })

   vim.api.nvim_create_autocmd("WinResized", {
      buffer = buf,
      callback = function()
         if vim.api.nvim_win_is_valid(win) then
            render_saved_sessions(win)
         end
      end,
   })
end

show_menu = function()
   local win = ensure_window()

   local buf = vim.api.nvim_create_buf(false, true)
   vim.bo[buf].bufhidden = "wipe"
   state.menu_buf = buf
   state.view = "menu"

   vim.api.nvim_win_set_buf(win, buf)
   render_menu(win)

   vim.keymap.set("n", "j", function() select_item(state.selected + 1) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "k", function() select_item(state.selected - 1) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "G", function() select_item(menu_item_count()) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "gg", function() select_item(1) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "<C-d>", function()
      local half = math.max(1, math.floor(menu_item_count() / 2))
      select_item(state.selected + half)
   end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "<C-u>", function()
      local half = math.max(1, math.floor(menu_item_count() / 2))
      select_item(state.selected - half)
   end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "<C-f>", function() select_item(state.selected + menu_item_count()) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "<C-b>", function() select_item(state.selected - menu_item_count()) end, { buffer = buf, nowait = true })

   vim.keymap.set("n", "<CR>", function()
      open_selected()
   end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "r", function()
      if state.selected > #state.terminals then
         open_selected({ resume = true })
      end
   end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "c", function()
      if state.selected > #state.terminals then
         open_selected({ provider = "codex" })
      end
   end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "R", function()
      show_saved_sessions({ return_to_menu = true })
   end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "f", show_chat_search, { buffer = buf, nowait = true })
   vim.keymap.set("n", "s", prompt_tab_filter, { buffer = buf, nowait = true })
   for key, profile in pairs(state.launch_profiles) do
      vim.keymap.set("n", key, function()
         if state.selected > #state.terminals then
            open_selected(profile)
         end
      end, { buffer = buf, nowait = true })
   end
   vim.keymap.set("n", "d", function()
      if state.selected <= #state.terminals then
         delete_terminal(state.selected, function()
            render_menu(win)
         end)
      else
         open_selected({ dangerously = true })
      end
   end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "D", function()
      if state.selected > #state.terminals then
         open_selected({ dangerously = true, resume = true })
      end
   end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "q", function()
      vim.api.nvim_win_close(win, true)
      state.win = nil
      state.menu_buf = nil
   end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "<Esc>", function()
      vim.api.nvim_win_close(win, true)
      state.win = nil
      state.menu_buf = nil
   end, { buffer = buf, nowait = true })

   vim.api.nvim_create_autocmd("WinResized", {
      buffer = buf,
      callback = function()
         if vim.api.nvim_win_is_valid(win) then
            render_menu(win)
         end
      end,
   })
end

local function poll_statuses()
   local changed = poll_codex_history()
   if poll_claude_history() then
      changed = true
   end

   for _, term in ipairs(state.terminals) do
      if update_term_session_from_file(term) then
         changed = true
      end

      if term.provider == "codex" then
         if resolve_codex_session(term) then
            changed = true
         end
         if term.codex_session_id and set_term_prompt(term, state.codex_latest_prompts[term.codex_session_id]) then
            changed = true
         end
      elseif term.provider == "claude" then
         if resolve_claude_session(term) then
            changed = true
         end
         if term.claude_session_id and set_term_prompt(term, state.claude_latest_prompts[term.claude_session_id]) then
            changed = true
         end
      end

      local status = read_file(status_dir .. "/" .. term.cmax_id)
      if status and status ~= term.status then
         term.status = status
         changed = true
      end

      local prompt = read_file(status_dir .. "/" .. term.cmax_id .. ".prompt")
      if prompt and set_term_prompt(term, prompt) then
         changed = true
      end

      schedule_term_heading(term)
   end

   pump_term_heading_jobs()

   if not changed then return end

   save_current_session()

   if state.menu_buf and vim.api.nvim_buf_is_valid(state.menu_buf)
      and state.win and vim.api.nvim_win_is_valid(state.win) then
      vim.schedule(function()
         if not (state.win and vim.api.nvim_win_is_valid(state.win)) then return end
         rerender_active_view()
      end)
   end
end

function M.setup(opts)
   opts = opts or {}
   state.launch_profiles = normalize_launch_profiles(opts.launch_profiles)
   state.auto_heading_opts = normalize_auto_heading_opts(opts.auto_headings)
   state.tab_filter_opts = normalize_tab_filter_opts(opts.tab_filter)

   install_hooks()
   pcall(vim.fn.mkdir, cmax_sessions_dir, "p")

   state.poll_timer = vim.uv.new_timer()
   state.poll_timer:start(1000, 500, vim.schedule_wrap(poll_statuses))

   vim.api.nvim_create_autocmd("VimLeavePre", {
      callback = function()
         save_current_session()
         if state.poll_timer then
            state.poll_timer:stop()
            state.poll_timer:close()
         end
         for _, term in ipairs(state.terminals) do
            cleanup_term_files(term)
         end
         os.remove(status_dir)
      end,
   })

   vim.keymap.set("n", "<Leader>cc", function()
      if state.win and vim.api.nvim_win_is_valid(state.win) then
         vim.api.nvim_set_current_win(state.win)
      end
      show_menu()
   end)

   vim.keymap.set("n", "<Leader>cR", function()
      local return_to_menu = state.win and vim.api.nvim_win_is_valid(state.win)
      show_saved_sessions({ return_to_menu = return_to_menu })
   end)

   vim.keymap.set("n", "<Leader>cf", show_chat_search)
   vim.keymap.set("n", "<Leader>cs", prompt_tab_filter)
end

return M
