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
   session_picker_return_to_menu = false,
   codex_claimed_sessions = {},
   codex_history_offset = 0,
   codex_latest_prompts = {},
   codex_session_cache = {},
   claude_history_offset = 0,
   claude_latest_prompts = {},
   launch_profiles = {},
   multiplexer_session_id = nil,
   multiplexer_created_at = nil,
}

local status_dir = (vim.env.TMPDIR or "/tmp") .. "/cmax-status"
local claude_hook_script_path = vim.fn.expand("~/.claude/hooks/cmax-status.py")
local claude_old_hook_script_path = vim.fn.expand("~/.claude/hooks/cmax-status.sh")
local claude_settings_path = vim.fn.expand("~/.claude/settings.json")
local claude_history_path = vim.fn.expand("~/.claude/history.jsonl")
local claude_sessions_path = vim.fn.expand("~/.claude/sessions")
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

local function set_term_prompt(term, prompt)
   prompt = sanitize_prompt(prompt)
   if not prompt or prompt == term.last_prompt then return false end
   term.last_prompt = prompt
   term.name = prompt
   return true
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

local function provider_title(provider)
   if provider == "codex" then
      return "Codex"
   end
   if provider == "claude" then
      return "Claude"
   end
   return provider:gsub("^%l", string.upper)
end

local function provider_session_id(term)
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
      last_prompt = term.last_prompt,
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

local function poll_history(path, offset_key, prompt_store, extract)
   local stat = vim.uv.fs_stat(path)
   if not stat or stat.type ~= "file" then return false end

   local offset = state[offset_key] or 0
   if stat.size < offset then
      offset = 0
      state[offset_key] = 0
      clear_table(prompt_store)
   end
   if stat.size == offset then return false end

   local f = io.open(path, "r")
   if not f then return false end
   f:seek("set", offset)

   local changed = false
   for line in f:lines() do
      local entry = decode_json_line(line)
      local session_id, prompt = extract(entry)
      prompt = sanitize_prompt(prompt)
      if session_id and prompt and prompt_store[session_id] ~= prompt then
         prompt_store[session_id] = prompt
         changed = true
      end
   end

   state[offset_key] = f:seek() or stat.size
   f:close()
   return changed
end

local function poll_codex_history()
   return poll_history(codex_history_path, "codex_history_offset", state.codex_latest_prompts, function(entry)
      if type(entry) ~= "table" then return nil end
      return entry.session_id, entry.text
   end)
end

local function poll_claude_history()
   return poll_history(claude_history_path, "claude_history_offset", state.claude_latest_prompts, function(entry)
      if type(entry) ~= "table" then return nil end
      return entry.sessionId or entry.session_id, entry.display
   end)
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

local function get_codex_session_meta(path)
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

local function current_item_count()
   if state.view == "sessions" then
      return saved_session_count()
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
      labels[#labels + 1] = term.last_prompt or term.name or (provider_title(term.provider) .. " " .. i)
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

local function find_terminal(cmax_id)
   for _, term in ipairs(state.terminals) do
      if term.cmax_id == cmax_id then
         return term
      end
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
               if state.view == "sessions" then
                  render_saved_sessions(state.win)
               else
                  render_menu(state.win)
               end
            end
         end)
      end,
   })

   local term = {
      buf = term_buf,
      job_id = job_id,
      pid = vim.fn.jobpid(job_id),
      name = name,
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
         label = item.last_prompt or item.name,
         last_prompt = item.last_prompt,
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
   end

   if not changed then return end

   save_current_session()

   if state.menu_buf and vim.api.nvim_buf_is_valid(state.menu_buf)
      and state.win and vim.api.nvim_win_is_valid(state.win) then
      vim.schedule(function()
         if not (state.win and vim.api.nvim_win_is_valid(state.win)) then return end
         if state.view == "sessions" then
            render_saved_sessions(state.win)
         else
            render_menu(state.win)
         end
      end)
   end
end

function M.setup(opts)
   opts = opts or {}
   state.launch_profiles = normalize_launch_profiles(opts.launch_profiles)

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
end

return M
