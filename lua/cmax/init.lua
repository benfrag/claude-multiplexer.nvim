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
}

local status_dir = (vim.env.TMPDIR or "/tmp") .. "/cmax-status"
local hook_script_path = vim.fn.expand("~/.claude/hooks/cmax-status.sh")
local settings_path = vim.fn.expand("~/.claude/settings.json")

local item_height = 5
local inner_height = item_height - 2

local HOOK_SCRIPT = [[#!/bin/bash
[ -z "$CMAX_ID" ] && exit 0
STATUS_DIR="${TMPDIR:-/tmp}/cmax-status"
mkdir -p "$STATUS_DIR"
INPUT=$(cat)
read -r EVENT NTYPE <<< $(echo "$INPUT" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print(d.get('hook_event_name',''), d.get('notification_type',''))
" 2>/dev/null)
case "$EVENT" in
  Stop)                STATUS="Waiting for input" ;;
  UserPromptSubmit)    STATUS="Working..." ;;
  PermissionRequest)   STATUS="Needs permission" ;;
  SessionStart)        STATUS="Starting..." ;;
  SessionEnd)          STATUS="Session ended" ;;
  PostToolUseFailure)  STATUS="Tool error" ;;
  PreCompact)          STATUS="Compacting..." ;;
  SubagentStart)       STATUS="Working (subagent)..." ;;
  Notification)
    case "$NTYPE" in
      permission_prompt) STATUS="Needs permission" ;;
      idle_prompt)       STATUS="Idle" ;;
      *)                 exit 0 ;;
    esac ;;
  *)                   exit 0 ;;
esac
TMPFILE="$STATUS_DIR/.cmax_${CMAX_ID}.tmp"
printf '%s' "$STATUS" > "$TMPFILE"
mv -f "$TMPFILE" "$STATUS_DIR/$CMAX_ID"
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

local function install_hooks()
   vim.fn.mkdir(vim.fn.fnamemodify(hook_script_path, ":h"), "p")
   local f = io.open(hook_script_path, "w")
   if not f then return end
   f:write(HOOK_SCRIPT)
   f:close()
   vim.fn.system({ "chmod", "+x", hook_script_path })

   local sf = io.open(settings_path, "r")
   if not sf then return end
   local content = sf:read("*a")
   sf:close()

   local ok, settings = pcall(vim.json.decode, content)
   if not ok or type(settings) ~= "table" then return end

   settings.hooks = settings.hooks or {}
   local cmax_hook = {
      type = "command",
      command = hook_script_path,
      timeout = 5,
      async = true,
   }

   local changed = false
   for _, event in ipairs(HOOK_EVENTS) do
      settings.hooks[event] = settings.hooks[event] or {}
      local found = false
      for _, entry in ipairs(settings.hooks[event]) do
         local hooks_list = entry.hooks or {}
         for _, h in ipairs(hooks_list) do
            if h.command == hook_script_path then
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
      local out = io.open(settings_path, "w")
      if out then
         out:write(vim.json.encode(settings))
         out:close()
      end
   end
end

local function item_count()
   return #state.terminals + 1
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

local function render_menu(win)
   local buf = state.menu_buf
   if not buf or not vim.api.nvim_buf_is_valid(buf) then return end

   local box_width = vim.api.nvim_win_get_width(win)
   if box_width < 6 then box_width = 6 end
   local inner_width = box_width - 4
   local top    = "┌" .. string.rep("─", box_width - 2) .. "┐"
   local bottom = "└" .. string.rep("─", box_width - 2) .. "┘"
   local empty  = "│" .. string.rep(" ", box_width - 2) .. "│"

   local function pad(text)
      local len = vim.fn.strdisplaywidth(text)
      if len > inner_width then
         text = text:sub(1, inner_width - 1) .. "…"
         len = inner_width
      end
      return "│ " .. text .. string.rep(" ", inner_width - len) .. " │"
   end

   local labels = {}
   for _, term in ipairs(state.terminals) do
      table.insert(labels, term.name)
   end
   table.insert(labels, "+ New")

   local lines = {}
   state.status_lines = {}
   for i, label in ipairs(labels) do
      table.insert(lines, top)
      table.insert(lines, pad(label))
      local term = state.terminals[i]
      if term and term.status and term.status ~= "" then
         table.insert(lines, pad("  ● " .. term.status))
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

   if state.selected > item_count() then
      state.selected = item_count()
   end
   apply_highlights(buf)

   local target = (state.selected - 1) * item_height + 1
   if vim.api.nvim_win_is_valid(win) then
      vim.api.nvim_win_set_cursor(win, { target, 0 })
   end
end

local show_menu

local function setup_term_keymaps(term_buf)
   vim.keymap.set("t", "<C-]>", [[<C-\><C-n>]], { buffer = term_buf, nowait = true })
   vim.keymap.set("n", "<C-]>", "<Nop>", { buffer = term_buf, nowait = true })
   vim.keymap.set("t", "<C-_>", function()
      vim.cmd("stopinsert")
      show_menu()
   end, { buffer = term_buf, nowait = true })
   vim.keymap.set("n", "-", function()
      show_menu()
   end, { buffer = term_buf, nowait = true })
end

local function create_terminal(cmd, label)
   state.counter = state.counter + 1
   local name = label or ("Claude " .. state.counter)
   local cmax_id = vim.fn.getpid() .. "_" .. state.counter
   local term_buf = vim.api.nvim_create_buf(false, true)
   vim.bo[term_buf].bufhidden = "hide"
   vim.api.nvim_win_set_buf(state.win, term_buf)

   local env = vim.fn.environ()
   env["CMAX_ID"] = cmax_id
   vim.fn.termopen(cmd, { env = env })

   table.insert(state.terminals, { buf = term_buf, name = name, cmax_id = cmax_id })
   setup_term_keymaps(term_buf)
   vim.cmd("startinsert")
end

local function select_item(index)
   if index < 1 then index = 1 end
   local count = item_count()
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

local function delete_terminal(index, on_done)
   local term = state.terminals[index]
   local name = term.name

   local text = "DELETE " .. name
   local buttons = "[Y]es    [N]o"
   local width = math.max(#text, #buttons) + 4
   local lines = {
      "┌" .. string.rep("─", width - 2) .. "┐",
      "│ " .. text .. string.rep(" ", width - 3 - #text) .. "│",
      "│" .. string.rep(" ", width - 2) .. "│",
      "│ " .. buttons .. string.rep(" ", width - 3 - #buttons) .. "│",
      "└" .. string.rep("─", width - 2) .. "┘",
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
      if vim.api.nvim_buf_is_valid(term.buf) then
         vim.api.nvim_buf_delete(term.buf, { force = true })
      end
      os.remove(status_dir .. "/" .. term.cmax_id)
      table.remove(state.terminals, index)
      if state.selected > item_count() then
         state.selected = item_count()
      end
      if on_done then on_done() end
   end

   vim.keymap.set("n", "y", confirm, { buffer = popup_buf, nowait = true })
   vim.keymap.set("n", "Y", confirm, { buffer = popup_buf, nowait = true })
   vim.keymap.set("n", "<CR>", confirm, { buffer = popup_buf, nowait = true })
   vim.keymap.set("n", "n", close_popup, { buffer = popup_buf, nowait = true })
   vim.keymap.set("n", "N", close_popup, { buffer = popup_buf, nowait = true })
   vim.keymap.set("n", "<Esc>", close_popup, { buffer = popup_buf, nowait = true })
   vim.keymap.set("n", "q", close_popup, { buffer = popup_buf, nowait = true })
end

local function open_selected(dangerously)
   local index = state.selected
   if index <= #state.terminals then
      local term = state.terminals[index]
      if vim.api.nvim_buf_is_valid(term.buf) then
         vim.api.nvim_win_set_buf(state.win, term.buf)
         vim.cmd("startinsert")
      end
   else
      local cmd = dangerously and "claude --dangerously-skip-permissions" or "claude"
      local label = dangerously
         and ("Claude " .. (state.counter + 1) .. " (yolo)")
         or nil
      create_terminal(cmd, label)
   end
end

show_menu = function()
   local win = state.win
   if not win or not vim.api.nvim_win_is_valid(win) then return end

   local buf = vim.api.nvim_create_buf(false, true)
   vim.bo[buf].bufhidden = "wipe"
   state.menu_buf = buf

   vim.api.nvim_win_set_buf(win, buf)
   render_menu(win)

   vim.keymap.set("n", "j", function() select_item(state.selected + 1) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "k", function() select_item(state.selected - 1) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "G", function() select_item(item_count()) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "gg", function() select_item(1) end, { buffer = buf, nowait = true })
   local half = math.max(1, math.floor(item_count() / 2))
   vim.keymap.set("n", "<C-d>", function() select_item(state.selected + half) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "<C-u>", function() select_item(state.selected - half) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "<C-f>", function() select_item(state.selected + item_count()) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "<C-b>", function() select_item(state.selected - item_count()) end, { buffer = buf, nowait = true })

   vim.keymap.set("n", "<CR>", function() open_selected(false) end, { buffer = buf, nowait = true })
   vim.keymap.set("n", "d", function()
      if state.selected <= #state.terminals then
         delete_terminal(state.selected, function()
            render_menu(win)
         end)
      else
         open_selected(true)
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
   local changed = false
   for _, term in ipairs(state.terminals) do
      local path = status_dir .. "/" .. term.cmax_id
      local f = io.open(path, "r")
      if f then
         local status = f:read("*a")
         f:close()
         if status ~= term.status then
            term.status = status
            changed = true
         end
      end
   end
   if changed and state.menu_buf and vim.api.nvim_buf_is_valid(state.menu_buf)
      and state.win and vim.api.nvim_win_is_valid(state.win) then
      vim.schedule(function()
         render_menu(state.win)
      end)
   end
end

function M.setup(opts)
   opts = opts or {}

   install_hooks()

   state.poll_timer = vim.uv.new_timer()
   state.poll_timer:start(1000, 500, vim.schedule_wrap(poll_statuses))

   vim.api.nvim_create_autocmd("VimLeavePre", {
      callback = function()
         if state.poll_timer then
            state.poll_timer:stop()
            state.poll_timer:close()
         end
         for _, term in ipairs(state.terminals) do
            os.remove(status_dir .. "/" .. term.cmax_id)
         end
         os.remove(status_dir)
      end,
   })

   vim.keymap.set("n", "<Leader>cc", function()
      if state.win and vim.api.nvim_win_is_valid(state.win) then
         vim.api.nvim_set_current_win(state.win)
         show_menu()
         return
      end

      vim.cmd("vsplit")
      state.win = vim.api.nvim_get_current_win()
      vim.wo[state.win].wrap = false
      vim.wo[state.win].number = false
      vim.wo[state.win].relativenumber = false
      vim.wo[state.win].signcolumn = "no"
      vim.wo[state.win].cursorline = false

      show_menu()
   end)
end

return M
