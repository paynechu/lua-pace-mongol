local type = type
local require = require
local assert = assert
local setmetatable = setmetatable

local string_byte = string.byte
local string_format = string.format
local table_insert = table.insert
local table_concat = table.concat
local os_time = os.time
local io_popen = io.popen

local hasposix, posix = pcall(require , "posix")

local ngx = ngx
local ngx_decode_base64 = ngx.decode_base64
local ngx_encode_base64 = ngx.encode_base64
local ngx_md5_bin = ngx.md5_bin

module(...)

local ll = require(_PACKAGE.."ll")
local num_to_le_uint = ll.num_to_le_uint
local num_to_be_uint = ll.num_to_be_uint
local be_uint_tonum = ll.be_uint_to_num

local function _tostring(ob)
  return ngx_encode_base64(ob.id):gsub('!', '+'):gsub('%.', '/')
end

local machineid
local function _get_os_machineid()
  if hasposix then
    machineid = posix.uname("%n")
  else
    machineid = assert(io_popen("uname -n")):read("*l")
  end
  machineid = ngx_md5_bin(machineid):sub(1, 3)
  return machineid
end

local pid
local function _get_os_pid()
  pid = num_to_le_uint(ngx.var.pid, 2)
  return pid 
end

local inc = 0
local function _generate_id()
  inc = inc + 1
  -- "A BSON ObjectID is a 12-byte value consisting of a 4-byte timestamp (seconds since epoch), a 3-byte machine id, a 2-byte process id, and a 3-byte counter. Note that the timestamp and counter fields must be stored big endian unlike the rest of BSON"
  return num_to_be_uint(os_time(), 4) .. (machineid or _get_os_machineid()) .. (pid or _get_os_pid()) .. num_to_be_uint(inc, 3)
end

local _new = function(cls, id) 
  assert(id == nil or type(id) == 'string')
  if id then
    local len = #id
    assert(len == 12 or len == 16)
    if len == 16 then
      id = ngx_decode_base64((id:gsub('%+', '!'):gsub('/', '.')))
    end
  end
  local o = { id = id or _generate_id() }
  setmetatable(o, cls)
  return o
end

__index = _M
__tostring = _tostring
__eq = function (a, b)
  return a.id == b.id
end

setmetatable(_M, { __call = _new })

tostring = _tostring

function get_ts(ob)
  return be_uint_to_num(ob.id, 1, 4)
end

function get_pid(ob)
  return be_uint_to_num(ob.id, 8, 9)
end

function get_hostname(ob)
  local t = {}
  for i = 5, 7 do
    table_insert(t, string_format("%02x", string_byte(ob.id, i, i))) 
  end
  return table_concat(t)
end

function get_inc(ob)
  return be_uint_to_num(ob.id, 10, 12)
end
