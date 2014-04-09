local mod_name = (...):match ( "^(.*)%..-$" )

local assert , error = assert , error
local pairs = pairs
local getmetatable = getmetatable
local type = type
local tonumber , tostring = tonumber , tostring
local t_insert = table.insert
local t_concat = table.concat
local t_maxn = table.maxn
local strformat = string.format
local strmatch = string.match
local strbyte = string.byte

local Array = Array
local Id = Id

local le_uint_to_num = le_uint_to_num
local le_int_to_num = le_int_to_num
local num_to_le_uint = num_to_le_uint
local num_to_le_int = num_to_le_int
local from_double = from_double
local to_double = to_double

local getlib = require(mod_name..".get")
local read_terminated_string = getlib.read_terminated_string

local binary_mt = {}
local utc_date = {}

local function read_document(get, numerical)
  local bytes = le_uint_to_num(get(4))

  local t = numerical and Array() or {}
  while true do
    local op = get(1)
    if op == "\0" then break end

    local e_name = read_terminated_string(get)
    local v
    if op == "\1" then -- Double
      v = from_double(get(8))
    elseif op == "\2" then -- String
      local len = le_uint_to_num(get(4))
      v = get(len - 1)
      assert(get(1) == "\0")
    elseif op == "\3" then -- Embedded document
      v = read_document(get, false)
    elseif op == "\4" then -- Array
      v = read_document(get, true)
    elseif op == "\5" then -- Binary
      local len = le_uint_to_num(get(4))
      local subtype = get(1)
      v = get(len)
    elseif op == "\7" then -- Id
      v = Id(get(12))
    elseif op == "\8" then -- false
      local f = get(1)
      if f == "\0" then
        v = false
      elseif f == "\1" then
        v = true
      else
        error(f:byte())
      end
    elseif op == "\9" then -- UTC datetime milliseconds
      v = le_uint_to_num(get(8), 1, 8)
    elseif op == "\10" then -- Null
      v = nil
    elseif op == "\16" then --int32
      v = le_int_to_num(get(4), 1, 8)
    elseif op == "\17" then --int64 // timestamp
      v = le_int_to_num(get(8), 1, 8)
    elseif op == "\18" then --int64
      v = le_int_to_num(get(8), 1, 8)
    else
      error ( "Unknown BSON type: " .. strbyte(op))
    end

    if numerical then
      t[tonumber(e_name) + 1] = v
    else
      t[e_name] = v
    end
  end
  return t
end

local function get_utc_date(v)
  return setmetatable({v = v}, utc_date)
end

local function get_bin_data(v)
  return setmetatable({v = v, st = "\0"}, binary_mt)
end

local function from_bson(get)
  return read_document(get, false)
end

local to_bson
local function pack(k, v)
  local ot = type(v)
  local mt = getmetatable(v)

  if ot == "number" then
    if math.floor(v) ~= v then
      return "\1" .. k .. "\0" .. to_double ( v )
    elseif v > 2147483647 or v < -2147483648 then -- 64bit
      return "\18" .. k .. "\0" .. num_to_le_int ( v , 8 )
    else -- 32bit
      return "\16" .. k .. "\0" .. num_to_le_int ( v , 4 )
    end
  elseif ot == "nil" then
    return "\10" .. k .. "\0"
  elseif ot == "userdata" then
    return "\10" .. k .. "\0"
  elseif ot == "string" then
    return "\2" .. k .. "\0" .. num_to_le_uint ( #v + 1 ) .. v .. "\0"
  elseif ot == "boolean" then
    if v == false then
      return "\8" .. k .. "\0\0"
    else
      return "\8" .. k .. "\0\1"
    end
  elseif mt == Id then
    return "\7" .. k .. "\0" .. v.id
  elseif mt == utc_date then
    return "\9" .. k .. "\0" .. num_to_le_int(v.v, 8)
  elseif mt == binary_mt then
    return "\5" .. k .. "\0" .. num_to_le_uint(string.len(v.v)) .. 
    v.st .. v.v
  elseif ot == "table" then
    local doc , array = to_bson(v)
    if array then
      return "\4" .. k .. "\0" .. doc
    else
      return "\3" .. k .. "\0" .. doc
    end
  else
    error ( "Failure converting " .. ot ..": " .. tostring ( v ) )
  end
end

function to_bson(ob)
  -- Find out if ob if an array; or a table
  local is_array = true
  local max = 0
  if Array then
    if getmetatable(ob) == Array then
      max = t_maxn(ob)
    else
      is_array = false
    end
  else
    for k, v in pairs(ob) do
      local t_k = type(k)
      if is_array then
        if t_k == "number" and k >= 1 then
          if k >= max then
            max = k
          end
        else
          is_array = false
        end
      end
      if not is_array then break end
    end
  end

  local m
  if is_array then
    local r = {}
    for i = 1, max do
      r[i] = pack(i - 1, ob[i])
    end
    m = t_concat(r, "", 1, max)
  else
    local r = {}
    for k, v in pairs(ob) do
      t_insert(r, pack(tostring(k), v))
    end
    m = t_concat(r)
  end
  return num_to_le_uint(#m + 4 + 1 )..m.."\0", is_array
end

return {
  from_bson = from_bson;
  to_bson = to_bson;
  get_bin_data = get_bin_data;
  get_utc_date = get_utc_date;
}
