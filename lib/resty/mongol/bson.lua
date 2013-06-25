local mod_name = (...):match ( "^(.*)%..-$" )

local assert , error = assert , error
local pairs = pairs
local getmetatable = getmetatable
local type = type
local tonumber , tostring = tonumber , tostring
local t_insert = table.insert
local t_concat = table.concat
local strformat = string.format
local strmatch = string.match
local strbyte = string.byte

local ll = require ( mod_name .. ".ll" )
local le_uint_to_num = ll.le_uint_to_num
local le_int_to_num = ll.le_int_to_num
local num_to_le_uint = ll.num_to_le_uint
local num_to_le_int = ll.num_to_le_int
local from_double = ll.from_double
local to_double = ll.to_double

local getlib = require ( mod_name .. ".get" )
local read_terminated_string = getlib.read_terminated_string

local obid = require ( mod_name .. ".object_id" )
local nbson = require ( mod_name .. ".bson-lua.bson" )
local new_object_id = obid.new
local object_id_mt = obid.metatable
local binary_mt = {}
local utc_date = {}


local function read_document ( get , numerical )
	local bytes = le_uint_to_num ( get ( 4 ) )

	local ho , hk , hv = false , false , false
	local t = { }
	while true do
		local op = get ( 1 )
		if op == "\0" then break end

		local e_name = read_terminated_string ( get )
		local v
		if op == "\1" then -- Double
			v = from_double ( get ( 8 ) )
		elseif op == "\2" then -- String
			local len = le_uint_to_num ( get ( 4 ) )
			v = get ( len - 1 )
			assert ( get ( 1 ) == "\0" )
		elseif op == "\3" then -- Embedded document
			v = read_document ( get , false )
		elseif op == "\4" then -- Array
			v = read_document ( get , true )
		elseif op == "\5" then -- Binary
			local len = le_uint_to_num ( get ( 4 ) )
			local subtype = get ( 1 )
			v = get ( len )
		elseif op == "\7" then -- ObjectId
			v = new_object_id ( get ( 12 ) )
		elseif op == "\8" then -- false
			local f = get ( 1 )
			if f == "\0" then
				v = false
			elseif f == "\1" then
				v = true
			else
				error ( f:byte ( ) )
			end
		elseif op == "\9" then -- UTC datetime milliseconds
			v = le_uint_to_num ( get ( 8 ) , 1 , 8 )
		elseif op == "\10" then -- Null
			v = nil
		elseif op == "\16" then --int32
			v = le_int_to_num ( get ( 4 ) , 1 , 8 )
        elseif op == "\17" then --int64
            v = le_int_to_num(get(8), 1, 8)
        elseif op == "\18" then --int64
            v = le_int_to_num(get(8), 1, 8)
		else
			error ( "Unknown BSON type: " .. strbyte ( op ) )
		end

		if numerical then
			t [ tonumber ( e_name ) ] = v
		else
			t [ e_name ] = v
		end

		-- Check for special universal map
		if e_name == "_keys" then
			hk = v
		elseif e_name == "_vals" then
			hv = v
		else
			ho = true
		end
	end

	if not ho and hk and hv then
		t = { }
			for i=1,#hk do
			t [ hk [ i ] ] = hv [ i ]
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

local function from_bson ( get )
	local t = read_document ( get , false )
	return t
end

local to_bson
function to_bson(ob)
	return nbson.encode(ob)
end

return {
	from_bson = from_bson ;
	to_bson = to_bson ;
    get_bin_data = get_bin_data;
    get_utc_date = get_utc_date;
}
