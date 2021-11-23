
--Tarantool client for LuaJIT.
--Written by Cosmin Apreutesei. Public Domain.

if not ... then require'test'; return end

local ffi     = require'ffi'
local bit     = require'bit'
local mp      = require'messagepack'
local sha1    = require'sha1'.sha1
local errors  = require'errors'
local glue    = require'glue'

local u8a     = glue.u8a
local u8p     = glue.u8p
local buffer  = glue.buffer
local empty   = glue.empty
local memoize = glue.memoize

local check_io, check, protect = errors.tcp_protocol_errors'tarantool'

local c = {host = '127.0.0.1', port = 3301, timeout = 2}

-- packet codes
local OK         = 0
local SELECT     = 1
local INSERT     = 2
local REPLACE    = 3
local UPDATE     = 4
local DELETE     = 5
local CALL       = 6
local AUTH       = 7
local EVAL       = 8
local UPSERT     = 9
local PING       = 64
local ERROR_TYPE = 65536

-- packet keys
local TYPE          = 0x00
local SYNC          = 0x01
local SPACE_ID      = 0x10
local INDEX_ID      = 0x11
local LIMIT         = 0x12
local OFFSET        = 0x13
local ITERATOR      = 0x14
local KEY           = 0x20
local TUPLE         = 0x21
local FUNCTION_NAME = 0x22
local USER_NAME     = 0x23
local OPS           = 0x28
local DATA          = 0x30
local ERROR         = 0x31

-- default spaces
local SPACE_SCHEMA  = 272
local SPACE_SPACE   = 280
local SPACE_INDEX   = 288
local SPACE_FUNC    = 296
local SPACE_USER    = 304
local SPACE_PRIV    = 312
local SPACE_CLUSTER = 320

-- default views
local VIEW_SPACE    = 281
local VIEW_INDEX    = 289

-- index info
local INDEX_SPACE_PRIMARY = 0
local INDEX_SPACE_NAME    = 2
local INDEX_INDEX_PRIMARY = 0
local INDEX_INDEX_NAME    = 2

local function line(b, n)
	local j
	for i = 0, n-1 do
		if b[i] == 10 then j = i; break end
	end
	return j and ffi.string(b, j)
end

local function xor_strings(s1, s2)
	assert(#s1 == #s2)
	local n = #s1
	local p1 = ffi.cast(u8p, s1)
	local p2 = ffi.cast(u8p, s2)
	local b = u8a(n)
	for i = 0, n-1 do
		b[i] = bit.bxor(p1[i], p2[i])
	end
	return ffi.string(b, n)
end

local request, select --fw. decl.

c.connect = protect(function(c)
	if not c.tcp then
		local sock = require'sock'
		c.tcp = sock.tcp
		c.clock = sock.clock
	end
	c.tcp = check_io(q, c.tcp()) --pin it so that it's closed automatically on error.
	local expires = c.clock() + c.timeout
	check_io(c, c.tcp:connect(c.host, c.port, expires))
	c._b = buffer()
	local b = c._b(64)
	local greeting = line(check_io(c, c.tcp:recvn(b, 64, expires)))
	local salt     = line(check_io(c, c.tcp:recvn(b, 64, expires)))
	if not c.user then
		return true
	end
	local rbody = {[USER_NAME] = c.user, [TUPLE] = {}}
	local password = c.password or ''
	if password ~= '' then
		local s1 = sha1(password)
		local s2 = sha1(s1)
		local s3 = sha1(salt .. s2)
		local scramble = xor_strings(s1, s3)
		rbody[TUPLE] = {'chap-sha1', scramble}
	end
	return request(c, {[TYPE] = AUTH}, rbody, expires)
end)

c.close = function(c)
	return c.tcp:close()
end

--[[local]] function request(c, header, body, expires)
	c.sync_num = ((c.sync_num or 0) + 1) % 100000
	header[SYNC] = c.sync_num
	local header = mp.pack(header)
	local body = mp.pack(body)
	local len = mp.pack(#header + #body)
	local request = len .. header .. body
	check_io(c, c.tcp:send(request))
	local size = ffi.string(check_io(c, c.tcp:recvn(c._b(5), 5, expires)), 5)
	local size = check(c, mp.unpack(size))
	local s = ffi.string(check_io(c, c.tcp:recvn(c._b(size), size, expires)), size)
	local unpack_next = mp.unpacker(s)
	local _, res_header = check(c, unpack_next())
	check(c, type(res_header) == 'table')
	check(c, res_header[SYNC] == c.sync_num)
	local _, res_body = check(c, unpack_next())
	check(c, type(res_body) == 'table')
	return {
		code  = res_header[TYPE],
		data  = res_body[DATA],
		error = res_body[ERROR],
	}
end

local function resolve_space(c, space)
	return type(space) == 'number' and space or c._lookup_space(space)
end

local function resolve_index(c, space, index)
	local spaceno = check(c, resolve_space(c, space), 'no space %s', space)
	return type(index) == 'number' and index or c._lookup_index(spaceno, index)
end

c.enable_lookups = function(c)
	c._lookup_space = memoize(function(space)
		local t = select(c, VIEW_SPACE, INDEX_SPACE_NAME, space)
		check(c, type(t) == 'table')
		return t[1] and t[1][1]
	end)
	c._lookup_index = memoize(function(spaceno, index)
		if not spaceno then return end
		local t = select(c, VIEW_INDEX, INDEX_INDEX_NAME, {spaceno, index})
		check(c, type(t) == 'table')
		return t[1] and t[1][2]
	end)
end

c.disable_lookups = function(c)
	c._lookup_space = nil
	c._lookup_index = nil
end

--[[local]] function select(c, space, index, key, opt)
	opt = opt or empty
	local spaceno = check(c, resolve_space(c, space), 'no space %s', space)
	local indexno = check(c, resolve_index(c, spaceno, index or 'primary'), 'no index %s', index)
	local body = {
		[SPACE_ID] = spaceno,
		[INDEX_ID] = indexno,
		[KEY] = type(key) == 'table' and key or key == nil and {} or {key},
	}
	body[LIMIT] = opt.limit or 0xFFFFFFFF
	body[OFFSET] = opt.offset or 0
	body[ITERATOR] = opt.iterator
	local res = request(c, {[TYPE] = SELECT}, body)
	check(c, res.code == OK, res.error)
	return res.data
end

c.select = protect(select)

function c.new(c1)
	setmetatable(c1, c1).__index = c
	c1:enable_lookups()
	return c1
end

return c
