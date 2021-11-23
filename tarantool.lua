
--Tarantool client for LuaJIT.
--Written by Cosmin Apreutesei. Public Domain.

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

c.connect = protect(function(opt)
	local c = setmetatable(opt or {}, {__index = c})
	c:clear_metadata_cache()
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
	if c.user then
		local rbody = {[USER_NAME] = c.user, [TUPLE] = {}}
		local password = c.password or ''
		if password ~= '' then
			local s1 = sha1(password)
			local s2 = sha1(s1)
			local s3 = sha1(salt .. s2)
			local scramble = xor_strings(s1, s3)
			rbody[TUPLE] = {'chap-sha1', scramble}
		end
		request(c, AUTH, rbody, expires)
	end
	return c
end)

c.close = function(c)
	return c.tcp:close()
end

--[[local]] function request(c, req_type, body, expires)
	c.sync_num = ((c.sync_num or 0) + 1) % 100000
	local header = {[SYNC] = c.sync_num, [TYPE] = req_type}
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
	local code  = res_header[TYPE]
	local data  = res_body[DATA]
	local error = res_body[ERROR]
	check(c, code == OK, error)
	return data
end

local function resolve_space(c, space)
	return type(space) == 'number' and space or c._lookup_space(space)
end

local function resolve_index(c, space, index)
	return type(index) == 'number' and index
		or c._lookup_index(resolve_space(c, space), index)
end

c.clear_metadata_cache = function(c)
	c._lookup_space = memoize(function(space)
		local t = select(c, VIEW_SPACE, INDEX_SPACE_NAME, space)
		check(c, type(t) == 'table')
		return check(c, t[1] and t[1][1], "no space '%s'", space)
	end)
	c._lookup_index = memoize(function(spaceno, index)
		if not spaceno then return end
		local t = select(c, VIEW_INDEX, INDEX_INDEX_NAME, {spaceno, index})
		check(c, type(t) == 'table')
		return check(c, t[1] and t[1][2], "no index '%s'", index)
	end)
end

local function key_arg(key)
	return type(key) == 'table' and key or key == nil and {} or {key}
end

--[[local]] function select(c, space, index, key, opt)
	opt = opt or empty
	local spaceno = resolve_space(c, space)
	local indexno = resolve_index(c, spaceno, index or 'primary')
	local body = {
		[SPACE_ID] = spaceno,
		[INDEX_ID] = indexno,
		[KEY] = key_arg(key),
	}
	body[LIMIT] = opt.limit or 0xFFFFFFFF
	body[OFFSET] = opt.offset or 0
	body[ITERATOR] = opt.iterator
	return request(c, SELECT, body)
end
c.select = protect(select)

c.insert = protect(function(c, space, tuple)
	return request(c, INSERT, {[SPACE_ID] = resolve_space(c, space), [TUPLE] = tuple})
end)

c.replace = protect(function(c, space, tuple)
	return request(c, REPLACE, {[SPACE_ID] = resolve_space(c, space), [TUPLE] = tuple})
end)

c.update = protect(function(c, space, index, key, oplist)
	return request(c, UPDATE, {
		[SPACE_ID] = resolve_space(c, space),
		[INDEX_ID] = resolve_index(c, index),
		[KEY] = key_arg(key),
		[TUPLE] = oplist,
	})
end)

c.delete = protect(function(c, space, key)
	return request(c, DELETE, {[SPACE_ID] = resolve_space(c, space), [KEY] = key_arg(key)})
end)

c.upsert = protect(function(c, space, tuple, oplist)
	return request(c, UPSERT, {
		[SPACE_ID] = resolve_space(c, space),
		[TUPLE] = tuple,
		[OPS] = oplist,
	})
end)

c.ping = protect(function(c)
	return request(c, PING, {})
end)

c.call = protect(function(self, proc, args)
	return unpack(request(c, CALL, {[FUNCTION_NAME] = proc, [TUPLE] = args}))
end)

if not ... then
	local tarantool = c
	local sock = require'sock'
	sock.run(function()
		local c = tarantool.connect{
			--user     = 'root',
			--password = 'pass',
		}
		pp(c:select('_vspace'))
		--pp(c:call(
		pp('close', c:close())
	end)
end

return c
