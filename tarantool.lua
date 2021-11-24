
--Tarantool client for LuaJIT.
--Written by Cosmin Apreutesei. Public Domain.

local ffi     = require'ffi'
local bit     = require'bit'
local mp      = require'messagepack'
local b64     = require'base64'
local sha1    = require'sha1'.sha1
local errors  = require'errors'
local glue    = require'glue'

local u8a     = glue.u8a
local u8p     = glue.u8p
local buffer  = glue.buffer
local empty   = glue.empty
local memoize = glue.memoize

local check_io, check, protect = errors.tcp_protocol_errors'tarantool'

local c = {host = '127.0.0.1', port = 3301, timeout = 2, tracebacks = false}

--IPROTO_*
local OK         = 0
local SELECT     = 1
local INSERT     = 2
local REPLACE    = 3
local UPDATE     = 4
local DELETE     = 5
local AUTH       = 7
local EVAL       = 8
local UPSERT     = 9
local CALL       = 10
local EXECUTE    = 11
local NOP        = 12
local PREPARE    = 13
local PING       = 0x40

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
local EXPR          = 0x27
local OPS           = 0x28
local OPTIONS       = 0x2b
local DATA          = 0x30
local ERROR         = 0x31
local METADATA      = 0x32
local BIND_METADATA = 0x33
local BIND_COUNT    = 0x34
local SQL_TEXT      = 0x40
local SQL_BIND      = 0x41
local STMT_ID       = 0x43
local FIELD_NAME    = 0x00
local FIELD_TYPE    = 0x01
local FIELD_COLL    = 0x02
local FIELD_IS_NULLABLE = 0x03
local FIELD_IS_AUTOINCREMENT = 0x04
local FIELD_SPAN = 0x05

-- default spaces
local SPACE_SCHEMA  = 272
local SPACE_SPACE   = 280
local SPACE_INDEX   = 288
local SPACE_FUNC    = 296
local SPACE_USER    = 304
local SPACE_PRIV    = 312
local SPACE_CLUSTER = 320

-- default views
local VIEW_SPACE = 281
local VIEW_INDEX = 289

-- index info
local INDEX_SPACE_NAME = 2
local INDEX_INDEX_NAME = 2

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

local request, tselect --fw. decl.

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
	check_io(c, c.tcp:recvn(b, 64, expires)) --greeting
	local salt = ffi.string(check_io(c, c.tcp:recvn(b, 64, expires)), 44)
	if c.user then
		local body = {[USER_NAME] = c.user, [TUPLE] = empty}
		if c.password and c.password ~= '' then
			local salt = b64.decode(salt):sub(1, 20)
			local s1 = sha1(c.password)
			local s2 = sha1(s1)
			local s3 = sha1(salt .. s2)
			local scramble = xor_strings(s1, s3)
			body[TUPLE] = {'chap-sha1', scramble}
		end
		request(c, AUTH, body, expires)
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
	index = index or 0
	local space = resolve_space(c, space)
	return space, type(index) == 'number' and index or c._lookup_index(space, index)
end

c.clear_metadata_cache = function(c)
	c._lookup_space = memoize(function(space)
		local t = tselect(c, VIEW_SPACE, INDEX_SPACE_NAME, space)
		check(c, type(t) == 'table')
		return check(c, t[1] and t[1][1], "no space '%s'", space)
	end)
	c._lookup_index = memoize(function(spaceno, index)
		if not spaceno then return end
		local t = tselect(c, VIEW_INDEX, INDEX_INDEX_NAME, {spaceno, index})
		check(c, type(t) == 'table')
		return check(c, t[1] and t[1][2], "no index '%s'", index)
	end)
end

local function key_arg(key)
	return type(key) == 'table' and key or key == nil and empty or {key}
end

--[[local]] function tselect(c, space, index, key, opt)
	opt = opt or empty
	local space, index = resolve_index(c, space, index)
	local body = {
		[SPACE_ID] = space,
		[INDEX_ID] = index,
		[KEY] = key_arg(key),
	}
	body[LIMIT] = opt.limit or 0xFFFFFFFF
	body[OFFSET] = opt.offset or 0
	body[ITERATOR] = opt.iterator
	return request(c, SELECT, body)
end
c.select = protect(tselect)

c.insert = protect(function(c, space, tuple)
	return request(c, INSERT, {[SPACE_ID] = resolve_space(c, space), [TUPLE] = tuple})
end)

c.replace = protect(function(c, space, tuple)
	return request(c, REPLACE, {[SPACE_ID] = resolve_space(c, space), [TUPLE] = tuple})
end)

c.update = protect(function(c, space, index, key, oplist)
	local space, index = resolve_index(c, space, index)
	return request(c, UPDATE, {
		[SPACE_ID] = space,
		[INDEX_ID] = index,
		[KEY] = key_arg(key),
		[TUPLE] = oplist,
	})
end)

c.delete = protect(function(c, space, key)
	local space, index = resolve_index(c, space, index)
	return request(c, DELETE, {
		[SPACE_ID] = space,
		[INDEX_ID] = index,
		[KEY] = key_arg(key),
	})
end)

c.upsert = protect(function(c, space, index, key, oplist)
	return request(c, UPSERT, {
		[SPACE_ID] = resolve_space(c, space),
		[INDEX_ID] = index,
		[OPS] = oplist,
		[TUPLE] = key_arg(key),
	})
end)

c.eval = protect(function(c, expr, ...)
	return unpack(request(c, EVAL, {[EXPR] = expr, [TUPLE] = {...}}))
end)

c.call = protect(function(c, fn, ...)
	return unpack(request(c, CALL, {[FUNCTION_NAME] = fn, [TUPLE] = {...}}))
end)

c.query = protect(function(c, sql, ...)
	local i, opt = 1
	if type(sql) == 'table' then --opt, sql, ...
		i, opt, sql = 2, sql, ...
	end
	return request(c, EXECUTE, {
		[STMT_ID] = type(sql) == 'number' and sql or nil,
		[SQL_TEXT] = type(sql) == 'string' and sql or nil,
		[SQL_BIND] = {select(i, ...)},
		[OPTIONS] = opt or empty,
	})
end)

local function fields_metadata(c, t, n)
	check(c, #t == n)
	local dt = {}
	for i, t in ipairs(t) do
		local t = t[METADATA]
		dt[i] = {
			name = t[FIELD_NAME],
			type = t[FIELD_TYPE],
			collation = t[FIELD_COLL],
			not_null = not t[FIELD_IS_NULLABLE],
			autoinc = t[FIELD_IS_AUTOINCREMENT],
			span = t[FIELD_SPAN],
		}
	end
	return dt
end

c.prepare = protect(function(c, sql)
	local ret = request(c, PREPARE, {
		[SQL_TEXT] = type(sql) == 'string' and sql or nil,
	})
	return fields_metadata(c, ret[BIND_METADATA], ret[BIND_COUNT])
end)

c.ping = protect(function(c)
	return request(c, PING, empty)
end)

if not ... then
	local tarantool = c
	local sock = require'sock'
	sock.run(function()
		local c = assert(tarantool.connect{
			user     = 'admin',
			password = 'admin',
		})
		--pp(c:eval"box.schema.space.create('test')")
		--pp(c:eval"box.space.test:create_index('primary', {parts = {1}})")
		--pp(c:eval"box.space.test:insert{'c', 4, 7}")
		pp(c:select('test'))
		pp(c:prepare('select * from test'))
		pp('close', c:close())
	end)
end

return c
