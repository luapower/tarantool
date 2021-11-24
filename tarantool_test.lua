
local tarantool = require'tarantool'
local sock = require'sock'

sock.run(function()
	local c = assert(tarantool.connect{
		user     = 'admin',
		password = 'admin',
		tracebacks = true,
	})
	c = c:stream()
	assert(c:ping())
	local pass = 9
	if pass == 1 then
		pp(c:eval[[
			box.schema.space.create('test')
			box.space.test:create_index('primary', {parts = {1}})
			box.space.test:insert{'e', 4, 7}
			box.space.test:insert{'c', 5, 6}
			box.space.test:insert{'d', 6, 5}
		]])
	elseif pass == 2 then
		pp(c:exec[[
			CREATE TABLE table2 (
				column1 INTEGER,
				column2 VARCHAR(100),
				column3 SCALAR,
				column4 DOUBLE,
				PRIMARY KEY (column1, column2));
		]])
	elseif pass == 3 then
		pp(c:exec[[
			insert into table2 values (1, 'a', 5, 7);
			insert into table2 values (1, 'b', 3, 1);
			insert into table2 values (2, 'a', 4, 2);
			insert into table2 values (2, 'b', 3, 6);
		]])
	elseif pass == 4 then
		local st = c:prepare('select * from table2 where column1 = ? and column2 = :c2')
		pp(st:exec{1, c2 = 'b'})
	elseif pass == 5 then
		pp(c:insert('test', {'h', 2}))
	elseif pass == 6 then
		pp(c:replace('test', {'h', 3, 6}))
	elseif pass == 7 then
		pp(c:update('test', nil, 'h', {{'+', 1, 100}, {'-', 2, 100}}))
		pp(c:select('test', nil, ''))
	elseif pass == 8 then
		pp(c:delete('test', 'h'))
	elseif pass == 9 then
		pp(c:eval([[
			return 'hello', ...
		]], 5, nil, 7))
	end
	assert(not c.tcp:closed())
	pp('close', c:close())
end)
