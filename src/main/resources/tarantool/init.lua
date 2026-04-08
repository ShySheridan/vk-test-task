box.cfg{
    listen = 3301
}

box.once('bootstrap_kv', function()
    local user = os.getenv('TARANTOOL_USER') or 'app'
    local password = os.getenv('TARANTOOL_PASSWORD') or 'app_password'

    local kv = box.schema.space.create('KV', {
        if_not_exists = true,
        engine = 'memtx'
    })

    kv:format({
        {name = 'key', type = 'string'},
        {name = 'value', type = 'varbinary', is_nullable = true}
    })

    kv:create_index('primary', {
        if_not_exists = true,
        type = 'TREE',
        parts = {
            {field = 'key', type = 'string'}
        }
    })

    box.schema.user.create(user, {
        password = password,
        if_not_exists = true
    })

    box.schema.user.grant(user, 'read,write,execute', 'universe', nil, {
        if_not_exists = true
    })
end)

print('Tarantool KV is ready')