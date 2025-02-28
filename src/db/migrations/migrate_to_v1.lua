box.cfg{
    listen = 3301,
    wal_mode = "none",
    memtx_dir = "../../db/snapshots",
    checkpoint_interval = 5,
    checkpoint_count = 2
}

function get_spaces()
    local spaces = {}
    for _, space in pairs(box.space._space:select()) do
        local id = space[1]  -- Space ID
        local name = space[3]  -- Space name
        if not name:match('^_') then
            table.insert(spaces, {id, name})
        end
    end
    return spaces
end

if not box.sequence.klines_id_seq then
    box.schema.sequence.create('klines_id_seq', {min = 1})
end

for _, pair in ipairs({'btc_usdt', 'trx_usdt', 'eth_usdt', 'doge_usdt', 'bch_usdt'}) do
    local space_name = 'klines_' .. pair
    if not box.space[space_name] then
        box.schema.space.create(space_name, {
            format = {
                {name = 'id', type = 'unsigned'},
                {name = 'time_frame', type = 'integer'},
                {name = 'o', type = 'decimal'},
                {name = 'h', type = 'decimal'},
                {name = 'l', type = 'decimal'},
                {name = 'c', type = 'decimal'},
                {name = 'utc_begin', type = 'unsigned'},
                {name = 'buy_base', type = 'decimal'},
                {name = 'sell_base', type = 'decimal'},
                {name = 'buy_quote', type = 'decimal'},
                {name = 'sell_quote', type = 'decimal'}
            }
        })
        box.space[space_name]:create_index('primary', {
            parts = {{field = 'id'}},
            sequence = 'klines_id_seq'
        })
    end
end