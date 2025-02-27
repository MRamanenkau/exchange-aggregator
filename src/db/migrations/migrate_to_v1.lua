local fiber = require('fiber')
local TWO_GB = 2 * 1024 * 1024 * 1024

box.cfg{
    listen = 3301,
    wal_mode = "none",
    memtx_memory = TWO_GB,
    memtx_dir = "../../../exchange_crawler_db/snapshots",
    checkpoint_interval = 300,
    checkpoint_count = 2
}

-- Persist in-memory data on disk based on RAM consumption
fiber.create(function()
    while true do
        local memory_info = box.info.memory()
        local total_usage = (memory_info.data or 0) + (memory_info.index or 0)
        if total_usage >= TWO_GB then
            box.snapshot()
            print("Snapshot created")
        end
        fiber.sleep(1) -- Check every second
    end
end)

if not box.space.recent_trades_btc_usdt then
    box.schema.space.create('recent_trades_btc_usdt', {
        format = {
            {name = 'id', type = 'unsigned'},
            {name = 'price', type = 'decimal'},
            {name = 'amount', type = 'decimal'},
            {name = 'side', type = 'string'},
            {name = 'timestamp', type = 'unsigned'}
        }
    })
end

if not box.space.recent_trades_btc_usdt.index.primary then
    box.space.recent_trades_btc_usdt:create_index('primary', {
        type = 'tree',
        parts = {'id'}
    })
end

if not box.space.klines_btc_usdt then
    box.schema.space.create('klines_btc_usdt', {
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
end

if not box.sequence.klines_id_seq then
    box.schema.sequence.create('klines_id_seq', {min = 1})
end

if not box.space.klines_btc_usdt.index.primary then
    box.space.klines_btc_usdt:create_index('primary', {
        type = 'tree',
        parts = {'id'},
        sequence = 'klines_id_seq'
    })
end

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

--if not box.sequence.klines_id_seq then
--    box.schema.sequence.create('klines_id_seq', {min = 1})
--end
--
--for _, pair in ipairs({'btc_usdt', 'trx_usdt', 'eth_usdt', 'doge_usdt', 'bch_usdt'}) do
--    local space_name = 'klines_' .. pair
--    if not box.space[space_name] then
--        box.schema.space.create(space_name, {
--            format = {
--                {name = 'id', type = 'unsigned'},
--                {name = 'time_frame', type = 'integer'},
--                {name = 'o', type = 'decimal'},
--                {name = 'h', type = 'decimal'},
--                {name = 'l', type = 'decimal'},
--                {name = 'c', type = 'decimal'},
--                {name = 'utc_begin', type = 'unsigned'},
--                {name = 'buy_base', type = 'decimal'},
--                {name = 'sell_base', type = 'decimal'},
--                {name = 'buy_quote', type = 'decimal'},
--                {name = 'sell_quote', type = 'decimal'}
--            }
--        })
--        box.space[space_name]:create_index('primary', {
--            parts = {{field = 'id'}},
--            sequence = 'klines_id_seq'
--        })
--    end
--end