select 
    b.id,
    b.purchase_time,
    b.underlying_symbol,
    b.payout_price,
    b.buy_price,
    b.sell_price,
    b.start_time,
    b.expiry_time,
    EXTRACT(EPOCH FROM (b.start_time - b.purchase_time)) as delay,
    EXTRACT(EPOCH FROM (b.expiry_time - b.start_time)) as duration,
    b.bet_type,
    b.remark,
    b.short_code,
    b.sell_time,
    a.client_loginid,
    t.staff_loginid,
    bm.market,
    bm.submarket,
    a.currency_code,
    c.residence,
    c.date_joined,
    b.tick_count,
    c.myaffiliates_token,
    t.source
from only 
    bet.financial_market_bet as b
join 
    transaction.transaction t on b.id = t.financial_market_bet_id and t.action_type = 'buy'
join 
    transaction.account a on a.id = b.account_id
join 
    betonmarkets.client c on c.loginid = a.client_loginid
left join 
    bet.market bm on b.underlying_symbol = bm.symbol
