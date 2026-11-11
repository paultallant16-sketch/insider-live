#!/usr/bin/env python3
"""Insider Live scanner - one pass over all watched team wallets on five chains.

Runs on a timer (GitHub Actions). Standard library only.
  * EVM chains (BSC, Base, Ethereum, Robinhood): ERC-20 Transfer logs to/from watched wallets via public RPC.
  * Solana: new signatures per watched wallet, token balance changes per transaction via public RPC.
  * GeckoTerminal (free, no key): token names, prices, market caps, new/trending pools for discovery.
Writes the dashboard data to docs/data/*.json and its own cursor to data/state.json.
Everything here is read-only public chain data. Nothing trades.
"""
import json, os, re, sys, time, html, zlib, urllib.request, urllib.error, datetime as dt

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, 'data')
OUT = os.path.join(ROOT, 'docs', 'data')
UA = 'insider-live-scanner/1.0 (+github actions)'
T0 = time.time()
RUN_BUDGET = float(os.environ.get('RUN_BUDGET', '540'))   # seconds for the whole pass
NOW = int(time.time())
KEEP_DAYS = 7
ERRORS = []

TRANSFER = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
EVM = {
    'BSC':       dict(gt='bsc', block_s=0.75, max_range=2000, back=2400,
                      rpcs=['https://bsc-rpc.publicnode.com', 'https://bsc.drpc.org', 'https://binance.llamarpc.com']),
    'Base':      dict(gt='base', block_s=2.0, max_range=2000, back=900,
                      rpcs=['https://base-rpc.publicnode.com', 'https://mainnet.base.org', 'https://base.drpc.org']),
    'Ethereum':  dict(gt='eth', block_s=12.0, max_range=500, back=150,
                      rpcs=['https://ethereum-rpc.publicnode.com', 'https://eth.drpc.org', 'https://eth.llamarpc.com']),
    'Robinhood': dict(gt='robinhood', block_s=0.106, max_range=10000, back=12000,
                      rpcs=['https://rpc.mainnet.chain.robinhood.com', 'https://rpc.nodeflare.app/robinhood/public']),
}
SOL_RPCS = ['https://api.mainnet-beta.solana.com', 'https://solana-rpc.publicnode.com']
SOL_RPCS_GAP = [('https://api.mainnet-beta.solana.com', 0.3), ('https://solana-rpc.publicnode.com', 0.12)]
GT_NET = {'Solana': 'solana', 'BSC': 'bsc', 'Base': 'base', 'Ethereum': 'eth', 'Robinhood': 'robinhood'}
# quote / base assets: a transfer of these is money moving, not a token pick
BASE_SYMBOLS = {'WETH', 'ETH', 'WBNB', 'BNB', 'USDC', 'USDT', 'DAI', 'BUSD', 'FDUSD', 'USD1', 'USDE', 'TUSD',
                'WSOL', 'SOL', 'WBTC', 'CBBTC', 'BTCB', 'USDC.E', 'USDBC', 'PYUSD', 'STETH', 'WSTETH', 'CBETH'}
WSOL = 'So11111111111111111111111111111111111111112'
STABLES = {'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'Es9vMFrzaCERmJfrF8KAQoQGyFw53xcjjZzdKgcqfTsQ'}
SOL_BASE_MINTS = {WSOL} | STABLES
SCAN_RESERVE = 200      # seconds kept back after the chain scans for prices, alerts, signals and discovery


def left():
    return RUN_BUDGET - (time.time() - T0)


def utc(ts):
    return dt.datetime.fromtimestamp(int(ts), dt.timezone.utc).strftime('%Y-%m-%d %H:%M')


def load(path, default):
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def save(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, separators=(',', ':'))
    os.replace(tmp, path)


def http(url, body=None, timeout=20):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={
        'User-Agent': UA, 'Accept': 'application/json',
        **({'Content-Type': 'application/json'} if data else {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8'))


class RPC:
    """JSON-RPC over a list of public endpoints; rotates on failure."""
    def __init__(self, name, urls, gap=0.15):
        self.name, self.urls, self.i, self.gap, self.last = name, urls, 0, gap, 0.0

    def call(self, method, params, tries=4):
        err = None
        for k in range(tries):
            wait = self.gap - (time.time() - self.last)
            if wait > 0:
                time.sleep(wait)
            url = self.urls[self.i % len(self.urls)]
            self.last = time.time()
            try:
                r = http(url, {'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params})
                if 'error' in r:
                    err = RuntimeError(str(r['error'])[:200])
                    msg = str(r['error']).lower()
                    if 'rate' in msg or 'traffic' in msg or 'capacity' in msg:
                        time.sleep(1.5 * (k + 1))
                        self.i += 1
                        continue
                    if any(s in msg for s in ('range', 'limit', 'too many', 'exceed', 'large')):
                        raise err          # caller shrinks the range
                    self.i += 1
                    continue
                return r.get('result')
            except urllib.error.HTTPError as e:
                err = e
                if e.code == 429:
                    time.sleep(1.5 * (k + 1))
                self.i += 1
            except RuntimeError:
                raise
            except Exception as e:
                err = e
                self.i += 1
        raise RuntimeError(f'{self.name} {method}: {err}')


# ---------------------------------------------------------------- GeckoTerminal
class GT:
    base = 'https://api.geckoterminal.com/api/v2'
    gap = 2.2       # free tier: 30 calls / minute
    last = 0.0
    calls = 0

    @classmethod
    def get(cls, path):
        for k in range(3):
            wait = cls.gap - (time.time() - cls.last)
            if wait > 0:
                time.sleep(wait)
            cls.last = time.time()
            cls.calls += 1
            try:
                return http(cls.base + path)
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    time.sleep(max(0, min(10 * (k + 1), left() - 30)))
                    continue
                if e.code == 404:
                    return None
                ERRORS.append(f'geckoterminal {e.code} {path[:60]}')
                return None
            except Exception as e:
                ERRORS.append(f'geckoterminal {type(e).__name__} {path[:60]}')
                time.sleep(2)
        return None


def token_info(chain, addrs, cache):
    """Fill cache[(chain, addr)] with symbol/decimals/price/mc. Refreshes prices for the given tokens."""
    net = GT_NET[chain]
    addrs = [a for a in dict.fromkeys(addrs) if a and
             (cache.get(f'{chain}|{a if chain == "Solana" else a.lower()}') or {}).get('at') != NOW]   # once per pass
    for i in range(0, len(addrs), 30):
        if left() < 25:
            break
        chunk = addrs[i:i + 30]
        r = GT.get(f'/networks/{net}/tokens/multi/{",".join(chunk)}?include=top_pools')
        pools = {p.get('id'): p.get('attributes') or {} for p in (r or {}).get('included', [])}
        for t in (r or {}).get('data', []):
            a = t.get('attributes', {})
            addr = a.get('address') or t.get('id', '').split('_', 1)[-1]
            key = f'{chain}|{addr if chain == "Solana" else addr.lower()}'
            mc = _f(a.get('market_cap_usd') or a.get('fdv_usd'))
            old = cache.get(key) or {}
            row = dict(sym=(a.get('symbol') or '?')[:24], name=(a.get('name') or '')[:40],
                       dec=a.get('decimals'), price=_f(a.get('price_usd')), mc=mc,
                       liq=_f(a.get('total_reserve_in_usd')), at=NOW,
                       peak=max(old.get('peak') or 0, mc or 0) or None, created=old.get('created'))
            tp = ((t.get('relationships') or {}).get('top_pools') or {}).get('data') or []
            pa = pools.get(tp[0].get('id')) if tp else None
            if pa:
                pc = pa.get('price_change_percentage') or {}
                row['pool'] = pa.get('address') or tp[0].get('id', '').split('_', 1)[-1]
                made = []
                for x in tp:                      # token age = its oldest top pool, not a fresh side pool
                    try:
                        made.append(int(dt.datetime.fromisoformat(
                            pools[x.get('id')]['pool_created_at'].replace('Z', '+00:00')).timestamp()))
                    except Exception:
                        pass
                if made:
                    row['created'] = min(made + ([old['created']] if old.get('created') else []))
                row.update(ch1=_f(pc.get('h1')), ch6=_f(pc.get('h6')), ch24=_f(pc.get('h24')),
                           vol24=_f((pa.get('volume_usd') or {}).get('h24')))
            cache[key] = row
            row['sym'] = html.unescape(row['sym'])
            row['name'] = html.unescape(row['name'])


def token_state(info):
    """fresh / running / fading / older, so the page can push already-dumped tokens out of the way."""
    mc, peak, created = info.get('mc'), info.get('peak'), info.get('created')
    ch1, ch6, ch24 = info.get('ch1'), info.get('ch6'), info.get('ch24')
    age_h = (NOW - created) / 3600 if created else None
    if (ch6 is not None and ch6 <= -35) or (ch24 is not None and ch24 <= -60) or (peak and mc and mc < 0.4 * peak):
        return 'fading', age_h
    if age_h is not None and age_h <= 24:
        return 'fresh', age_h
    if (ch1 or 0) >= 10 or (ch6 or 0) >= 25:
        return 'running', age_h
    return 'older', age_h


def token_extra(info):
    state, age_h = token_state(info)
    return dict(state=state, age_h=round(age_h, 1) if age_h is not None else None, created=info.get('created'),
                ch1=info.get('ch1'), ch6=info.get('ch6'), ch24=info.get('ch24'), vol24=info.get('vol24'),
                peak=info.get('peak'))


# ---------------------------------------------------------------- is this a real, tradable coin?
SAFE_TTL = 6 * 3600
RISK_TOK = {}           # 'chain|ca' -> reasons found this pass (KOL dump ring active, insiders dumping sent tokens)
SAFE_CALLS = [0]


def safety(chain, ca, st, budget=14):
    """Holder spread and sell checks from GeckoTerminal's token info, cached for 6 hours per token."""
    sc = st.setdefault('safe', {})
    key = f'{chain}|{ca}'
    row = sc.get(key)
    if row and NOW - row.get('at', 0) < SAFE_TTL:
        return row
    if SAFE_CALLS[0] >= budget or left() < 30:
        return row
    SAFE_CALLS[0] += 1
    r = GT.get(f'/networks/{GT_NET[chain]}/tokens/{ca}/info')
    a = ((r or {}).get('data') or {}).get('attributes') or {}
    if not a:
        return row
    h = a.get('holders') or {}
    dist = h.get('distribution_percentage') or {}
    row = dict(at=NOW, holders=h.get('count'), top10=_f(dist.get('top_10')), honeypot=a.get('is_honeypot'),
               mint=bool(a.get('mint_authority')) and a.get('mint_authority') not in ('no', 'renounced'),
               freeze=bool(a.get('freeze_authority')) and a.get('freeze_authority') not in ('no', 'renounced'),
               gt_score=_f(a.get('gt_score')))
    sc[key] = row
    return row


def tradable(info, sf, strict=True, key=None):
    """(ok, reasons). Fails closed only on hard evidence; unknown holder data is not a pass for buy signals."""
    why = []
    liq, mc = info.get('liq'), info.get('mc')
    if liq is None:
        if strict:
            why.append('liquidity unknown')
    elif liq < 10_000:
        why.append('liquidity under $10K')
    elif mc and mc > 100 * liq:
        why.append('market cap far above liquidity')
    if key and key in RISK_TOK:
        why.extend(RISK_TOK[key])
    if sf:
        if sf.get('honeypot') is True:
            why.append("honeypot: can't sell")
        if sf.get('holders') is not None and sf['holders'] < 100:
            why.append(f"only {sf['holders']} holders")
        if sf.get('top10') is not None and sf['top10'] > 75:
            why.append(f"top 10 wallets hold {sf['top10']:.0f}%")
        if sf.get('freeze'):
            why.append('creator can freeze wallets')
        if sf.get('mint'):
            why.append('creator can mint more')
    return (not why), why


def _f(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


# ---------------------------------------------------------------- EVM
def topic(addr):
    return '0x' + '0' * 24 + addr.lower()[2:]


def scan_evm(chain, cfg, wallets, st, newmoves, contracts=None):
    if not wallets:
        return
    if contracts is not None and not contracts:
        ERRORS.append(f'{chain}: no candidate contracts this pass')
        return                  # never move the cursor past blocks we did not look at
    rpc = RPC(chain, cfg['rpcs'], gap=0.12)
    cur = st.setdefault('evm', {}).setdefault(chain, {})
    try:
        head = int(rpc.call('eth_blockNumber', []), 16)
    except Exception as e:
        ERRORS.append(f'{chain}: no RPC ({str(e)[:80]})')
        return
    start = cur.get('last', head - cfg['back']) + 1
    # never fall more than ~3 h behind; skip ahead instead of crawling forever
    max_lag = int(10800 / cfg['block_s'])
    if head - start > max_lag:
        ERRORS.append(f'{chain}: skipped {head - start - max_lag} blocks after an outage')
        start = head - max_lag
    if start > head:
        return
    wl = sorted(wallets)
    wset = set(wl)
    tops = [topic(w) for w in wl]
    groups_of = 60     # addresses per OR-filter
    rng = cur.get('range', cfg['max_range'])
    logs = []
    b = start
    t_end = time.time() + min(100, max(20, (left() - SCAN_RESERVE) / 3))   # per-chain cap
    while b <= head and left() > SCAN_RESERVE and time.time() < t_end:
        e = min(head, b + rng - 1)
        try:
            got = []
            for i in range(0, len(tops), groups_of):
                part = tops[i:i + groups_of]
                for flt in ([TRANSFER, None, part], [TRANSFER, part]):
                    q = {'fromBlock': hex(b), 'toBlock': hex(e), 'topics': flt}
                    if contracts is None:
                        got += rpc.call('eth_getLogs', [q]) or []
                    else:
                        for j in range(0, len(contracts), 10):
                            got += rpc.call('eth_getLogs', [dict(q, address=contracts[j:j + 10])]) or []
            logs += got
            cur['last'] = e
            b = e + 1
            if rng < cfg['max_range']:
                rng = min(cfg['max_range'], rng * 2)
        except Exception as ex:
            msg = str(ex).lower()
            if rng > 20 and any(x in msg for x in ('range', 'too many', 'exceed', 'large', 'limit', 'results')):
                rng = max(20, rng // 4)
                continue
            ERRORS.append(f'{chain} getLogs: {str(ex)[:100]}')
            break
    cur['range'] = rng
    cur['head'] = head
    cur['at'] = NOW
    if not logs:
        return
    # timestamps: interpolate between the first and last block we fetched
    blocks = sorted({int(l['blockNumber'], 16) for l in logs})
    try:
        t_lo = int(rpc.call('eth_getBlockByNumber', [hex(blocks[0]), False])['timestamp'], 16)
        t_hi = int(rpc.call('eth_getBlockByNumber', [hex(blocks[-1]), False])['timestamp'], 16)
    except Exception:
        t_hi = NOW
        t_lo = NOW - int((blocks[-1] - blocks[0]) * cfg['block_s'])
    span = max(1, blocks[-1] - blocks[0])
    seen = set()
    agg = {}
    for l in logs:
        if len(l.get('topics', [])) != 3:
            continue            # ERC-721 (4 topics) or malformed
        key = (l['transactionHash'], l.get('logIndex'))
        if key in seen:
            continue
        seen.add(key)
        frm = '0x' + l['topics'][1][-40:]
        to = '0x' + l['topics'][2][-40:]
        bn = int(l['blockNumber'], 16)
        ts = int(t_lo + (bn - blocks[0]) * (t_hi - t_lo) / span)
        try:
            raw = int(l.get('data') or '0x0', 16)
        except Exception:
            raw = 0
        tok = l['address'].lower()
        if frm in wset and to in wset:
            side, w = 'internal', to
        elif to in wset:
            side, w = 'buy', to
        elif frm in wset:
            side, w = 'sell', frm
        else:
            continue
        k = (l['transactionHash'], w, tok, side)       # split routes / tax tokens: add the legs together
        if k in agg:
            agg[k]['raw'] = str(int(agg[k]['raw']) + raw)
        else:
            agg[k] = dict(chain=chain, wallet=w, ts=ts, side=side, ca=tok, raw=str(raw),
                          tx=l['transactionHash'], cp=frm if side == 'buy' else to)
    newmoves.extend(agg.values())


# ---------------------------------------------------------------- Blockscout (per wallet, no key)
BLOCKSCOUT = {'Ethereum': 'https://eth.blockscout.com'}


def scan_blockscout(chain, wallets, st, newmoves):
    host = BLOCKSCOUT[chain]
    cur = st.setdefault('bs', {}).setdefault(chain, {})
    last = cur.setdefault('last', {})
    wset = set(wallets)
    done = fails = 0
    t_end = time.time() + min(100, max(20, (left() - SCAN_RESERVE) / 2))
    for w in sorted(wallets):
        if left() < SCAN_RESERVE or time.time() > t_end or fails >= 5:
            break
        try:
            try:
                r = http(f'{host}/api/v2/addresses/{w}/token-transfers?type=ERC-20', timeout=20)
            except urllib.error.HTTPError as e1:
                if e1.code != 429:
                    raise
                time.sleep(5)
                r = http(f'{host}/api/v2/addresses/{w}/token-transfers?type=ERC-20', timeout=20)
        except Exception as e:
            fails += 1
            if fails == 1:
                ERRORS.append(f'{chain} blockscout: {getattr(e, "code", type(e).__name__)}')
            time.sleep(1)
            continue
        done += 1
        since = last.get(w, NOW - 3 * 3600)
        newest = since
        agg = {}
        for it in (r or {}).get('items', []):
            try:
                ts = int(dt.datetime.fromisoformat(it['timestamp'].replace('Z', '+00:00')).timestamp())
            except Exception:
                continue
            if ts <= since:
                continue
            newest = max(newest, ts)
            frm = ((it.get('from') or {}).get('hash') or '').lower()
            to = ((it.get('to') or {}).get('hash') or '').lower()
            t = it.get('token') or {}
            tok = (t.get('address') or t.get('address_hash') or '').lower()
            raw = (it.get('total') or {}).get('value') or '0'
            if frm in wset and to in wset:
                side, ww = 'internal', to
            elif to == w:
                side, ww = 'buy', w
            elif frm == w:
                side, ww = 'sell', w
            else:
                continue
            tx = it.get('transaction_hash') or it.get('tx_hash') or ''
            k = (tx, ww, tok, side)
            try:
                rv = int(raw)
            except Exception:
                rv = 0
            if k in agg:
                agg[k]['raw'] = str(int(agg[k]['raw']) + rv)
            else:
                agg[k] = dict(chain=chain, wallet=ww, ts=ts, side=side, ca=tok, raw=str(rv), tx=tx)
        newmoves.extend(agg.values())
        last[w] = newest
        time.sleep(1.0)
    cur['covered'] = done
    cur['of'] = len(wallets)
    cur['at'] = NOW


def bsc_contracts(st, moves, chain='BSC'):
    """BSC public nodes only answer log queries scoped to token contracts, so watch trending and new
    BSC tokens plus every token our teams touched in the last 7 days."""
    cas = []
    n = GT_NET[chain]
    for path in (f'/networks/{n}/trending_pools?duration=5m&page=1', f'/networks/{n}/trending_pools?duration=1h&page=1',
                 f'/networks/{n}/trending_pools?duration=6h&page=1', f'/networks/{n}/new_pools?page=1',
                 f'/networks/{n}/new_pools?page=2', f'/networks/{n}/new_pools?page=3'):
        if left() < 200:
            break
        r = GT.get(path)
        for p in (r or {}).get('data', []):
            base = (p.get('relationships', {}).get('base_token', {}).get('data', {}) or {}).get('id', '')
            if base:
                cas.append(base.split('_', 1)[-1].lower())
    key = f'{chain.lower()}_contracts'
    fresh = list(dict.fromkeys(c for c in cas if c.startswith('0x')))[:60]
    moved = list(dict.fromkeys(m['ca'] for m in sorted(moves, key=lambda m: -m['ts']) if m['chain'] == chain))
    cas = list(dict.fromkeys(fresh + moved[:40] + st.get(key, [])))[:120]
    st[key] = cas
    return cas


# ---------------------------------------------------------------- Solana
def _sol_parse(w, s, tx, wset, newmoves):
    """One Solana transaction -> buy / sell / internal moves for wallet w."""
    m = tx['meta']
    ts = tx.get('blockTime') or s.get('blockTime') or NOW
    own = {}                             # (owner, mint) -> balance change, for every watched owner
    for sgn, arr in ((-1, m.get('preTokenBalances') or []), (1, m.get('postTokenBalances') or [])):
        for b in arr:
            o = b.get('owner')
            if o in wset:
                amt = _f((b.get('uiTokenAmount') or {}).get('uiAmountString')) or 0.0
                own[(o, b['mint'])] = own.get((o, b['mint']), 0.0) + sgn * amt
    d = {mint: v for (o, mint), v in own.items() if o == w}
    sol_delta = 0.0
    try:
        keys = [k['pubkey'] if isinstance(k, dict) else k for k in tx['transaction']['message']['accountKeys']]
        i = keys.index(w)
        sol_delta = (m['postBalances'][i] - m['preBalances'][i]) / 1e9
    except Exception:
        pass
    picks = [k for k, v in d.items() if k not in SOL_BASE_MINTS and abs(v) > 1e-12]
    one = len(picks) == 1                # only value a plain one-token swap
    base_sol = sol_delta + d.get(WSOL, 0.0)
    base_usd = sum(d.get(k, 0.0) for k in STABLES)
    for mint in picks:
        delta = d[mint]
        opp = any(o != w and mm == mint and v * delta < 0 for (o, mm), v in own.items())
        side = 'internal' if opp else ('buy' if delta > 0 else 'sell')
        newmoves.append(dict(chain='Solana', wallet=w, ts=ts, side=side, ca=mint, amount=abs(delta),
                             sol=base_sol if one else 0.0, stable=base_usd if one else 0.0,
                             tx=s['signature']))


def _sol_pool(jobs, fn, deadline):
    """Run fn(rpc, job) over jobs with one worker per public endpoint (each has its own rate limit)."""
    import threading
    q = list(jobs)
    lock = threading.Lock()
    fails = [0]

    def worker(url, gap):
        rpc = RPC('Solana', [url], gap=gap)
        while True:
            if left() < 75 or time.time() > deadline or fails[0] > 80:
                return
            with lock:
                if not q:
                    return
                job = q.pop(0)
            try:
                fn(rpc, job)
            except Exception as e:
                with lock:
                    fails[0] += 1
                    if fails[0] in (1, 10):
                        ERRORS.append(f'Solana {url.split("/")[2][:20]}: {str(e)[:70]}')
                time.sleep(1.0)
    ts = [threading.Thread(target=worker, args=(u, g), daemon=True) for u, g in SOL_RPCS_GAP]
    for t in ts:
        t.start()
    for t in ts:
        t.join(max(1, deadline - time.time() + 20))
    return q                                           # jobs not reached this pass


def scan_solana(wallets, st, newmoves, priority=(), deadline=None):
    """Freshness first. Step 1: list the latest signatures of every watched wallet (proven ones first), spread
    over several public endpoints at once. Step 2: open the new transactions newest-first until time runs out.
    A busy bot wallet only gets its newest few looked at: an hours-late trade is no use to anyone following."""
    if not wallets:
        return
    deadline = deadline or (time.time() + left())
    cur = st.setdefault('sol', {})
    cur.pop('last', None)                              # old single-bookmark format
    seen = cur.setdefault('seen', {})                  # wallet -> recent signatures already handled
    wset = set(wallets)
    pri = [w for w in sorted(set(priority)) if w in wset]
    rest = sorted(wset - set(pri))
    pos = cur.get('pos', 0) % max(1, len(rest))
    order = pri + rest[pos:] + rest[:pos]
    lists = {}

    def get_sigs(rpc, w):
        lists[w] = rpc.call('getSignaturesForAddress', [w, {'limit': 25}], tries=2) or []
    left_over = _sol_pool(order, get_sigs, time.time() + 0.45 * max(30, deadline - time.time()))
    listed = [w for w in order if w in lists]
    jobs, busy = [], 0
    for w in listed:
        sigs = lists[w]
        have = set(seen.get(w) or [])
        first = not have
        fresh = [s for s in sigs if s['signature'] not in have and not s.get('err')
                 and (s.get('blockTime') or NOW) >= NOW - 6 * 3600]
        if first:
            fresh = [s for s in fresh if (s.get('blockTime') or 0) >= NOW - 1800]
        if len(fresh) >= 25:
            busy += 1
            fresh = fresh[:8]                          # bot-like wallet: newest few only
        done = [s['signature'] for s in sigs if s not in fresh]
        seen[w] = (done + list(have))[:80]
        jobs += [(s.get('blockTime') or NOW, w, s) for s in fresh]
    jobs.sort(key=lambda j: -j[0])                     # newest first

    def get_tx(rpc, job):
        _, w, s = job
        try:
            tx = rpc.call('getTransaction', [s['signature'], {'encoding': 'jsonParsed',
                                                             'maxSupportedTransactionVersion': 0}], tries=2)
        except RuntimeError as e:
            if '-32015' in str(e):                    # a transaction format we cannot read: skip it for good
                seen[w] = [s['signature']] + (seen.get(w) or [])[:79]
                return
            raise
        if tx and tx.get('meta'):
            _sol_parse(w, s, tx, wset, newmoves)
            seen[w] = [s['signature']] + (seen.get(w) or [])[:79]
    not_done = _sol_pool(jobs, get_tx, deadline)
    rest_done = sum(1 for w in listed if w not in pri)
    cur['pos'] = (pos + rest_done) % max(1, len(rest))
    cur.update(covered=len(listed), of=len(order), txs=len(jobs) - len(not_done), tx_left=len(not_done),
               busy=busy, at=NOW)
    for w in list(seen):
        if w not in wset:
            seen.pop(w)


# ---------------------------------------------------------------- discovery
DISC_PATHS = ['trending_pools?duration=5m&page=1', 'trending_pools?duration=1h&page=1', 'new_pools?page=1',
              'trending_pools?duration=6h&page=1', 'trending_pools?duration=24h&page=1', 'new_pools?page=2']
DISC_MIN_MC = 500_000
DISC_MAX_AGE = 7 * 86400


def discover(chain, disc, st):
    """Collect tokens on this chain that are past $500K: two GeckoTerminal lists per pass, rotating through
    trending (5m/1h/6h/24h) and new pools, so over a few passes every list is covered."""
    net = GT_NET[chain]
    rot = st.setdefault('disc_rot', {})
    k = rot.get(chain, 0)
    rot[chain] = (k + 2) % len(DISC_PATHS)
    for path in (DISC_PATHS[k % len(DISC_PATHS)], DISC_PATHS[(k + 1) % len(DISC_PATHS)]):
        if left() < 60:
            return
        r = GT.get(f'/networks/{net}/{path}')
        for p in (r or {}).get('data', []):
            a = p.get('attributes', {})
            mc = _f(a.get('market_cap_usd')) or _f(a.get('fdv_usd')) or 0
            if mc < DISC_MIN_MC or mc > 2_000_000_000:
                continue
            sym0 = (a.get('name') or '?').split(' / ')[0].strip().upper()
            if sym0 in BASE_SYMBOLS or 'USD' in sym0:
                continue
            base = (p.get('relationships', {}).get('base_token', {}).get('data', {}) or {}).get('id', '')
            ca = base.split('_', 1)[-1]
            if not ca:
                continue
            if chain != 'Solana':
                ca = ca.lower()
            key = f'{chain}|{ca}'
            row = disc.get(key) or dict(chain=chain, ca=ca, first_seen=NOW)
            row.update(sym=html.unescape((a.get('name') or '?').split(' / ')[0])[:24],
                       mc=mc, ath=max(mc, row.get('ath') or 0), seen=NOW)
            disc[key] = row


def evm_backfill(chain, ca, created, wallets, st):
    """Every transfer of this token INTO a watched wallet since the token launched (getLogs with the token as
    address filter, so any block range is allowed). Resumes across passes; keeps each wallet's first receipt."""
    bf = st.setdefault('bf', {}).setdefault(f'{chain}|{ca}', {})
    if bf.get('v') != 3:
        bf.clear()
        bf['v'] = 3                                  # earlier entries used a token age that could be wrong
    if bf.get('done') or not wallets:
        return bf
    cfg = EVM[chain]
    rpc = RPC(chain, cfg['rpcs'], gap=0.12)
    try:
        head = int(rpc.call('eth_blockNumber', []), 16)
    except Exception:
        return bf
    if 'next' not in bf:
        age = max(NOW - (created or NOW), DISC_MAX_AGE)   # never trust a pool date alone: cover the full 7 days
        bf.update(next=max(0, head - int(age / cfg['block_s'] * 1.05) - 300), first={},
                  rng=100000 if chain == 'Robinhood' else 20000)
    wl = sorted(wallets)
    wset = set(wl)
    tops = [topic(w) for w in wl]
    t_end = time.time() + 20
    while bf['next'] <= head and time.time() < t_end and left() > 30:
        b = bf['next']
        e = min(head, b + bf['rng'] - 1)
        try:
            got = []
            for i in range(0, len(tops), 500):       # the token address filter lets one call carry every wallet
                got += rpc.call('eth_getLogs', [{'fromBlock': hex(b), 'toBlock': hex(e), 'address': ca,
                                                 'topics': [TRANSFER, None, tops[i:i + 500]]}]) or []
        except Exception as ex:
            msg = str(ex).lower()
            cap = re.search(r'only (\d+) are allowed|max(?:imum)? (?:block )?range[^0-9]*(\d+)|(\d+) block', msg)
            if bf['rng'] > 500 and any(x in msg for x in ('range', 'too many', 'exceed', 'large', 'limit', 'results', 'allowed')):
                n = next((int(g) for g in (cap.groups() if cap else ()) if g), 0)
                if 500 <= n < bf['rng']:
                    bf['rng'] = bf['cap'] = n          # the node told us its limit; remember it
                else:
                    bf['rng'] = max(500, bf['rng'] // 4)
                continue
            ERRORS.append(f'{chain} backfill: {str(ex)[:80]}')
            break
        for l in got:
            t = l.get('topics') or []
            if len(t) != 3:
                continue
            frm, to = '0x' + t[1][-40:], '0x' + t[2][-40:]
            if frm in wset or to not in wset:
                continue                    # a team wallet passing tokens to its sibling is not a buy
            bn = int(l['blockNumber'], 16)
            prev = bf['first'].get(to)
            try:
                raw = int(l.get('data') or '0x0', 16)
            except Exception:
                raw = 0
            if not prev or bn < prev[0]:
                bf['first'][to] = [bn, l['transactionHash'], str(raw)]
            elif bn == prev[0] and l['transactionHash'] == prev[1]:
                prev[2] = str(int(prev[2]) + raw)      # split delivery in the same buy
        bf['next'] = e + 1
        bf['rng'] = min(bf['rng'] * 2, bf.get('cap') or (100_000 if chain == 'Robinhood' else 50_000))
    if bf['next'] > head:
        bf['done'] = True
    tss = bf.setdefault('ts', {})
    for w, v in sorted(bf['first'].items(), key=lambda kv: kv[1][0])[:6]:
        if w not in tss and left() > 25:
            try:
                tss[w] = int(rpc.call('eth_getBlockByNumber', [hex(v[0]), False])['timestamp'], 16)
            except Exception:
                pass
    return bf


NATIVE_REF = {'BSC': ('BSC', '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'),          # WBNB
              'Base': ('Ethereum', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'),    # ETH (via mainnet WETH)
              'Ethereum': ('Ethereum', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'),
              'Robinhood': ('Ethereum', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')}


def usd_paid(chain, tx, wallet, ca, tokens):
    """Dollars a wallet paid in one buy: native coin sent with the transaction, or other tokens it sent out."""
    rpc = RPC(chain, EVM[chain]['rpcs'], gap=0.12)
    t = rpc.call('eth_getTransactionByHash', [tx]) or {}
    native = int(t.get('value') or '0x0', 16) / 1e18
    if native > 0:
        rc, ra = NATIVE_REF[chain]
        token_info(rc, [ra], tokens)
        px = (tokens.get(f'{rc}|{ra}') or {}).get('price')
        return native * px if px else None
    rcpt = rpc.call('eth_getTransactionReceipt', [tx]) or {}
    outs = {}
    for l in rcpt.get('logs') or []:
        tp = l.get('topics') or []
        if len(tp) == 3 and tp[0] == TRANSFER and tp[1] == topic(wallet) and l['address'].lower() != ca:
            outs[l['address'].lower()] = outs.get(l['address'].lower(), 0) + int(l.get('data') or '0x0', 16)
    if not outs:
        return None
    token_info(chain, list(outs)[:3], tokens)
    usd = 0.0
    for a, raw in outs.items():
        i = tokens.get(f'{chain}|{a}') or {}
        if i.get('price') and i.get('dec') is not None:
            usd += raw / 10 ** int(i['dec']) * i['price']
    return usd or None


def price_at(candles, ts):
    """Close of the last 1-minute candle at or before ts (candles: [t, o, h, l, c, v])."""
    best = None
    for c in candles:
        if c[0] <= ts and (best is None or c[0] > best[0]):
            best = c
    if best is None:
        early = [c for c in candles if c[0] <= ts + 600]
        if early:
            best = min(early, key=lambda c: c[0])
            return best[1]
        return None
    return best[4]


def check_entry(r, first, st, tokens):
    """Market cap when the first team wallet bought, when our scanner would have seen it, and the best it
    reached afterwards: the honest answer to 'could we actually have bought this?'."""
    net = GT_NET[r['chain']]
    pool = r.get('pool')
    if not pool or not first:
        return
    fts, fw, seen_ts = first[0]['ts'], first[0]['wallet'], first[0].get('seen') or first[0]['ts'] + 420
    if r.get('entry_ts') == fts and (r.get('entry_mc') or NOW - (r.get('entry_try') or 0) < 1800):
        return                                       # done, or tried recently: do not burn API calls every pass
    r['entry_try'] = NOW
    m1 = GT.get(f'/networks/{net}/pools/{pool}/ohlcv/minute?aggregate=1&before_timestamp={int(seen_ts) + 600}'
                f'&limit=60&currency=usd&token={r["ca"]}')
    h1 = GT.get(f'/networks/{net}/pools/{pool}/ohlcv/hour?aggregate=1&limit=200&currency=usd&token={r["ca"]}')
    mins = ((m1 or {}).get('data', {}).get('attributes', {}) or {}).get('ohlcv_list') or []
    hours = ((h1 or {}).get('data', {}).get('attributes', {}) or {}).get('ohlcv_list') or []
    if not hours:
        return
    last_close = max(hours, key=lambda c: c[0])[4]
    supply = (r.get('mc') or 0) / last_close if last_close else None
    if not supply:
        return
    p_first, p_seen = price_at(mins, fts), price_at(mins, seen_ts)
    f0 = first[0]
    if not p_first and r['chain'] in EVM and f0.get('tx') and f0.get('raw') and left() > 30:
        info = tokens.get(f"{r['chain']}|{r['ca']}") or {}
        try:                                         # no candles that early (launchpad curve): price the buy itself
            paid = usd_paid(r['chain'], f0['tx'], f0['wallet'], r['ca'], tokens)
            amt = int(f0['raw']) / 10 ** int(info['dec']) if info.get('dec') is not None else 0
            if paid and amt:
                p_first = paid / amt
                if info.get('price') and info.get('mc'):
                    supply = info['mc'] / info['price']
                r['entry_from'] = 'buy'
        except Exception as e:
            ERRORS.append(f'entry price {r["chain"]}: {type(e).__name__}')
    if not p_seen:                                   # no minute candle then: first price the pool ever showed
        later = sorted(c for c in hours if c[0] >= fts - 3600)
        if later:
            p_seen, seen_ts = later[0][4], max(int(seen_ts), later[0][0])
            r['seen_from'] = 'first candle'
    peak_p = max([c[2] for c in hours if c[0] >= fts - 3600] or [0])
    top = max(r.get('ath') or 0, r.get('mc') or 0)

    def sane(v):                                     # reject numbers a bad candle or pool could produce
        return round(v) if v and top and top / 1e4 < v < top * 20 else None
    entry_mc = sane(p_first * supply if p_first else None)
    seen_mc = sane(p_seen * supply if p_seen else None)
    if entry_mc and seen_mc and seen_mc < entry_mc:
        seen_mc = entry_mc                           # a follower never gets in cheaper than the team did
    r.update(entry_ts=fts, entry_wallet=fw, entry_mc=entry_mc,
             seen_ts=int(seen_ts), seen_mc=seen_mc,
             peak_after=max(sane(peak_p * supply) or 0, r.get('peak_after') or 0, r.get('mc') or 0))


def verdict(r):
    seen, peak = r.get('seen_mc') or r.get('entry_mc'), r.get('peak_after')
    if not r.get('first_buys'):
        return 'none'
    if not seen or not peak:
        return 'unknown'
    room = peak / seen
    r['room'] = round(room, 2)
    return 'yes' if room >= 2 else 'some' if room >= 1.3 else 'late'


# ---------------------------------------------------------------- team labels
ADJ = ['Amber', 'Arctic', 'Ash', 'Azure', 'Black', 'Blaze', 'Bronze', 'Cedar', 'Chrome', 'Cobalt', 'Copper', 'Coral',
       'Crimson', 'Dawn', 'Dusk', 'Ember', 'Emerald', 'Frost', 'Ghost', 'Gold', 'Granite', 'Iron', 'Ivory', 'Jade',
       'Lunar', 'Midnight', 'Neon', 'Night', 'Obsidian', 'Onyx', 'Orchid', 'Phantom', 'Pine', 'Quartz', 'Rapid',
       'Raven', 'Ruby', 'Rust', 'Sable', 'Sage', 'Scarlet', 'Shadow', 'Silent', 'Silver', 'Slate', 'Solar', 'Steel',
       'Storm', 'Swift', 'Thunder', 'Tidal', 'Titan', 'Velvet', 'Violet', 'Wild', 'Winter', 'Zinc', 'Hollow', 'Echo']
NOUN = ['Badgers', 'Bears', 'Bees', 'Bulls', 'Cobras', 'Comets', 'Condors', 'Coyotes', 'Cranes', 'Crows', 'Dingos',
        'Eagles', 'Falcons', 'Foxes', 'Geckos', 'Hawks', 'Herons', 'Hornets', 'Hounds', 'Jackals', 'Jaguars', 'Kites',
        'Lions', 'Lynxes', 'Mantas', 'Marlins', 'Minks', 'Moths', 'Orcas', 'Otters', 'Owls', 'Panthers', 'Pumas',
        'Ravens', 'Rhinos', 'Sharks', 'Sparrows', 'Stags', 'Swifts', 'Tigers', 'Vipers', 'Wasps', 'Wolves', 'Wrens']
CHAIN_SHORT = {'Solana': 'SOL', 'Robinhood': 'RH', 'BSC': 'BSC', 'Base': 'BASE', 'Ethereum': 'ETH'}


def team_style(g):
    """What kind of operation this is, in plain words, from how the research linked the wallets."""
    n, k = g['name'].lower(), g['kind']
    if k == 'copy':
        return 'Copy-trade picks', 'Hand-picked wallets that tested well when copied.'
    if k == 'deployer' or 'deployer' in n or 'launch' in n:
        return 'Launch team', 'Wallets tied to the token creator; they buy their own launches early.'
    if 'farm' in n or (n.startswith('funder ') and g['n'] >= 10):
        return 'Wallet farm', 'Many fresh wallets funded from one source, buying together to look like separate people.'
    if 'same-second' in n or 'same-block' in n:
        return 'Same-second snipers', 'Wallets that buy in the very same second, so one person or bot runs them.'
    if 'same-minute' in n:
        return 'Same-minute crew', 'Wallets that keep buying the same new tokens within a minute of each other.'
    if 'funder' in n:
        return 'One-funder crew', 'Wallets all funded by the same source wallet, trading the same tokens.'
    if k == 'cluster':
        return 'Linked cluster', 'Wallets linked by funding and timing that show up early together.'
    if k == 'solo' or g['n'] == 1:
        if 'insider' in n:
            return 'Solo insider', 'One wallet that keeps getting into winners before they pump.'
        return 'Smart early buyer', 'One wallet that is repeatedly early on tokens that go on to run.'
    return 'Insider team', 'Wallets linked together that keep getting in early on the same winners.'


def team_grade(g):
    w, f = g['winners'], g['failed']
    hit = w / (w + f) if (w + f) else 0
    if g['kind'] == 'copy':
        return 'Tested', hit
    if w >= 10 and hit >= 0.75:
        return 'Elite', hit
    if w >= 5 and hit >= 0.6:
        return 'Strong', hit
    if w >= 3:
        return 'Decent', hit
    return 'Unproven', hit


def label_groups(groups):
    used = set()
    for g in sorted(groups, key=lambda g: g['id']):
        h = zlib.crc32(g['id'].encode())
        while True:
            nick = f"{ADJ[h % len(ADJ)]} {NOUN[(h >> 12) % len(NOUN)]}"
            if (g['chain'], nick) not in used:
                break
            h = (h * 2654435761 + 1) & 0xffffffff
        used.add((g['chain'], nick))
        style, about = team_style(g)
        grade, hit = team_grade(g)
        g.update(nick=nick, style=style, about=about, grade=grade, hit=round(hit, 3),
                 label=f"{nick} · {style}", short=f"{nick} ({CHAIN_SHORT.get(g['chain'], g['chain'])})",
                 research_name=g['name'])
    return groups


# ---------------------------------------------------------------- phone alerts (ntfy.sh, free, no account)
GMGN_NET = {'Solana': 'sol', 'Robinhood': 'robinhood', 'BSC': 'bsc', 'Base': 'base', 'Ethereum': 'eth'}


def notify(rows, gmap):
    topic = os.environ.get('NTFY_TOPIC', 'insider-live-49ab55a62a52').strip()
    for r in rows:
        if r.get('notified') or not topic:
            continue
        if NOW - r['first_signal'] > 1800:
            r['notified'] = True             # too old to be useful on a phone
            continue
        names = ', '.join(gmap[g].get('nick', g) + (' *' if gmap[g].get('legit') else '') for g in r['teams'][:3] if g in gmap)
        age = r.get('age_at_signal_h') or 0
        age_txt = f"{int(age * 60)}m old" if age < 1 else f"{age:.0f}h old"
        mc = r.get('mc_at_signal') or 0
        body = (f"{r['n_teams']} proven team(s), {r['n_wallets']} wallet(s) bought ${r['buy_usd']:,.0f} "
                f"in {r['sym']} on {r['chain']} ({age_txt}, MC ${mc:,.0f}). {names}")
        payload = dict(topic=topic, title=f"BUY SIGNAL {r['sym']} ({r['chain']})", message=body,
                       tags=['rotating_light'], priority=4,
                       click=f"https://gmgn.ai/{GMGN_NET.get(r['chain'], r['chain'])}/token/{r['ca']}")
        try:                                  # JSON body: emoji / Chinese names are fine, unlike HTTP headers
            req = urllib.request.Request('https://ntfy.sh/', data=json.dumps(payload).encode('utf-8'),
                                         headers={'Content-Type': 'application/json', 'User-Agent': UA})
            urllib.request.urlopen(req, timeout=10).read()
            r['notified'] = True              # only once it really went out; otherwise retried next pass
        except Exception as e:
            ERRORS.append(f'ntfy: {type(e).__name__}')


# ---------------------------------------------------------------- seed
ROLE = {'l': 'leader', 'm': 'member', 'f': 'funder', 's': 'solo', 'd': 'deployer', 'p': 'pre-pump buyer'}


def load_seed():
    """data/seed.txt: id, chain, kind, tier, name, winners, failed, profit, last_ts, wallets ("addr:r addr:r ...")."""
    groups, watch = [], {}
    with open(os.path.join(DATA, 'seed.txt'), encoding='utf-8') as f:
        for line in f:
            p = line.rstrip('\n').split('|')
            if len(p) < 10 or p[0].startswith('#'):
                continue
            gid, chain = p[0], p[1]
            ws = []
            for tok in p[9].split():
                w, _, r = tok.partition(':')
                if chain != 'Solana':
                    w = w.lower()
                ws.append(dict(w=w, role=ROLE.get(r, r)))
                watch.setdefault(chain, {}).setdefault(w, [])
                if gid not in watch[chain][w]:
                    watch[chain][w].append(gid)
            groups.append(dict(id=gid, chain=chain, kind=p[2], tier=p[3], name=p[4], n=len(ws),
                               winners=int(p[5] or 0), failed=int(p[6] or 0), profit=int(p[7] or 0),
                               seed_last_ts=int(p[8]) if p[8] else None, wallets=ws,
                               legit=int(p[10]) if len(p) > 10 and p[10].isdigit() else 0))
    return label_groups(groups), watch


def load_flags(groups, watch):
    """data/flags.txt from the research:
         team|<group id>|<label>|<reason>          a seed team caught dumping on followers: never drives signals
         ring|<chain>|<wallet>|<role>|<ring name>|<note>   KOL dump-ring wallets: watched, never followed"""
    path = os.path.join(DATA, 'flags.txt')
    if not os.path.exists(path):
        return
    byid = {g['id']: g for g in groups}
    rings, kols = {}, {}
    with open(path, encoding='utf-8') as f:
        for line in f:
            p = line.rstrip('\n').split('|')
            if p[0] == 'team' and len(p) >= 4 and p[1] in byid:
                byid[p[1]].update(flag=p[2], flag_reason=p[3], flag_src='research')
            elif p[0] == 'kol' and len(p) >= 8:
                chain, ws, name, tw, grade, why, stats = p[1], p[2], p[3], p[4], p[5], p[6], p[7]
                gid = 'KOL:' + (re.sub(r'[^A-Za-z0-9]+', '', tw or name)[:20] or 'x') + \
                      f'-{zlib.crc32((name + chain).encode()) % 1000:03d}'
                try:
                    st_ = json.loads(stats) if stats else {}
                except Exception:
                    st_ = {}
                wl = [w if chain == 'Solana' else w.lower() for w in ws.split(';') if w]
                if not wl or gid in kols:
                    continue
                kols[gid] = dict(id=gid, chain=chain, kind='kol', tier='K', name=name, twitter=tw, n=len(wl),
                                 winners=0, failed=0, profit=0, seed_last_ts=None, wallets=wl, legit=0,
                                 nick=name, style='KOL', about='', grade='KOL', hit=0, label=f'{name} (KOL)',
                                 short=f"{name} ({CHAIN_SHORT.get(chain, chain)})", research_name=name,
                                 kol_grade=grade, kol_why=why, kol_stats=st_)
                for w in wl:
                    lst = watch.setdefault(chain, {}).setdefault(w, [])
                    if gid not in lst:
                        lst.append(gid)
            elif p[0] == 'ring' and len(p) >= 6:
                chain, w, role, name, note = p[1], p[2], p[3], p[4], p[5]
                if chain != 'Solana':
                    w = w.lower()
                gid = 'RING:' + (re.sub(r'[^A-Za-z0-9]+', '', name)[:20] or 'x') + f'-{zlib.crc32(name.encode()) % 1000:03d}'
                g = rings.get((chain, gid))
                if not g:
                    g = rings[(chain, gid)] = dict(id=gid, chain=chain, kind='ring', tier='R', name=name, n=0,
                                                   winners=0, failed=0, profit=0, seed_last_ts=None, wallets=[],
                                                   legit=0, nick=f'{name} ring', style='KOL dump ring',
                                                   about=note, grade='Ring', hit=0, label=f'{name} ring · KOL dump ring',
                                                   short=f"{name} ring ({CHAIN_SHORT.get(chain, chain)})",
                                                   research_name=name, flag='KOL dump ring',
                                                   flag_reason=note)
                g['wallets'].append(dict(w=w, role=role))
                g['n'] = len(g['wallets'])
                lst = watch.setdefault(chain, {}).setdefault(w, [])
                if gid not in lst:
                    lst.append(gid)
    groups.extend(rings.values())
    groups.extend(kols.values())


def auto_flags(groups, moves, tokens):
    """Live check of the Cupsey pattern inside our own teams: selling tokens the team never bought (they were
    sent in by a deployer or an operational wallet) on tokens launched while we were watching."""
    if not moves:
        return
    t0 = min(m['ts'] for m in moves)
    got, recv, sold = set(), {}, {}
    for m in sorted(moves, key=lambda m: m['ts']):
        key = (m['wallet'], m['chain'], m['ca'])
        if m['side'] in ('buy', 'internal'):
            got.add(key)
            continue
        ti = tokens.get(f"{m['chain']}|{m['ca']}") or {}
        cr = ti.get('created')
        if not cr or cr < t0 + 600 or (ti.get('mc') or 0) > 50_000_000:
            continue                                  # older than our data (buy may be before it), or a big minted asset
        for g in m['groups']:
            sold[g] = sold.get(g, 0) + (m.get('usd') or 0)
            if key not in got:
                recv[g] = recv.get(g, 0) + (m.get('usd') or 0)
    for g in groups:
        r, s_ = recv.get(g['id'], 0), sold.get(g['id'], 0)
        g['recv_sold_usd'] = round(r)
        if not g.get('flag') and r >= 3000 and s_ and r / s_ >= 0.5:
            g.update(flag='Dumps tokens it never bought',
                     flag_reason=f'${r:,.0f} of its ${s_:,.0f} sells on new tokens were tokens sent to it, not bought '
                                 f'(the Cupsey side-wallet pattern)', flag_src='live')


def risk_tokens(moves, gmap, tokens):
    """Per token: a KOL dump ring touched it in the last 24h, or watched wallets dumped $2K+ of it in the last
    6h without ever buying it."""
    RISK_TOK.clear()
    if not moves:
        return
    t0 = min(m['ts'] for m in moves)
    got, dump = set(), {}
    for m in sorted(moves, key=lambda m: m['ts']):
        k = f"{m['chain']}|{m['ca']}"
        ring = [gmap[g]['name'] for g in m['groups'] if g in gmap and gmap[g].get('kind') == 'ring']
        if ring and NOW - m['ts'] < 86400:
            why = f'KOL dump ring active ({ring[0]})'
            if why not in RISK_TOK.setdefault(k, []):
                RISK_TOK[k].append(why)
        key = (m['wallet'], k)
        if m['side'] in ('buy', 'internal'):
            got.add(key)
        elif m['side'] == 'sell' and key not in got and NOW - m['ts'] < 6 * 3600:
            cr = (tokens.get(k) or {}).get('created') or 0
            if cr > t0 + 600:                         # launched while we were watching: no hidden earlier buy
                dump[k] = dump.get(k, 0) + (m.get('usd') or 0)
    for k, v in dump.items():
        if v >= 2000:
            RISK_TOK.setdefault(k, []).append(f'insiders dumping ${v:,.0f} of tokens they never bought')


KOL_ORDER = ['Follow', 'Watch', 'Not enough data', "Don't follow", 'Exit liquidity', 'Avoid: dump ring']


def kol_followups(kmoves, tokens):
    """Market cap 1h and 6h after each KOL buy: what someone who copied it and held would have."""
    need = {}
    for m in kmoves:
        if m['side'] != 'buy' or not m.get('mc'):
            continue
        age = NOW - m['ts']
        for h, key in ((1, 'mc_1h'), (6, 'mc_6h')):
            if key not in m and h * 3600 <= age <= (h + 4) * 3600:
                need.setdefault(m['chain'], {})[m['ca']] = 1
    for chain, cas in need.items():
        if left() < SCAN_RESERVE / 2:
            break
        token_info(chain, list(cas)[:120], tokens)
    for m in kmoves:
        if m['side'] != 'buy' or not m.get('mc'):
            continue
        age = NOW - m['ts']
        for h, key in ((1, 'mc_1h'), (6, 'mc_6h')):
            if key not in m and h * 3600 <= age <= (h + 4) * 3600:
                i = tokens.get(f"{m['chain']}|{m['ca']}") or {}
                if i.get('at') == NOW and i.get('mc'):
                    m[key] = i['mc']


def kol_rows(groups, kmoves, tokens, safe):
    """One row per KOL: research grade (30-day copy backtest + ring check), adjusted by what we see live."""
    rows = []
    by = {}
    for m in kmoves:
        for g in m['groups']:
            by.setdefault(g, []).append(m)
    for g in groups:
        if g.get('kind') != 'kol':
            continue
        ms = sorted(by.get(g['id'], []), key=lambda m: -m['ts'])
        buys = [m for m in ms if m['side'] == 'buy' and (m.get('usd') or 0) >= 100]
        first = {}
        for m in sorted(buys, key=lambda m: m['ts']):
            first.setdefault(m['ca'], m)
        r1 = [max(-1, min(m['mc_1h'] / m['mc'] - 1, 10)) for m in first.values() if m.get('mc_1h') and m.get('mc')]
        r6 = [max(-1, min(m['mc_6h'] / m['mc'] - 1, 10)) for m in first.values() if m.get('mc_6h') and m.get('mc')]
        med = lambda v: round(sorted(v)[len(v) // 2], 3) if v else None
        live = dict(n1=len(r1), med1=med(r1), win1=round(sum(x > 0 for x in r1) / len(r1), 2) if r1 else None,
                    mean1=round(sum(r1) / len(r1), 3) if r1 else None, n6=len(r6), med6=med(r6))
        grade, why = g.get('kol_grade') or 'Not enough data', [x for x in (g.get('kol_why') or '').split(';') if x]
        if len(r1) >= 8:                              # live results can lower a grade, or lift one we had no data for
            if grade == 'Follow' and (live['med1'] < -0.15 or live['mean1'] < -0.05):
                grade = 'Watch'
                why.insert(0, f"live: copies held 1h lost (median {live['med1']:+.0%} over {len(r1)} buys)")
            elif grade == 'Not enough data':
                grade = 'Watch' if live['med1'] >= 0 else "Don't follow"
                why.insert(0, f"live only: median {live['med1']:+.0%} 1h after {len(r1)} buys")
        risky = [m for m in buys[:20] if RISK_TOK.get(f"{m['chain']}|{m['ca']}")]
        if len(risky) >= 3 and grade not in ('Avoid: dump ring',):
            why.insert(0, f'live: {len(risky)} of its recent buys had ring or insider dumping')
            if grade == 'Follow':
                grade = 'Watch'
        recent = []
        for m in buys[:8]:
            k = f"{m['chain']}|{m['ca']}"
            info = tokens.get(k) or {}
            ok, risk = tradable(info, safe.get(k), strict=False, key=k) if info else (None, [])
            recent.append(dict(ca=m['ca'], sym=m['sym'], ts=m['ts'], usd=m['usd'], mc=m['mc'], mc_now=info.get('mc'),
                               tradable=ok, risk=risk[:2]))
        rows.append(dict(id=g['id'], name=g['name'], twitter=g.get('twitter'), chain=g['chain'], wallets=g['wallets'],
                         grade=grade, why=why[:5], research=g.get('kol_stats') or {}, live=live,
                         buys_3d=sum(1 for m in buys if NOW - m['ts'] < 3 * 86400),
                         last_buy=buys[0]['ts'] if buys else None, recent=recent))
    rows.sort(key=lambda r: (KOL_ORDER.index(r['grade']) if r['grade'] in KOL_ORDER else 9,
                             -((r['research'] or {}).get('copy5_mean') or -9)))
    return rows


# ---------------------------------------------------------------- main
def main():
    groups, watch = load_seed()                              # teams + wallets from the research
    load_flags(groups, watch)                                # research flags + KOL dump-ring wallets
    st = load(os.path.join(DATA, 'state.json'), {})
    tokens = st.setdefault('tokens', {})
    moves = load(os.path.join(OUT, 'moves.json'), {}).get('rows', [])
    kmoves = load(os.path.join(OUT, 'kol_moves.json'), {}).get('rows', [])   # KOL wallets: kept apart from teams
    kol_ids = {g['id'] for g in groups if g.get('kind') == 'kol'}
    auto_flags(groups, moves, tokens)
    alerts = load(os.path.join(OUT, 'alerts.json'), {}).get('rows', [])
    disc = {f"{r['chain']}|{r['ca']}": r for r in load(os.path.join(OUT, 'discovery.json'), {}).get('rows', [])}

    newmoves = []
    # proven = graded Elite/Strong/Tested or early on a lasting token; these drive buy signals
    good_ids = {g['id'] for g in groups if (g['grade'] in ('Elite', 'Strong', 'Tested') or g.get('legit'))
                and not g.get('flag')}                       # flagged dumpers and rings never drive signals
    proven = {}
    for chain, ws in watch.items():
        proven[chain] = {w for w, gids in ws.items() if any(x in good_ids for x in gids)}
    chains_order = ['Solana', 'Robinhood', 'BSC', 'Base', 'Ethereum']
    sol_deadline = time.time() + 0.40 * (left() - SCAN_RESERVE)    # Solana may not starve the EVM chains
    for chain in chains_order:
        if left() < SCAN_RESERVE + 20:
            ERRORS.append(f'{chain}: skipped this pass (time budget)')
            continue
        try:
            if chain == 'Solana':
                scan_solana(list(watch.get('Solana', {})), st, newmoves, priority=proven.get('Solana', ()),
                            deadline=sol_deadline)
            elif chain in BLOCKSCOUT:
                scan_blockscout(chain, [w.lower() for w in watch.get(chain, {})], st, newmoves)
            elif chain in ('BSC', 'Base'):
                scan_evm(chain, EVM[chain], [w.lower() for w in watch.get(chain, {})], st, newmoves,
                         contracts=bsc_contracts(st, moves, chain))
            else:
                scan_evm(chain, EVM[chain], [w.lower() for w in watch.get(chain, {})], st, newmoves)
        except Exception as e:
            ERRORS.append(f'{chain}: {type(e).__name__} {str(e)[:100]}')

    # moves we could not price last time (token not indexed yet, rate limit) get another try for an hour
    newmoves = st.get('pending', []) + newmoves
    # token names / prices for everything new
    by_chain = {}
    for m in newmoves:
        by_chain.setdefault(m['chain'], []).append(m['ca'])
    for chain, cas in by_chain.items():
        need = [c for c in dict.fromkeys(cas)]
        token_info(chain, need, tokens)
    sol_price = None
    if 'Solana' in by_chain:
        token_info('Solana', ['So11111111111111111111111111111111111111112'], tokens)
        sol_price = (tokens.get('Solana|So11111111111111111111111111111111111111112') or {}).get('price')

    have = {(m['chain'], m['tx'], m['wallet'], m['ca'], m['side']) for m in moves + kmoves}
    pending = []
    for m in newmoves:
        k = (m['chain'], m['tx'], m['wallet'], m['ca'], m['side'])
        if k in have:
            continue
        have.add(k)
        info = tokens.get(f"{m['chain']}|{m['ca']}") or {}
        sym = info.get('sym') or '?'
        if sym.upper() in BASE_SYMBOLS:
            continue
        if m['chain'] == 'Solana':
            amount = m['amount']
            if abs(m.get('stable', 0)) >= 1:
                usd = abs(m['stable'])
            elif sol_price and abs(m.get('sol', 0)) > 0.01:      # above token-account rent and fees
                usd = abs(m['sol']) * sol_price
            else:
                usd = None
        else:
            dec = info.get('dec')
            amount = int(m['raw']) / (10 ** int(dec)) if dec is not None else None
            usd = None
        if usd is None and amount is not None and info.get('price'):
            usd = amount * info['price']
        if usd is None:
            have.discard(k)
            if NOW - m['ts'] < 3600:
                pending.append(m)          # try again next pass
            continue                       # unpriced after an hour: airdrop spam / unknown token
        if usd < 5:
            continue                       # dust and rounding noise
        gids = watch.get(m['chain'], {}).get(m['wallet'] if m['chain'] == 'Solana' else m['wallet'].lower()) \
            or watch.get(m['chain'], {}).get(m['wallet'], [])
        row = dict(chain=m['chain'], wallet=m['wallet'], groups=gids, ts=m['ts'], side=m['side'],
                   ca=m['ca'], sym=sym, usd=round(usd, 2) if usd else None,
                   mc=info.get('mc'), tx=m['tx'], new=NOW)
        team_ids = [g for g in gids if g not in kol_ids]
        if team_ids and len(team_ids) < len(gids):
            kmoves.append(dict(row, groups=[g for g in gids if g in kol_ids]))
            row['groups'] = team_ids
        if team_ids:
            moves.append(row)
        else:
            kmoves.append(row)

    st['pending'] = pending[-500:]
    cut = NOW - KEEP_DAYS * 86400
    moves = [m for m in moves if m['ts'] >= cut]
    moves.sort(key=lambda m: -m['ts'])
    moves = moves[:6000]
    kmoves = [m for m in kmoves if m['ts'] >= cut and m['side'] != 'internal']
    kmoves.sort(key=lambda m: -m['ts'])
    kmoves = kmoves[:6000]
    risk_tokens(moves, {g['id']: g for g in groups}, tokens)

    # group status from the freshest move
    gl = {}
    for m in moves:
        if m['side'] == 'internal':
            continue
        for g in m['groups']:
            if g not in gl or m['ts'] > gl[g]['ts']:
                gl[g] = m
    out_groups = []
    for g in groups:
        r = dict(g)
        lm = gl.get(g['id'])
        last_ts = lm['ts'] if lm else g.get('seed_last_ts')
        if lm:
            r.update(last_side=lm['side'], last_sym=lm['sym'], last_ca=lm['ca'])
        r['last_ts'] = last_ts
        age = NOW - last_ts if last_ts else None
        r['status'] = ('dormant' if age is None else 'live' if age < 3600 else 'active' if age < 86400
                       else 'recent' if age < 3 * 86400 else 'quiet' if age < 30 * 86400 else 'dormant')
        r['buys_7d'] = sum(1 for m in moves if g['id'] in m['groups'] and m['side'] == 'buy')
        out_groups.append(r)

    # alerts: several wallets of one group, or several groups, buying the same token within 6 h
    gname = {g['id']: g['short'] for g in groups}
    win = NOW - 6 * 3600
    buys = {}
    for m in moves:
        if m['side'] != 'buy' or m['ts'] < win or (m.get('usd') or 0) < 50:
            continue
        b = buys.setdefault((m['chain'], m['ca']), dict(sym=m['sym'], mc=m['mc'], by={}, usd=0, first=m['ts']))
        for g in m['groups']:
            b['by'].setdefault(g, set()).add(m['wallet'])
        b['usd'] += m['usd'] or 0
        b['first'] = min(b['first'], m['ts'])
    akeys = {(a['chain'], a['ca'], a['kind'], a.get('group', '')): a for a in alerts}
    for (chain, ca), b in buys.items():
        multi = [g for g, ws in b['by'].items()]
        if len({frozenset(ws) for ws in b['by'].values()}) < 2 or len(set().union(*b['by'].values())) < 2:
            multi = []                     # the same wallets sitting in overlapping teams are not separate teams
        for g, ws in b['by'].items():
            if len(ws) >= 2:
                k = (chain, ca, 'group', g)
                a = akeys.get(k) or dict(chain=chain, ca=ca, kind='group', group=g, first_alert=NOW, mc_at_alert=b['mc'])
                a.update(sym=b['sym'], mc=b['mc'], wallets=len(ws), group_name=gname.get(g, g),
                         usd=round(b['usd'], 2), first_buy=b['first'], updated=NOW)
                akeys[k] = a
        if len(multi) >= 2:
            k = (chain, ca, 'multi', '')
            a = akeys.get(k) or dict(chain=chain, ca=ca, kind='multi', group='', first_alert=NOW, mc_at_alert=b['mc'])
            a.update(sym=b['sym'], mc=b['mc'], groups=sorted(multi)[:12], n_groups=len(multi),
                     group_names=[gname.get(g, g) for g in sorted(multi)[:6]],
                     usd=round(b['usd'], 2), first_buy=b['first'], updated=NOW)
            akeys[k] = a
    alerts = [a for a in akeys.values() if a['first_alert'] >= cut]
    # refresh market caps of alerted tokens so the page shows what happened next
    for chain in GT_NET:
        cas = [a['ca'] for a in alerts if a['chain'] == chain and NOW - a.get('updated', 0) < 2 * 86400]
        if cas and left() > 60:
            token_info(chain, cas[:60], tokens)
    for a in alerts:
        info = tokens.get(f"{a['chain']}|{a['ca']}") or {}
        if info:
            a.update(token_extra(info))
            ok, why = tradable(info, (st.get('safe') or {}).get(f"{a['chain']}|{a['ca']}"), key=f"{a['chain']}|{a['ca']}")
            a.update(tradable=ok, risk=why)
        if info.get('mc'):
            a['mc_now'] = info['mc']
            a.setdefault('mc_at_alert', a.get('mc') or info['mc'])
            a['peak_mc'] = max(a.get('peak_mc') or 0, info['mc'])
    alerts.sort(key=lambda a: -a.get('updated', 0))

    # hot board: every token our teams bought in the last 3 days, and how it has done since the first team buy
    since3 = NOW - 3 * 86400
    hot = {}
    for m in sorted(moves, key=lambda m: m['ts']):
        if m['side'] == 'internal' or m['ts'] < since3:
            continue
        k = f"{m['chain']}|{m['ca']}"
        h = hot.setdefault(k, dict(chain=m['chain'], ca=m['ca'], sym=m['sym'], first_buy=None, mc_first=None,
                                   buy_usd=0.0, sell_usd=0.0, buys=0, sells=0, wallets=set(), groups=set(),
                                   last=0, buys_6h=0, usd_6h=0.0, groups_6h=set()))
        h['last'] = max(h['last'], m['ts'])
        if m['side'] == 'buy':
            if h['first_buy'] is None:
                h['first_buy'], h['mc_first'] = m['ts'], m.get('mc')
            h['buys'] += 1
            h['last_buy'] = m['ts']
            h['buy_usd'] += m.get('usd') or 0
            h['wallets'].add(m['wallet'])
            h['groups'].update(m['groups'])
            if m['ts'] >= NOW - 6 * 3600:
                h['buys_6h'] += 1
                h['usd_6h'] += m.get('usd') or 0
                h['groups_6h'].update(m['groups'])
        else:
            h['sells'] += 1
            h['sell_usd'] += m.get('usd') or 0
    hot = {k: h for k, h in hot.items() if h['first_buy']}
    for chain in GT_NET:
        cas = [h['ca'] for h in sorted(hot.values(), key=lambda h: -h['last']) if h['chain'] == chain][:90]
        if cas and left() > 50:
            token_info(chain, cas, tokens)
    hrows = []
    for h in hot.values():
        info = tokens.get(f"{h['chain']}|{h['ca']}") or {}
        mc_now = info.get('mc')
        chg = (mc_now / h['mc_first'] - 1) if (mc_now and h['mc_first']) else None
        sf = (st.get('safe') or {}).get(f"{h['chain']}|{h['ca']}")
        if not sf and any(g in good_ids for g in h['groups_6h']) and h['buys_6h']:
            sf = safety(h['chain'], h['ca'], st)
        ok, why = tradable(info, sf, key=f"{h['chain']}|{h['ca']}")
        hrows.append(dict(tradable=ok, risk=why, holders=(sf or {}).get('holders'), top10=(sf or {}).get('top10'),
                          chain=h['chain'], ca=h['ca'], sym=info.get('sym') or h['sym'], first_buy=h['first_buy'],
                          last=h['last'], last_buy=h.get('last_buy'), mc_first=h['mc_first'], mc_now=mc_now,
                          change=round(chg, 4) if chg is not None else None, liq=info.get('liq'),
                          buy_usd=round(h['buy_usd'], 2), sell_usd=round(h['sell_usd'], 2), buys=h['buys'],
                          sells=h['sells'], n_wallets=len(h['wallets']), n_groups=len(h['groups']),
                          groups=sorted(h['groups'])[:8], buys_6h=h['buys_6h'], usd_6h=round(h['usd_6h'], 2),
                          groups_6h=sorted(h['groups_6h'])[:12], **token_extra(info)))
    hrows.sort(key=lambda r: -(r['last_buy'] or 0))

    # follower score: what copying each team would have done lately. The research grade measures who was early,
    # which also rewards wallets that dump on their followers; the backtest showed it does not predict this.
    mc_now = {f"{r['chain']}|{r['ca']}": r.get('mc_now') for r in hrows}
    firsts, fol = {}, {}
    for m in sorted(moves, key=lambda m: m['ts']):
        if m['side'] == 'buy' and (m.get('usd') or 0) >= 50 and m.get('mc') and 3600 <= NOW - m['ts'] <= 3 * 86400:
            for g in m['groups']:
                firsts.setdefault((g, m['chain'], m['ca']), m)
    for (g, c, ca), m in firsts.items():
        now_mc = mc_now.get(f'{c}|{ca}')
        if now_mc:
            fol.setdefault(g, []).append(max(-1.0, min(now_mc / m['mc'] - 1, 10.0)))
    follow = {g: dict(n=len(v), med=round(sorted(v)[len(v) // 2], 3), win=round(sum(x > 0 for x in v) / len(v), 2))
              for g, v in fol.items()}
    for r in out_groups:
        if r['id'] in follow:
            r['follow'] = follow[r['id']]
    trusted = {g for g in good_ids if (follow.get(g) or {}).get('n', 0) >= 5 and follow[g]['med'] >= 0}

    # buy signals: proven teams putting real money into a fresh launch, not dust and not exits
    sigs = {f"{r['chain']}|{r['ca']}": r for r in load(os.path.join(OUT, 'signals.json'), {}).get('rows', [])}
    win2 = NOW - 2 * 3600
    cand = {}
    for m in moves:
        if m['ts'] < win2 or m['side'] == 'internal':
            continue
        if not any(g in good_ids for g in m['groups']):
            continue
        k = f"{m['chain']}|{m['ca']}"
        c = cand.setdefault(k, dict(chain=m['chain'], ca=m['ca'], sym=m['sym'], buy=0.0, sell=0.0, wallets=set(),
                                    teams=set(), first=m['ts'], big=0.0))
        usd = m.get('usd') or 0
        if m['side'] == 'buy':
            if usd < 50:
                continue                              # dust
            c['buy'] += usd
            c['big'] = max(c['big'], usd)
            c['wallets'].add(m['wallet'])
            c['teams'].update(g for g in m['groups'] if g in good_ids)
            c['first'] = min(c['first'], m['ts'])
        else:
            c['sell'] += usd
    need = {}
    for c in cand.values():
        need.setdefault(c['chain'], []).append(c['ca'])
    for chain, lst in need.items():
        if left() > 45:
            token_info(chain, lst[:60], tokens)       # fresh price change and age before judging
    for k, c in cand.items():
        info = tokens.get(k) or {}
        state, age_h = token_state(info)
        if age_h is None or age_h > 24 or state == 'fading':
            continue
        sym0 = (info.get('sym') or c['sym'] or '').upper()
        if sym0 in BASE_SYMBOLS or 'USD' in sym0:
            continue                                  # stablecoins and base coins are not launches
        sf = safety(c['chain'], c['ca'], st)
        ok, why = tradable(info, sf, key=k)
        if not ok or not sf:
            continue                                  # never signal a coin we cannot show is real and sellable
        strong = (c['buy'] >= 300 and len(c['wallets']) >= 2) or c['big'] >= 500
        if not (c['teams'] & trusted):
            continue                                  # at least one team whose recent buys paid a follower
        if not strong or c['sell'] > 0.5 * c['buy']:
            continue
        r = sigs.get(k)
        if not r:
            r = dict(chain=c['chain'], ca=c['ca'], first_signal=NOW, mc_at_signal=info.get('mc'),
                     age_at_signal_h=round(age_h, 2), first_buy=c['first'], notified=False)
            sigs[k] = r
        r.update(trusted=sorted(c['teams'] & trusted)[:6], sym=info.get('sym') or c['sym'], buy_usd=round(c['buy'], 2), sell_usd=round(c['sell'], 2),
                 n_wallets=len(c['wallets']), teams=sorted(c['teams'])[:8], n_teams=len(c['teams']),
                 biggest=round(c['big'], 2), last_seen=NOW)
    cas = {}
    for r in sigs.values():
        if NOW - r['first_signal'] < 3 * 86400:
            cas.setdefault(r['chain'], []).append(r['ca'])
    for chain, lst in cas.items():
        if left() > 40:
            token_info(chain, lst[:60], tokens)
    for k, r in sigs.items():
        info = tokens.get(k) or {}
        if info.get('mc'):
            r['mc_now'] = info['mc']
            r['peak_mc'] = max(r.get('peak_mc') or 0, info['mc'])
            if r.get('mc_at_signal'):
                r['change'] = round(info['mc'] / r['mc_at_signal'] - 1, 4)
                r['peak_change'] = round(r['peak_mc'] / r['mc_at_signal'] - 1, 4)
        r.update({kk: v for kk, v in token_extra(info).items() if kk in ('state', 'age_h', 'ch1', 'ch6', 'liq')} if info else {})
        r['liq'] = info.get('liq')
        sf = (st.get('safe') or {}).get(k)
        if info and not sf and NOW - r['first_signal'] < 86400:
            sf = safety(r['chain'], r['ca'], st)
        if info:
            ok, why = tradable(info, sf, key=k)
            r.update(tradable=ok and bool(sf), risk=why if why or sf else ['holder data not checked yet'],
                     holders=(sf or {}).get('holders'), top10=(sf or {}).get('top10'))
        sym0 = (r.get('sym') or '').upper()
        if sym0 in BASE_SYMBOLS or 'USD' in sym0:
            r.update(tradable=False, risk=['stablecoin or base coin'])
    srows = sorted((r for r in sigs.values() if NOW - r['first_signal'] < 7 * 86400), key=lambda r: -r['first_signal'])
    notify([r for r in srows if r.get('tradable')], {g['id']: g for g in groups})

    # discovery: every token past $500K in the last 7 days, whether our teams bought it, at what market cap,
    # and whether a follower could still have made money after our scanner saw the buy
    for chain in ['Solana', 'Robinhood', 'BSC', 'Base', 'Ethereum']:
        if left() < 70:
            break
        try:
            discover(chain, disc, st)
        except Exception as e:
            ERRORS.append(f'discovery {chain}: {type(e).__name__}')
    for m in moves:                                   # anything our teams bought that went past $500K
        if m['side'] == 'buy' and (m.get('mc') or 0) >= DISC_MIN_MC:
            disc.setdefault(f"{m['chain']}|{m['ca']}", dict(chain=m['chain'], ca=m['ca'], first_seen=NOW,
                                                             sym=m['sym'], mc=m['mc'], ath=m['mc'], seen=NOW))
    by_c = {}
    for r in sorted(disc.values(), key=lambda r: -r.get('seen', 0)):
        by_c.setdefault(r['chain'], []).append(r['ca'])
    for chain, cas in by_c.items():
        if left() > 60:
            token_info(chain, cas[:60], tokens)
    for key, r in list(disc.items()):
        info = tokens.get(key) or {}
        if info:
            r.update(sym=info.get('sym') or r.get('sym'), liq=info.get('liq'), vol24=info.get('vol24'),
                     created=info.get('created') or r.get('created'), pool=info.get('pool') or r.get('pool'))
            if info.get('mc'):
                r['mc'] = info['mc']
                r['ath'] = max(r.get('ath') or 0, info['mc'])
                r['peak_after'] = max(r.get('peak_after') or 0, info['mc']) if r.get('entry_ts') else r.get('peak_after')
        if r.get('created') and NOW - r['created'] > DISC_MAX_AGE:
            disc.pop(key)                              # older than 7 days: not a fresh launch any more
            continue
        liq = r.get('liq')
        if liq is not None and (liq < 10_000 or (r.get('mc') or 0) > 1000 * liq):
            disc.pop(key)                              # fake market cap: almost nothing behind it
    order = sorted(disc.values(), key=lambda r: (r.get('checked') or 0, -(r.get('created') or 0)))
    for r in order:
        if left() < 35:
            break
        chain, ca = r['chain'], r['ca']
        firsts = {}
        for m in moves:
            if m['chain'] == chain and m['ca'] == ca and m['side'] == 'buy':
                w = m['wallet']
                if w not in firsts or m['ts'] < firsts[w]['ts']:
                    firsts[w] = dict(wallet=w, ts=m['ts'], usd=m.get('usd'), seen=m.get('new'), src='live')
        if chain in EVM and r.get('created'):
            try:
                bf = evm_backfill(chain, ca, r['created'], [w.lower() for w in watch.get(chain, {})], st)
            except Exception as e:
                bf = {}
                ERRORS.append(f'backfill {chain}: {type(e).__name__}')
            for w, v in (bf.get('first') or {}).items():
                ts = (bf.get('ts') or {}).get(w)
                if ts and (w not in firsts or ts < firsts[w]['ts']):
                    firsts[w] = dict(wallet=w, ts=ts, usd=None, seen=None, src='chain', tx=v[1], raw=v[2])
            r['backfill_done'] = bool(bf.get('done'))
        elif chain == 'Solana' and r.get('pool') and r.get('created') and NOW - r['created'] < 6 * 3600 \
                and NOW - (r.get('trades_at') or 0) > 1200 and left() > 50:
            r['trades_at'] = NOW                       # fresh Solana launch: read its latest trades for our wallets
            t = GT.get(f"/networks/solana/pools/{r['pool']}/trades")
            for tr in (t or {}).get('data', []):
                a = tr.get('attributes') or {}
                w = a.get('tx_from_address') or ''
                if a.get('kind') == 'buy' and w in watch.get('Solana', {}):
                    try:
                        ts = int(dt.datetime.fromisoformat(a['block_timestamp'].replace('Z', '+00:00')).timestamp())
                    except Exception:
                        continue
                    if w not in firsts or ts < firsts[w]['ts']:
                        firsts[w] = dict(wallet=w, ts=ts, usd=_f(a.get('volume_in_usd')), seen=None, src='trades')
        fl = sorted(firsts.values(), key=lambda x: x['ts'])
        for f in fl:
            f['groups'] = watch.get(chain, {}).get(f['wallet'] if chain == 'Solana' else f['wallet'].lower(), [])
        r['first_buys'] = fl[:8]
        r['n_wallets'] = len(fl)
        gs = []
        for f in fl:
            for g in f['groups']:
                if g not in gs:
                    gs.append(g)
        r['groups'] = gs[:12]
        r['watched'] = [f['wallet'] for f in fl]
        r['checked'] = NOW
        if fl and left() > 40:
            try:
                check_entry(r, fl, st, tokens)
            except Exception as e:
                ERRORS.append(f'entry {chain}: {type(e).__name__}')
        r['verdict'] = verdict(r)
    st['bf'] = {k: v for k, v in st.get('bf', {}).items() if k in disc}
    drows = [r for r in disc.values() if r.get('seen', 0) >= cut]
    for r in drows:
        sf = (st.get('safe') or {}).get(f"{r['chain']}|{r['ca']}")
        if r.get('first_buys') and not sf:
            sf = safety(r['chain'], r['ca'], st, budget=20)   # tokens our teams bought get checked first
        ok, why = tradable(dict(liq=r.get('liq'), mc=r.get('mc')), sf, strict=False, key=f"{r['chain']}|{r['ca']}")
        r.update(tradable=ok, risk=why)
    drows.sort(key=lambda r: (-(r.get('created') or 0)))
    st['safe'] = {k: v for k, v in (st.get('safe') or {}).items() if NOW - v.get('at', 0) < 3 * 86400}

    # trim token cache
    st['tokens'] = {k: v for k, v in tokens.items() if NOW - v.get('at', 0) < 3 * 86400}

    runs = load(os.path.join(OUT, 'meta.json'), {}).get('runs', [])
    runs.insert(0, dict(at=NOW, secs=int(time.time() - T0), new_moves=sum(1 for m in moves if m.get('new') == NOW),
                        gt_calls=GT.calls, errors=ERRORS[:8]))
    meta = dict(updated=NOW, updated_utc=utc(NOW), runs=runs[:48],
                chains={c: dict(({k: v for k, v in st.get('bs', {}).get(c, {}).items() if k != 'last'}
                                 if c in BLOCKSCOUT else st.get('evm', {}).get(c, {})),
                                wallets=len(watch.get(c, {}))) for c in EVM},
                solana=dict({k: v for k, v in st.get('sol', {}).items() if k not in ('last', 'seen')},
                            wallets=len(watch.get('Solana', {}))))
    save(os.path.join(OUT, 'moves.json'), dict(updated=NOW, rows=moves))
    save(os.path.join(OUT, 'groups.json'), dict(updated=NOW, rows=out_groups))
    save(os.path.join(OUT, 'alerts.json'), dict(updated=NOW, rows=alerts[:500]))
    save(os.path.join(OUT, 'hot.json'), dict(updated=NOW, rows=hrows[:800]))
    save(os.path.join(OUT, 'signals.json'), dict(updated=NOW, rows=srows[:400]))
    try:
        kol_followups(kmoves, tokens)
        krows = kol_rows(groups, kmoves, tokens, st.get('safe') or {})
    except Exception as e:
        ERRORS.append(f'KOLs: {type(e).__name__} {str(e)[:80]}')
        krows = []
    save(os.path.join(OUT, 'kol_moves.json'), dict(updated=NOW, rows=kmoves))
    save(os.path.join(OUT, 'kols.json'), dict(updated=NOW, rows=krows))
    save(os.path.join(OUT, 'discovery.json'), dict(updated=NOW, rows=drows[:400]))
    save(os.path.join(OUT, 'meta.json'), meta)
    save(os.path.join(DATA, 'state.json'), st)
    print(f'done in {int(time.time() - T0)}s: {meta["runs"][0]["new_moves"]} new moves, '
          f'{len(alerts)} alerts, {len(drows)} new tokens, {GT.calls} GeckoTerminal calls')
    for e in ERRORS:
        print('  !', e)


if __name__ == '__main__':
    main()
