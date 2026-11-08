#!/usr/bin/env python3
"""Insider Live scanner - one pass over all watched team wallets on five chains.

Runs on a timer (GitHub Actions). Standard library only.
  * EVM chains (BSC, Base, Ethereum, Robinhood): ERC-20 Transfer logs to/from watched wallets via public RPC.
  * Solana: new signatures per watched wallet, token balance changes per transaction via public RPC.
  * GeckoTerminal (free, no key): token names, prices, market caps, new/trending pools for discovery.
Writes the dashboard data to docs/data/*.json and its own cursor to data/state.json.
Everything here is read-only public chain data. Nothing trades.
"""
import json, os, sys, time, html, zlib, urllib.request, urllib.error, datetime as dt

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
GT_NET = {'Solana': 'solana', 'BSC': 'bsc', 'Base': 'base', 'Ethereum': 'eth', 'Robinhood': 'robinhood'}
# quote / base assets: a transfer of these is money moving, not a token pick
BASE_SYMBOLS = {'WETH', 'ETH', 'WBNB', 'BNB', 'USDC', 'USDT', 'DAI', 'BUSD', 'FDUSD', 'USD1', 'USDE', 'TUSD',
                'WSOL', 'SOL', 'WBTC', 'CBBTC', 'BTCB', 'USDC.E', 'USDBC', 'PYUSD', 'STETH', 'WSTETH', 'CBETH'}
WSOL = 'So11111111111111111111111111111111111111112'
STABLES = {'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'Es9vMFrzaCERmJfrF8KAQoQGyFw53xcjjZzdKgcqfTsQ'}
SOL_BASE_MINTS = {WSOL} | STABLES
SCAN_RESERVE = 110      # seconds kept back after the chain scans for prices, alerts, signals and discovery


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
                try:
                    row['created'] = int(dt.datetime.fromisoformat(pa['pool_created_at'].replace('Z', '+00:00')).timestamp())
                except Exception:
                    pass
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
def scan_solana(wallets, st, newmoves, priority=(), deadline=None):
    """Proven wallets first, then a rotating slice of the rest. Each trade is valued from the money that
    actually left or entered the wallet (native SOL + WSOL, or USDC/USDT), and transfers between two watched
    wallets are marked internal so a funder topping up its crew is never read as buying."""
    if not wallets:
        return
    deadline = deadline or (time.time() + left())
    rpc = RPC('Solana', SOL_RPCS, gap=0.2)
    cur = st.setdefault('sol', {})
    last = cur.setdefault('last', {})
    wset = set(wallets)
    pri = [w for w in sorted(set(priority)) if w in wset]
    rest = sorted(wset - set(pri))
    pos = cur.get('pos', 0) % max(1, len(rest))
    order = pri + rest[pos:] + rest[:pos]        # proven wallets first, every pass
    done = fails = rest_done = 0
    for w in order:
        if left() < 90 or time.time() > deadline:
            break
        try:
            if last.get(w):
                sigs, before = [], None          # page back to the bookmark so busy wallets lose nothing
                while True:
                    p = {'limit': 100, 'until': last[w]}
                    if before:
                        p['before'] = before
                    page = rpc.call('getSignaturesForAddress', [w, p]) or []
                    sigs += page
                    if len(page) < 100 or len(sigs) >= 300:
                        break
                    before = page[-1]['signature']
            else:
                sigs = rpc.call('getSignaturesForAddress', [w, {'limit': 3}]) or []   # first sight: latest few
        except Exception as e:
            msg = str(e)
            if '-32020' in msg:
                last.pop(w, None)        # bookmark too old for this node: restart this wallet from its latest
                continue
            fails += 1
            if fails == 1:
                ERRORS.append(f'Solana sigs: {msg[:80]}')
            if fails >= 6:
                break
            time.sleep(3)
            continue
        done += 1
        if w not in pri:
            rest_done += 1
        for s in reversed(sigs):                 # oldest first; the bookmark only moves past what we read
            if left() < 60 or time.time() > deadline + 15:
                break
            if s.get('err'):
                last[w] = s['signature']
                continue
            try:
                tx = rpc.call('getTransaction', [s['signature'], {'encoding': 'jsonParsed',
                                                                   'maxSupportedTransactionVersion': 0}])
            except Exception:
                break                            # retry this one next pass
            if not tx or not tx.get('meta'):
                break                            # node has not got it yet; retry next pass
            last[w] = s['signature']
            m = tx['meta']
            ts = tx.get('blockTime') or NOW
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
    cur['pos'] = (pos + rest_done) % max(1, len(rest))
    cur['covered'] = done
    cur['of'] = len(order)
    cur['at'] = NOW


# ---------------------------------------------------------------- discovery
def discover(chain, watch_set, disc):
    net = GT_NET[chain]
    pools = []
    for path in (f'/networks/{net}/trending_pools?duration=1h&page=1', f'/networks/{net}/new_pools?page=1'):
        if left() < 40:
            return
        r = GT.get(path)
        pools += (r or {}).get('data', [])
    for p in pools:
        a = p.get('attributes', {})
        created = a.get('pool_created_at')
        try:
            cts = int(dt.datetime.fromisoformat(created.replace('Z', '+00:00')).timestamp())
        except Exception:
            continue
        mc = _f(a.get('market_cap_usd')) or _f(a.get('fdv_usd')) or 0
        if NOW - cts > 72 * 3600 or mc < 500_000:
            continue
        sym0 = (a.get('name') or '?').split(' / ')[0].strip().upper()
        if sym0 in BASE_SYMBOLS or 'USD' in sym0 or mc > 2_000_000_000:
            continue    # stables, majors and new pools of old big tokens
        base = (p.get('relationships', {}).get('base_token', {}).get('data', {}) or {}).get('id', '')
        ca = base.split('_', 1)[-1]
        if chain != 'Solana':
            ca = ca.lower()
        key = f'{chain}|{ca}'
        row = disc.get(key) or dict(chain=chain, ca=ca, first_seen=NOW, watched=[])
        row.update(sym=html.unescape((a.get('name') or '?').split(' / ')[0])[:24], pool=a.get('address'), created=cts,
                   mc=mc, ath=max(mc, row.get('ath') or 0), liq=_f(a.get('reserve_in_usd')),
                   vol24=_f((a.get('volume_usd') or {}).get('h24')), seen=NOW)
        disc[key] = row
    # who traded these? check pool trades of the freshest few for watched wallets
    fresh = sorted((r for r in disc.values() if r['chain'] == chain and NOW - r['seen'] < 900),
                   key=lambda r: -(r.get('mc') or 0))[:4]
    for r in fresh:
        if left() < 40 or not r.get('pool'):
            break
        t = GT.get(f'/networks/{net}/pools/{r["pool"]}/trades')
        hits = set(r.get('watched') or [])
        for tr in (t or {}).get('data', []):
            fa = (tr.get('attributes') or {}).get('tx_from_address') or ''
            fa = fa if chain == 'Solana' else fa.lower()
            if fa in watch_set:
                hits.add(fa)
        r['watched'] = sorted(hits)


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


# ---------------------------------------------------------------- main
def main():
    groups, watch = load_seed()                              # teams + wallets from the research
    st = load(os.path.join(DATA, 'state.json'), {})
    tokens = st.setdefault('tokens', {})
    moves = load(os.path.join(OUT, 'moves.json'), {}).get('rows', [])
    alerts = load(os.path.join(OUT, 'alerts.json'), {}).get('rows', [])
    disc = {f"{r['chain']}|{r['ca']}": r for r in load(os.path.join(OUT, 'discovery.json'), {}).get('rows', [])}

    newmoves = []
    # proven = graded Elite/Strong/Tested or early on a lasting token; these drive buy signals
    good_ids = {g['id'] for g in groups if g['grade'] in ('Elite', 'Strong', 'Tested') or g.get('legit')}
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

    have = {(m['chain'], m['tx'], m['wallet'], m['ca'], m['side']) for m in moves}
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
        moves.append(dict(chain=m['chain'], wallet=m['wallet'], groups=gids, ts=m['ts'], side=m['side'],
                          ca=m['ca'], sym=sym, usd=round(usd, 2) if usd else None,
                          mc=info.get('mc'), tx=m['tx'], new=NOW))

    st['pending'] = pending[-500:]
    cut = NOW - KEEP_DAYS * 86400
    moves = [m for m in moves if m['ts'] >= cut]
    moves.sort(key=lambda m: -m['ts'])
    moves = moves[:6000]

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
        hrows.append(dict(chain=h['chain'], ca=h['ca'], sym=info.get('sym') or h['sym'], first_buy=h['first_buy'],
                          last=h['last'], last_buy=h.get('last_buy'), mc_first=h['mc_first'], mc_now=mc_now,
                          change=round(chg, 4) if chg is not None else None, liq=info.get('liq'),
                          buy_usd=round(h['buy_usd'], 2), sell_usd=round(h['sell_usd'], 2), buys=h['buys'],
                          sells=h['sells'], n_wallets=len(h['wallets']), n_groups=len(h['groups']),
                          groups=sorted(h['groups'])[:8], buys_6h=h['buys_6h'], usd_6h=round(h['usd_6h'], 2),
                          groups_6h=sorted(h['groups_6h'])[:12], **token_extra(info)))
    hrows.sort(key=lambda r: -(r['last_buy'] or 0))

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
        strong = (c['buy'] >= 300 and len(c['wallets']) >= 2) or c['big'] >= 500
        if not strong or c['sell'] > 0.5 * c['buy']:
            continue
        r = sigs.get(k)
        if not r:
            r = dict(chain=c['chain'], ca=c['ca'], first_signal=NOW, mc_at_signal=info.get('mc'),
                     age_at_signal_h=round(age_h, 2), first_buy=c['first'], notified=False)
            sigs[k] = r
        r.update(sym=info.get('sym') or c['sym'], buy_usd=round(c['buy'], 2), sell_usd=round(c['sell'], 2),
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
    srows = sorted((r for r in sigs.values() if NOW - r['first_signal'] < 7 * 86400), key=lambda r: -r['first_signal'])
    notify(srows, {g['id']: g for g in groups})

    # discovery: new tokens past $500K, and whether watched wallets are in
    watch_set = {}
    for chain, ws in watch.items():
        watch_set[chain] = set(ws) if chain == 'Solana' else {w.lower() for w in ws}
    for chain in ['Solana', 'BSC', 'Base', 'Robinhood', 'Ethereum']:
        if left() < 45:
            break
        try:
            discover(chain, watch_set.get(chain, set()), disc)
        except Exception as e:
            ERRORS.append(f'discovery {chain}: {type(e).__name__}')
    for r in disc.values():
        early = {m['wallet'] for m in moves if m['chain'] == r['chain'] and m['ca'] == r['ca'] and m['side'] == 'buy'}
        r['watched'] = sorted(set(r.get('watched') or []) | early)
        gs = set()
        for w in r['watched']:
            gs.update(watch.get(r['chain'], {}).get(w, []))
        r['groups'] = sorted(gs)
    drows = [r for r in disc.values() if r.get('seen', 0) >= cut]
    drows.sort(key=lambda r: (-len(r.get('watched') or []), -(r.get('mc') or 0)))

    # trim token cache
    st['tokens'] = {k: v for k, v in tokens.items() if NOW - v.get('at', 0) < 3 * 86400}

    runs = load(os.path.join(OUT, 'meta.json'), {}).get('runs', [])
    runs.insert(0, dict(at=NOW, secs=int(time.time() - T0), new_moves=sum(1 for m in moves if m.get('new') == NOW),
                        gt_calls=GT.calls, errors=ERRORS[:8]))
    meta = dict(updated=NOW, updated_utc=utc(NOW), runs=runs[:48],
                chains={c: dict(({k: v for k, v in st.get('bs', {}).get(c, {}).items() if k != 'last'}
                                 if c in BLOCKSCOUT else st.get('evm', {}).get(c, {})),
                                wallets=len(watch.get(c, {}))) for c in EVM},
                solana=dict({k: v for k, v in st.get('sol', {}).items() if k != 'last'},
                            wallets=len(watch.get('Solana', {}))))
    save(os.path.join(OUT, 'moves.json'), dict(updated=NOW, rows=moves))
    save(os.path.join(OUT, 'groups.json'), dict(updated=NOW, rows=out_groups))
    save(os.path.join(OUT, 'alerts.json'), dict(updated=NOW, rows=alerts[:500]))
    save(os.path.join(OUT, 'hot.json'), dict(updated=NOW, rows=hrows[:800]))
    save(os.path.join(OUT, 'signals.json'), dict(updated=NOW, rows=srows[:400]))
    save(os.path.join(OUT, 'discovery.json'), dict(updated=NOW, rows=drows[:400]))
    save(os.path.join(OUT, 'meta.json'), meta)
    save(os.path.join(DATA, 'state.json'), st)
    print(f'done in {int(time.time() - T0)}s: {meta["runs"][0]["new_moves"]} new moves, '
          f'{len(alerts)} alerts, {len(drows)} new tokens, {GT.calls} GeckoTerminal calls')
    for e in ERRORS:
        print('  !', e)


if __name__ == '__main__':
    main()
