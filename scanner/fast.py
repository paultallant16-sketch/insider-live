#!/usr/bin/env python3
"""Fast watcher: polls only the proven team wallets every ~20 seconds and pushes a phone alert as soon as one buys.

The full scanner (scan.py) does everything else on a slower loop: every wallet, prices, the dashboard and the
stricter BUY SIGNAL alerts. This one exists for speed only: it keeps no history and commits nothing.
"""
import json
import os
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scan as S  # noqa: E402

S.RUN_BUDGET = 1e12                      # scan.left() is always large here; this loop has its own end time
END = time.time() + float(os.environ.get('FAST_SECONDS', str(5 * 3600 + 1500)))
TOPIC = os.environ.get('NTFY_TOPIC', '').strip()
MIN_USD = float(os.environ.get('FAST_MIN_USD', '150'))
CYCLE = float(os.environ.get('FAST_CYCLE', '20'))
MAX_AGE = 7 * 86400                      # pre-pump teams buy tokens up to days old; older than a week is not "new"
PER_HOUR = 20                            # never more than this many fast alerts an hour
EVM_FAST = ('Robinhood', 'BSC', 'Base')  # Ethereum (12 s blocks, no cheap wallet-only logs) stays on the slow loop
SOL = 'So11111111111111111111111111111111111111112'


def log(*a):
    print(time.strftime('%H:%M:%S'), *a, flush=True)


def load_targets():
    """Proven, unflagged team wallets per chain, plus each team's live Follow score from the dashboard data."""
    groups, watch = S.load_seed()
    S.load_flags(groups, watch)
    good = {g['id'] for g in groups if g.get('kind') not in ('kol', 'ring') and not g.get('flag')
            and (g['grade'] in ('Elite', 'Strong', 'Tested') or g.get('legit'))}
    gmap = {g['id']: g for g in groups}
    try:
        live = {g['id']: g for g in S.load(os.path.join(S.OUT, 'groups.json'), {}).get('rows', [])}
    except Exception:
        live = {}
    for gid, g in gmap.items():
        if gid in live:
            g['follow'] = live[gid].get('follow')
            if live[gid].get('flag'):                   # flagged live (dumps tokens it never bought)
                good.discard(gid)
    tgt = {}
    for chain, ws in watch.items():
        m = {w: [x for x in gids if x in good] for w, gids in ws.items()}
        tgt[chain] = {w: v for w, v in m.items() if v}
    return gmap, tgt


class Watch:
    def __init__(self):
        self.tokens = {}           # scan.token_info cache
        self.safe = {}             # holder / honeypot checks, cached 6h like the slow loop
        self.sol_seen = {}         # wallet -> signatures already handled
        self.evm_last = {}         # chain -> last block looked at
        self.sent = {}             # 'chain|ca' -> time of last alert
        self.sent_times = []
        self.sol_px = None
        self.sol_px_at = 0
        self.reload()

    def reload(self):
        try:
            subprocess.run(['git', 'pull', '-q', '--ff-only'], timeout=60, capture_output=True)
        except Exception:
            pass
        self.gmap, self.tgt = load_targets()
        self.loaded = time.time()
        log('targets:', {c: len(v) for c, v in self.tgt.items()})

    # ------------------------------------------------------------ Solana
    def solana(self, out):
        ws = list(self.tgt.get('Solana', {}))
        if not ws:
            return
        wset = set(ws)
        lists = {}

        def sigs(rpc, w):
            lists[w] = rpc.call('getSignaturesForAddress', [w, {'limit': 6}], tries=2) or []
        S._sol_pool(ws, sigs, time.time() + 25)
        jobs = []
        for w, L in lists.items():
            have = self.sol_seen.get(w)
            if have is None:                            # first look: remember, do not alert on history
                self.sol_seen[w] = [s['signature'] for s in L]
                continue
            new = [s for s in L if s['signature'] not in have and not s.get('err')
                   and (s.get('blockTime') or S.NOW) >= S.NOW - 900]
            self.sol_seen[w] = ([s['signature'] for s in L] + have)[:40]
            jobs += [(w, s) for s in new]

        def tx(rpc, job):
            w, s = job
            t = rpc.call('getTransaction', [s['signature'], {'encoding': 'jsonParsed',
                                                             'maxSupportedTransactionVersion': 0}], tries=2)
            if t and t.get('meta'):
                S._sol_parse(w, s, t, wset, out)
        S._sol_pool(jobs, tx, time.time() + 25)

    # ------------------------------------------------------------ EVM
    def evm(self, chain, out):
        ws = sorted(self.tgt.get(chain, {}))
        if not ws:
            return
        cfg = S.EVM[chain]
        urls = cfg.get('wide') or cfg['rpcs']
        step = cfg.get('wide_range', 2000)
        size = 16 if cfg.get('wide') else 60
        rpc = S.RPC(chain + ' fast', urls, gap=0.2)
        head = int(rpc.call('eth_blockNumber', []), 16)
        last = self.evm_last.get(chain)
        if last is None or head - last > int(900 / cfg['block_s']):
            self.evm_last[chain] = head                 # first look, or fell >15 min behind: start from now
            return
        tops = [S.topic(w) for w in ws]
        logs, b = [], last + 1
        t_end = time.time() + 20
        while b <= head and time.time() < t_end:
            e = min(head, b + step - 1)
            for i in range(0, len(tops), size):
                logs += rpc.call('eth_getLogs', [{'fromBlock': hex(b), 'toBlock': hex(e),
                                                  'topics': [S.TRANSFER, None, tops[i:i + size]]}]) or []
            self.evm_last[chain] = e
            b = e + 1
        S._evm_moves(chain, cfg, rpc, logs, set(ws), out)

    # ------------------------------------------------------------ valuing + alerting
    def sol_price(self):
        if time.time() - self.sol_px_at > 600:
            S.token_info('Solana', [SOL], self.tokens)
            self.sol_px = (self.tokens.get(f'Solana|{SOL}') or {}).get('price') or self.sol_px
            self.sol_px_at = time.time()
        return self.sol_px

    def usd(self, m):
        if m['chain'] == 'Solana':
            if m.get('stable'):
                return abs(m['stable'])
            px = self.sol_price()
            return abs(m.get('sol') or 0) * px if px and abs(m.get('sol') or 0) > 0.01 else None
        try:
            return S.usd_paid(m['chain'], m['tx'], m['wallet'], m['ca'], self.tokens)
        except Exception:
            return None

    def handle(self, moves):
        buys = {}
        for m in moves:
            if m['side'] != 'buy' or m['ca'] in S.SOL_BASE_MINTS:
                continue
            gids = self.tgt.get(m['chain'], {}).get(m['wallet'] if m['chain'] == 'Solana' else m['wallet'].lower())
            if not gids:
                continue
            buys.setdefault((m['chain'], m['ca']), []).append((m, gids))
        for (chain, ca), L in buys.items():
            k = f'{chain}|{ca}'
            if time.time() - self.sent.get(k, 0) < 3 * 3600:
                continue
            S.token_info(chain, [ca], self.tokens)
            info = self.tokens.get(k) or {}
            sym = (info.get('sym') or '?').upper()
            if not info or sym in S.BASE_SYMBOLS or 'USD' in sym:
                continue
            created = info.get('created')
            if created and S.NOW - created > MAX_AGE:
                continue
            total, wallets, teams = 0.0, set(), []
            for m, gids in L:
                v = self.usd(m)
                if v:
                    total += v
                wallets.add(m['wallet'])
                teams += [g for g in gids if g not in teams]
            if total < MIN_USD or (total < 2 * MIN_USD and len(wallets) < 2):
                continue                                 # small single-wallet buys are noise
            sf = S.safety(chain, ca, {'safe': self.safe}, budget=10 ** 9)
            ok, why = S.tradable(info, sf)
            if sf and sf.get('honeypot') is True:
                continue                                 # cannot sell: never alert
            first_ts = min(m['ts'] for m, _ in L)
            self.alert(chain, ca, info, total, wallets, teams, first_ts, created, ok, why)
            self.sent[k] = time.time()

    def alert(self, chain, ca, info, total, wallets, teams, first_ts, created, ok, why):
        now = time.time()
        self.sent_times = [t for t in self.sent_times if now - t < 3600]
        if len(self.sent_times) >= PER_HOUR:
            log('hourly cap reached, skipping', info.get('sym'))
            return
        names = []
        for g in teams[:3]:
            gg = self.gmap.get(g, {})
            f = gg.get('follow') or {}
            fs = f" (follow {f['med']:+.0%}/{f['n']})" if f.get('n') else ''
            names.append(f"{gg.get('nick') or gg.get('name') or g}{' *' if gg.get('legit') else ''}{fs}")
        mc, liq = info.get('mc') or 0, info.get('liq') or 0
        age = (S.NOW - created) / 3600 if created else None
        age_txt = '' if age is None else (f'{int(age * 60)}m old' if age < 1 else f'{age:.0f}h old')
        late = int(now - first_ts)
        risk = '' if ok else ' ⚠ ' + '; '.join(why[:2])
        body = (f"{', '.join(names)} bought ${total:,.0f} ({len(wallets)} wallet{'s' if len(wallets) > 1 else ''}) "
                f"of {info.get('sym')} on {chain}. MC ${mc:,.0f}, liq ${liq:,.0f}"
                f"{', ' + age_txt if age_txt else ''}. Seen {late}s after the buy.{risk}")
        title = f"⚡ TEAM BUY {info.get('sym')} ({chain})"
        log('ALERT', title, body)
        if not TOPIC:
            return
        payload = dict(topic=TOPIC, title=title, message=body, tags=['zap'], priority=4 if ok else 3,
                       click=f"https://gmgn.ai/{S.GMGN_NET.get(chain, chain)}/token/{ca}")
        try:
            req = urllib.request.Request('https://ntfy.sh/', data=json.dumps(payload).encode('utf-8'),
                                         headers={'Content-Type': 'application/json', 'User-Agent': S.UA})
            urllib.request.urlopen(req, timeout=10).read()
            self.sent_times.append(now)
        except Exception as e:
            log('ntfy failed', type(e).__name__)


def main():
    w = Watch()
    n = 0
    while time.time() < END:
        t0 = time.time()
        S.NOW = int(t0)
        S.ERRORS.clear()
        moves = []
        try:
            w.solana(moves)
        except Exception as e:
            log('solana', type(e).__name__, str(e)[:120])
        for chain in EVM_FAST:
            try:
                w.evm(chain, moves)
            except Exception as e:
                log(chain, type(e).__name__, str(e)[:120])
        try:
            w.handle(moves)
        except Exception as e:
            log('handle', type(e).__name__, str(e)[:120])
        n += 1
        if n % 30 == 0:
            log(f'cycle {n}: {len(moves)} moves, {time.time() - t0:.0f}s', S.ERRORS[:3])
        if time.time() - w.loaded > 1800:
            w.reload()                                   # new teams / flags / follow scores from the slow loop
        time.sleep(max(2.0, CYCLE - (time.time() - t0)))


if __name__ == '__main__':
    main()
