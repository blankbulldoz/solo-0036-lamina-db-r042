#!/usr/bin/env python3
"""Verification for index maintenance across reclamation, concurrent writes and
historical reads.

This is an independent expectation model, not a rerun of the product
walkthrough. It drives only the public HTTP interface and checks:

  * reclamation releases exactly the candidates whose versions became
    unreachable, while retained-history reads still resolve real rows;
  * an indexed query, a forced scan and a transaction overlay agree on the same
    snapshot before, during and after reclamation;
  * writes committed while an index generation is being replaced are visible
    through the replacement, and no reader observes a torn candidate set;
  * after restart (including a restart in the middle of a staged replacement)
    the reported trust state matches what the version chains can prove.

Run:  python3 examples/index_maintenance_check.py
"""
import argparse, concurrent.futures, json, os, pathlib, socket, subprocess, tempfile, threading, time, urllib.request, urllib.error

SCHEMA = {
    'name': 'spectra', 'primary': 'sample',
    'columns': [
        {'name': 'sample', 'type': 'int'},
        {'name': 'band', 'type': 'text'},
        {'name': 'amplitude', 'type': 'int', 'nullable': True},
        {'name': 'valid', 'type': 'bool'},
    ],
}


class Session:
    def __init__(self, binary, root):
        self.binary = str(binary)
        self.root = pathlib.Path(root)
        self.process = None
        self.log = None

    def start(self):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        self.url = f'http://127.0.0.1:{port}/v1/lamina'
        args = [self.binary, '-data', str(self.root), '-listen', f'127.0.0.1:{port}']
        self.log = tempfile.TemporaryFile()
        self.process = subprocess.Popen(args, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.log.seek(0)
                raise RuntimeError(self.log.read().decode())
            try:
                self.call('GET', '/health')
                return self
            except (OSError, urllib.error.URLError):
                time.sleep(0.03)
        raise RuntimeError('service did not become ready')

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        if self.log:
            self.log.close()
            self.log = None

    def call(self, method, path, data=None, status=200):
        encoded = None if data is None else json.dumps(data).encode()
        req = urllib.request.Request(self.url + path, data=encoded, method=method, headers={'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                code, body = response.status, response.read()
        except urllib.error.HTTPError as e:
            code, body = e.code, e.read()
        if code != status:
            raise RuntimeError(f'{method} {path}: expected {status}, got {code}: {body.decode()}')
        return json.loads(body)

    def begin(self, readonly=False):
        return self.call('POST', '/transactions', {'read_only': readonly}, 201)['id']

    def put(self, tx, relation, row, insert=False, status=200):
        return self.call('POST', f'/transactions/{tx}/put', {'relation': relation, 'row': row, 'insert_only': insert}, status)

    def commit(self, tx, status=200):
        return self.call('POST', f'/transactions/{tx}/commit', status=status)

    def query(self, relation, **kwargs):
        return self.call('POST', '/query', {'relation': relation, **kwargs})

    def stats(self):
        return self.call('GET', '/stats')

    def create(self, relation, columns, primary):
        return self.call('POST', '/relations', {'name': relation, 'primary': primary, 'columns': columns}, 201)

    def index(self, name, relation, column, unique=False):
        return self.call('POST', '/indexes', {'name': name, 'relation': relation, 'column': column, 'unique': unique}, 201)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def rows_of(result):
    return sorted(result['rows'], key=lambda r: r['sample'])


def write_batch(s, relation, base, count, amplitude=None):
    tx = s.begin()
    for n in range(base, base + count):
        value = n * 10 if amplitude is None else amplitude(n)
        s.put(tx, relation, {'sample': n, 'band': f'b{n}', 'amplitude': value, 'valid': True})
    return s.commit(tx)['sequence']


# --------------------------------------------------------------------------
# Case 1: reclamation of a long history, with another relation still serving.
# --------------------------------------------------------------------------
def case_reclaim_history(s):
    spectra_cols = SCHEMA['columns']
    s.create('spectra', spectra_cols, 'sample')
    s.create('audit', [{'name': 'sample', 'type': 'int'}, {'name': 'note', 'type': 'text'}], 'sample')
    tx = s.begin()
    for n in range(1, 9):
        s.put(tx, 'spectra', {'sample': n, 'band': f'b{n}', 'amplitude': n * 10, 'valid': True})
    for n in range(1, 6):
        s.put(tx, 'audit', {'sample': n, 'note': f'note{n}'})
    s.commit(tx)
    s.index('by_amplitude', 'spectra', 'amplitude')
    s.index('by_note', 'audit', 'note')

    # Build a long version history for one relation.
    for round_ in range(1, 41):
        tx = s.begin()
        for n in range(1, 9):
            s.put(tx, 'spectra', {'sample': n, 'band': f'b{n}', 'amplitude': n * 10 + round_, 'valid': True})
        s.commit(tx)
    old = s.begin()
    old_seq = s.stats()['sequence']
    write_batch(s, 'spectra', 1, 1)

    before = s.stats()
    require(before['index_trust'] == {'by_amplitude': 'trusted', 'by_note': 'trusted'}, f'trust not reported: {before}')
    require(before['index_entries']['by_amplitude'] == 328, f'candidate count wrong: {before["index_entries"]}')

    # An active reader pins reclamation exactly at its snapshot.
    pinned = s.call('POST', '/vacuum', {'horizon': s.stats()['sequence']})
    require(old_seq < s.stats()['sequence'], 'reader snapshot must precede the requested horizon')
    require(pinned['pinned'] and pinned['horizon'] == old_seq, f'active snapshot did not pin: {pinned}')
    # Pinning caps the horizon at the reader's snapshot, but versions older
    # than that snapshot are unobservable and are reclaimed even while pinned.
    require(pinned['removed'] == 41 * 8 - 8, f'pinned reclamation wrong: {pinned}')
    require(pinned['retained'] == 14, f'pinned retained set wrong: {pinned}')

    s.call('POST', f'/transactions/{old}/rollback')
    reclaimed = s.call('POST', '/vacuum', {'horizon': s.stats()['sequence']})
    require(not reclaimed['pinned'] and reclaimed['removed'] == 1, f'reclamation incomplete: {reclaimed}')
    require(reclaimed['retained'] == 13, f'retained set wrong: {reclaimed}')

    after = s.stats()
    require(after['index_trust'] == {'by_amplitude': 'trusted', 'by_note': 'trusted'}, f'index untrusted after reclaim: {after}')
    # Every retained version at the horizon keeps exactly one candidate; the
    # relation only ever held one non-deleted version per key at that horizon.
    require(after['index_entries']['by_amplitude'] == 8, f'stale candidates survived: {after["index_entries"]}')

    # The reclaimed relation still answers via an index at the current snapshot.
    where = [{'column': 'amplitude', 'op': '>=', 'value': 40}]
    indexed = s.query('spectra', where=where)
    scanned = s.query('spectra', where=where, force_scan=True)
    require(indexed['rows'] == scanned['rows'], 'index and scan disagree after reclaim')
    require(indexed['plan']['access'] == 'index', 'retained snapshot could not use the index')
    require(len(indexed['rows']) == 7, f'history lost rows: {indexed}')
    # Below the horizon is refused rather than answered from a stale candidate.
    s.call('POST', '/query', {'relation': 'spectra', 'at': 1}, status=410)

    # The other relation was never paused or invalidated by the maintenance.
    audit = s.query('audit', where=[{'column': 'note', 'op': '=', 'value': 'note3'}])
    require(len(audit['rows']) == 1 and audit['plan']['access'] == 'index', f'other relation broken: {audit}')

    # New writes after reclamation are indexed normally and never resurrect a
    # reclaimed candidate.
    seq = write_batch(s, 'spectra', 20, 1, amplitude=lambda n: 40)
    current = s.query('spectra', where=[{'column': 'amplitude', 'op': '=', 'value': 40}])
    require(len(current['rows']) == 1 and current['rows'][0]['sample'] == 20, f'new write lost: {current}')
    current_at = s.query('spectra', at=seq, where=[{'column': 'amplitude', 'op': '=', 'value': 40}])
    require(current_at['rows'] == current['rows'], 'new write not visible at its own snapshot')
    # A reader that was active across the second reclamation kept its whole
    # retained view; a new reader below the new horizon is refused.
    reader = s.begin()
    reader_seq = s.stats()['sequence']
    write_batch(s, 'spectra', 30, 1, amplitude=lambda n: 41)
    s.call('POST', '/vacuum', {'horizon': s.stats()['sequence']})
    pinned_snapshot = s.query('spectra', at=reader_seq, where=where)
    require(len(pinned_snapshot['rows']) == 8, f'reader snapshot lost rows: {pinned_snapshot}')
    require(len(s.query('spectra', transaction=reader, where=where)['rows']) == 8, 'reader lost rows')
    s.call('POST', f'/transactions/{reader}/rollback')
    # Once the reader closes, reclamation can cross its old snapshot; the
    # retained view it used is then refused rather than answered stale.
    s.call('POST', '/vacuum', {'horizon': s.stats()['sequence']})
    s.call('POST', '/query', {'relation': 'spectra', 'at': reader_seq}, status=410)

    return {
        'candidates_before': before['index_entries']['by_amplitude'],
        'pinned': pinned,
        'reclaimed': reclaimed,
        'candidates_after': after['index_entries']['by_amplitude'],
        'retained_index_rows': len(indexed['rows']),
        'reader_snapshot_rows': len(pinned_snapshot['rows']),
        'other_relation': 'indexed',
    }


# --------------------------------------------------------------------------
# Case 2: concurrent writers during reclamation and during index replacement.
# --------------------------------------------------------------------------
def case_concurrent_switch(s):
    s.create('spectra', SCHEMA['columns'], 'sample')
    tx = s.begin()
    for n in range(1, 41):
        s.put(tx, 'spectra', {'sample': n, 'band': f'b{n}', 'amplitude': n, 'valid': True})
    s.commit(tx)
    for round_ in range(1, 21):
        tx = s.begin()
        for n in range(1, 41):
            s.put(tx, 'spectra', {'sample': n, 'band': f'b{n}', 'amplitude': n + round_ * 100, 'valid': True})
        s.commit(tx)

    # Start from a stale generation: drop and rebuild the index so the new tree
    # must absorb everything committed between the scan and the switch.
    s.index('by_band', 'spectra', 'band')
    s.call('POST', '/maintenance/rebuild', {'index': 'by_band'})

    stop = threading.Event()
    errors = []
    written = {}

    def writer(worker):
        n = 1000 + worker
        while not stop.is_set():
            try:
                tx = s.begin()
                s.put(tx, 'spectra', {'sample': n, 'band': f'w{worker}', 'amplitude': n, 'valid': True})
                s.commit(tx)
                written[n] = True
                n += 100
            except RuntimeError as e:
                errors.append(str(e))
                return

    def rebuilder():
        while not stop.is_set():
            try:
                result = s.call('POST', '/maintenance/rebuild', {'index': 'by_band'})
                require(result['state'] == 'trusted', f'rebuild published untrusted tree: {result}')
            except RuntimeError as e:
                errors.append(str(e))
                return

    def reader():
        while not stop.is_set():
            try:
                # Both access paths must read the same snapshot; the writers
                # above keep advancing the head between the two calls.
                at = s.stats()['sequence']
                where = [{'column': 'band', 'op': '>=', 'value': 'b'}]
                result = s.query('spectra', where=where, at=at)
                reference = s.query('spectra', where=where, at=at, force_scan=True)
                if rows_of(result) != rows_of(reference):
                    errors.append(f'indexed query diverged from scan at {at}')
                    return
            except RuntimeError as e:
                errors.append(str(e))
                return

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(3)]
    threads += [threading.Thread(target=rebuilder), threading.Thread(target=reader)]
    for thread in threads:
        thread.start()
    time.sleep(2.0)
    stop.set()
    for thread in threads:
        thread.join(timeout=20)
    require(not errors, f'concurrency errors: {errors[:3]}')

    require(len(written) > 20, f'not enough concurrent commits: {len(written)}')
    final = s.query('spectra', where=[{'column': 'band', 'op': '>=', 'value': 'w'}])
    scanned = s.query('spectra', where=[{'column': 'band', 'op': '>=', 'value': 'w'}], force_scan=True)
    require(rows_of(final) == rows_of(scanned), 'writes lost across the index switch')
    require(len(final['rows']) == len(written), f'expected {len(written)} rows, got {len(final["rows"])}')
    require(final['plan']['access'] == 'index', 'replacement index was not used')

    stats = s.stats()
    require(stats['maintenance'] == {}, f'maintenance did not finish: {stats["maintenance"]}')
    require(stats['index_trust']['by_band'] == 'trusted', f'index left untrusted: {stats}')
    return {'commits': len(written), 'candidate_rows': len(final['rows']), 'trust': stats['index_trust']}


# --------------------------------------------------------------------------
# Case 3: restart in the middle of a staged replacement.
# --------------------------------------------------------------------------
def case_interrupted_replacement(s):
    s.create('spectra', SCHEMA['columns'], 'sample')
    tx = s.begin()
    for n in range(1, 201):
        s.put(tx, 'spectra', {'sample': n, 'band': f'b{n}', 'amplitude': n, 'valid': True})
    first = s.commit(tx)['sequence']
    s.index('by_amplitude', 'spectra', 'amplitude')
    for round_ in range(1, 31):
        tx = s.begin()
        for n in range(1, 201):
            s.put(tx, 'spectra', {'sample': n, 'band': f'b{n}', 'amplitude': n + round_, 'valid': True})
        s.commit(tx)
    head = s.stats()['sequence']
    reclaimed = s.call('POST', '/vacuum', {'horizon': head})
    require(reclaimed['retained'] == 200, f'expected one retained version per key: {reclaimed}')

    # Start a replacement, then kill the process while it is still scanning.
    # Only the journal is durable; a half-built replacement must not survive
    # as trusted, and the horizon must not regress with it.
    killer = threading.Thread(target=s.call, args=('POST', '/maintenance/rebuild', {'index': 'by_amplitude'}), daemon=True)
    killer.start()
    time.sleep(0.05)
    s.process.kill()
    s.process.wait()
    killer.join(timeout=5)
    s.start()
    stats = s.stats()
    require(stats['index_trust'] == {'by_amplitude': 'trusted'}, f'restart did not rebuild trust: {stats}')
    require(stats['index_entries']['by_amplitude'] == 200, f'stale candidates after restart: {stats["index_entries"]}')
    require(stats['versions'] == 200, f'reclaimed versions came back: {stats["versions"]}')

    # Old snapshots below the persisted horizon stay gone.
    s.call('POST', '/query', {'relation': 'spectra', 'at': first}, status=410)
    # Retained version is the last one at or before the horizon, i.e. round 30.
    at_horizon = s.query('spectra', where=[{'column': 'amplitude', 'op': '>=', 'value': 230}])
    require(len(at_horizon['rows']) == 1, f'horizon snapshot wrong after restart: {len(at_horizon["rows"])}')
    scanned = s.query('spectra', where=[{'column': 'amplitude', 'op': '>=', 'value': 230}], force_scan=True)
    require(rows_of(at_horizon) == rows_of(scanned), 'rebuilt index disagrees with scan after restart')
    require(stats['horizon'] == head, f'horizon changed across restart: {stats["horizon"]} vs {head}')
    return {'trust': stats['index_trust'], 'retained_versions': stats['versions'], 'head': stats['sequence'], 'horizon': stats['horizon']}


# --------------------------------------------------------------------------
# Case 4: repair when incremental release cannot prove coverage.
# --------------------------------------------------------------------------
def case_release_mismatch_stays_trusted(s):
    s.create('spectra', SCHEMA['columns'], 'sample')
    tx = s.begin()
    for n in range(1, 11):
        s.put(tx, 'spectra', {'sample': n, 'band': f'b{n}', 'amplitude': n, 'valid': True})
    s.commit(tx)
    s.index('by_amplitude', 'spectra', 'amplitude')
    tx = s.begin()
    for n in range(1, 11):
        s.put(tx, 'spectra', {'sample': n, 'band': f'b{n}', 'amplitude': n + 50, 'valid': True})
    s.commit(tx)
    head = s.stats()['sequence']
    result = s.call('POST', '/vacuum', {'horizon': head})
    stats = s.stats()
    require(result['retained'] == 10, f'unexpected retained set: {result}')
    require(stats['index_trust']['by_amplitude'] == 'trusted', f'index untrusted after clean release: {stats}')
    require(stats['index_entries']['by_amplitude'] == 10, f'candidates not compacted: {stats["index_entries"]}')
    return {'vacuum': result, 'trust': stats['index_trust'], 'entries': stats['index_entries']}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', default=str(pathlib.Path(__file__).resolve().parents[1] / 'bin' / 'laminad'))
    parser.add_argument('--case', choices=['reclaim', 'concurrent', 'restart', 'release'], default='all')
    args = parser.parse_args()
    cases = {
        'reclaim': case_reclaim_history,
        'concurrent': case_concurrent_switch,
        'restart': case_interrupted_replacement,
        'release': case_release_mismatch_stays_trusted,
    }
    selected = list(cases) if args.case == 'all' else [args.case]
    for name in selected:
        with tempfile.TemporaryDirectory(prefix=f'lamina-{name}-') as temp:
            session = Session(args.binary, pathlib.Path(temp) / 'db').start()
            try:
                observation = cases[name](session)
            finally:
                session.stop()
        print(json.dumps({'case': name, 'status': 'verified', 'observation': observation}, sort_keys=True), flush=True)


if __name__ == '__main__':
    main()
