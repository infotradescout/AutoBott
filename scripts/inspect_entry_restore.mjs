/** Explicit source-only inspection; never loaded by the trading application. */
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
if (process.env.AUTOBOTT_INSPECT_ENTRY_RESTORE === 'true') {
  const env = process.env;
  delete env.AUTOBOTT_INSPECT_ENTRY_RESTORE;
  delete env.AUTOBOTT_VERIFY_TOKEN;
  delete env.NODE_OPTIONS;
  const safe = Object.fromEntries(Object.entries(env).filter(([k]) => ['PATH','HOME','SYSTEMROOT','TMPDIR','TEMP','TMP'].includes(k)));
  safe.GIT_CONFIG_NOSYSTEM = '1';
  safe.GIT_CONFIG_GLOBAL = '/dev/null';
  safe.GIT_TERMINAL_PROMPT = '0';
  const script = String.raw`
import difflib, hashlib, json, pathlib, subprocess, tempfile
BASE='5e0e8c3823d311fa82fed648844668c13cc54579'
DONOR='7b99cb5f9cc9ffd15d8acf626dec510133293042'
CURRENT='69aa105ac584c86b9f8a8d9531af9f885311c741'
SOURCE='https://github.com/infotradescout/AutoBott.git'
with tempfile.TemporaryDirectory(prefix='entry-restore-source-') as repository:
    def git(*args, check=True):
        r=subprocess.run(['git','-C',repository,'-c','credential.helper=','-c','http.extraHeader=','-c','core.hooksPath=/dev/null',*args],capture_output=True,timeout=90)
        if check and r.returncode:
            raise RuntimeError('source_git_failed:'+r.stderr.decode(errors='replace')[-600:])
        return r
    git('init','--bare')
    git('fetch','--depth=1','--no-tags',SOURCE,BASE,DONOR,CURRENT)
    assert git('rev-parse',CURRENT+'^{tree}').stdout.decode().strip()=='99129695a5746a773076a3d6dc85cefc7d498770'
    def data(ref,path):
        r=git('show',f'{ref}:{path}',check=False)
        return r.stdout if r.returncode==0 else None
    def blob(raw):
        return hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest() if raw is not None else None
    files=git('diff','--name-only',BASE,DONOR).stdout.decode().splitlines()
    rows=[]
    for path in files:
        destination='tests/dashboard_regression_cases.py' if path=='tests/test_dashboard_app.py' else path
        base, donor, current=data(BASE,path), data(DONOR,path), data(CURRENT,destination)
        row={'path':destination,'donor_path':path,'base':blob(base),'donor':blob(donor),'current':blob(current)}
        if donor is None:
            row['status']='donor_deleted_requires_review'
        elif donor==current:
            row['status']='already_present'
        elif current==base or current is None and base is None:
            row['status']='restore_exact'
        elif donor==base:
            row['status']='keep_current'
        elif current is None or base is None:
            row['status']='identity_conflict'
        else:
            with tempfile.TemporaryDirectory(prefix='entry-merge-inspect-') as d:
                ps=[pathlib.Path(d)/x for x in ('ours','base','theirs')]
                for p,raw in zip(ps,(current,base,donor)):p.write_bytes(raw)
                merged=git('merge-file','-p','--diff3',*[str(p) for p in ps],check=False)
                if merged.returncode==0:
                    row['status']='merge_clean'
                    row['merged_blob']=blob(merged.stdout)
                    row['patch']=''.join(difflib.unified_diff(current.decode().splitlines(True),merged.stdout.decode().splitlines(True),fromfile=destination,tofile=destination))
                else:
                    row['status']='conflict'
                    lines=merged.stdout.decode().splitlines()
                    blocks=[]; start=None
                    for i,line in enumerate(lines):
                        if line.startswith('<<<<<<<'):start=max(0,i-4)
                        if line.startswith('>>>>>>>') and start is not None:
                            blocks.append('\n'.join(lines[start:min(len(lines),i+5)]));start=None
                    row['conflicts']=blocks
        rows.append(row)
        print('AUTOBOTT_ENTRY_RESTORE '+json.dumps(row,sort_keys=True),flush=True)
    print('AUTOBOTT_ENTRY_RESTORE_SUMMARY '+json.dumps({'base':BASE,'donor':DONOR,'current':CURRENT,'files':len(rows),'counts':{s:sum(r['status']==s for r in rows) for s in sorted({r['status'] for r in rows})},'workspace_modified':False,'source_url':SOURCE,'broker_requests':0}),flush=True)
`;
  const child = spawnSync('python',['-c',script],{env:safe,encoding:'utf8',timeout:150000,maxBuffer:500000});
  if (child.stdout) process.stdout.write(child.stdout);
  assert.equal(child.status,0,'source_merge_inspection_failed:'+(child.stderr||'').slice(-1200));
}
