#!/usr/bin/env bash
#
# Renders a captured terminal transcript to a PNG using headless Chrome.
#
# No Node dependencies on purpose: this is a Java repository and a node_modules directory to
# render four images would be a poor trade. Chrome's --headless --screenshot needs nothing
# installed beyond a browser that is already on the machine.
#
# The image is generated FROM the captured .txt, never photographed from a terminal, so it cannot
# drift from what the commands actually printed. Re-running capture-verification.sh regenerates
# both; if a claim stops being true, the next image says so.
#
#   scripts/render-terminal.sh <input.txt> <output.png> [title]
#
set -euo pipefail
IN="$1"; OUT="$2"; TITLE="${3:-verification}"

CHROME=""
for c in "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
         "/Applications/Chromium.app/Contents/MacOS/Chromium" \
         "$(command -v google-chrome || true)" "$(command -v chromium || true)"; do
  [ -x "$c" ] && { CHROME="$c"; break; }
done
[ -z "$CHROME" ] && { echo "no Chrome found, skipping $OUT"; exit 0; }

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

python3 - "$IN" "$TITLE" > "$TMP/page.html" <<'PY'
import html, re, sys
raw = open(sys.argv[1], encoding='utf-8', errors='replace').read()
raw = re.sub(r'\x1b\[[0-9;]*m', '', raw)          # strip ANSI
title = html.escape(sys.argv[2])

def line(l):
    e = html.escape(l)
    if l.startswith('$ '):                                    return f'<span class=cmd>{e}</span>'
    if re.search(r'BUILD SUCCESS|applied cleanly|SUCCESS:', l): return f'<span class=ok>{e}</span>'
    if re.search(r'BUILD FAILURE|violates check constraint', l):return f'<span class=err>{e}</span>'
    if l.startswith('Tests run:'):                             return f'<span class=hi>{e}</span>'
    if re.match(r'^(──|════|\s*[+|])', l):                     return f'<span class=dim>{e}</span>'
    if 'expect' in l:                                          return f'<span class=note>{e}</span>'
    return e

body = '\n'.join(line(l) for l in raw.split('\n'))
cols = max((len(l) for l in raw.split('\n')), default=80)
width = min(1500, max(720, cols * 8 + 60))

print(f'''<!doctype html><meta charset=utf-8><style>
*{{box-sizing:border-box}} body{{margin:0;background:#0d1117;
  font-family:ui-monospace,"SF Mono",Menlo,monospace;width:{width}px}}
.bar{{background:#161b22;padding:10px 16px;display:flex;align-items:center;gap:8px;
  border-bottom:1px solid #30363d}}
.dot{{width:12px;height:12px;border-radius:50%}}
.r{{background:#ff5f57}}.y{{background:#febc2e}}.g{{background:#28c840}}
.t{{color:#8b949e;font-size:13px;margin-left:10px}}
pre{{margin:0;padding:18px 20px;color:#c9d1d9;font-size:13px;line-height:1.55;white-space:pre}}
.cmd{{color:#79c0ff;font-weight:600}}.ok{{color:#3fb950;font-weight:600}}
.err{{color:#ff7b72}}.hi{{color:#d2a8ff;font-weight:600}}
.dim{{color:#6e7681}}.note{{color:#e3b341}}
</style>
<div class=bar><span class="dot r"></span><span class="dot y"></span><span class="dot g"></span>
<span class=t>{title} — notification-platform</span></div>
<pre>{body}</pre>''')
PY

LINES=$(wc -l < "$IN"); HEIGHT=$(( LINES * 21 + 90 ))
[ "$HEIGHT" -gt 12000 ] && HEIGHT=12000
COLS=$(awk '{ if (length > m) m = length } END { print m+0 }' "$IN")
WIDTH=$(( COLS * 8 + 60 )); [ "$WIDTH" -lt 720 ] && WIDTH=720; [ "$WIDTH" -gt 1500 ] && WIDTH=1500

"$CHROME" --headless --disable-gpu --hide-scrollbars --force-device-scale-factor=2 \
  --screenshot="$OUT" --window-size="${WIDTH},${HEIGHT}" \
  "file://$TMP/page.html" >/dev/null 2>&1 || true
[ -f "$OUT" ] || echo "render failed: $OUT"
