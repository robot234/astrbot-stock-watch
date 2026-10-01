#!/bin/bash
set -u
export XDG_RUNTIME_DIR=/run/user/1000
WEB=/home/pi/apps/stock-watch-web
NEW=$WEB/releases/ui-redesign-20261001T1510CST
pkill -f "webapp.server --host 127.0.0.1 .* --port 18767" 2>/dev/null; sleep 1
echo "staging_listeners=$(ss -ltn | grep -c ':18767 ')"
PREV=$(readlink -f $WEB/current)
echo "prev=$PREV"
[ -d "$NEW" ] || { echo "new_release_missing"; exit 2; }
ln -sfn "$NEW" $WEB/current.next && mv -Tf $WEB/current.next $WEB/current
echo "now=$(readlink -f $WEB/current)"
systemctl --user restart stock-watch-web.service
ok=0
for i in 1 2 3 4 5 6 7 8 9 10; do
  sleep 2
  if [ "$(systemctl --user is-active stock-watch-web.service)" = active ] && curl -sf -o /dev/null http://192.168.124.6:8767/api/overview; then ok=1; break; fi
done
if [ $ok -ne 1 ]; then
  echo "health_failed_rolling_back"
  ln -sfn "$PREV" $WEB/current.next && mv -Tf $WEB/current.next $WEB/current
  systemctl --user restart stock-watch-web.service; sleep 4
  echo "rolled_back_to=$(readlink -f $WEB/current) active=$(systemctl --user is-active stock-watch-web.service)"
  exit 3
fi
echo "active=$(systemctl --user is-active stock-watch-web.service)"
ss -ltnp 2>/dev/null | grep ':8767 '
for r in overview candidates health "performance?horizon=5" settings signals intraday revision; do
  printf "%s=%s " "$r" "$(curl -s -o /dev/null -w '%{http_code}' "http://192.168.124.6:8767/api/$r")"
done; echo
echo "index_has_tabbar=$(curl -s http://192.168.124.6:8767/ | grep -c tabbar)"
echo "appjs_sha=$(curl -s http://192.168.124.6:8767/app.js | sha256sum | cut -c1-12)"
